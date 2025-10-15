package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"github.com/senku-tech/pg-outbox/pkg/config"
)

// EventPublisher handles publishing events to NATS
type EventPublisher struct {
	nc        *nats.Conn
	js        jetstream.JetStream
	cfg       *config.NATSConfig
	logger    *zap.Logger
	topicMap  map[string]string
}

// OutboxEvent represents an event to be published
type OutboxEvent struct {
	ID        string          `json:"id"`
	CreatedAt time.Time       `json:"created_at"`
	Topic     string          `json:"topic"`
	Metadata  json.RawMessage `json:"metadata"`
	Payload   json.RawMessage `json:"payload"`
}

// NewEventPublisher creates a new NATS event publisher
func NewEventPublisher(cfg *config.NATSConfig, logger *zap.Logger) (*EventPublisher, error) {
	// Connect to NATS with retry logic
	var nc *nats.Conn
	var err error

	for attempts := 0; attempts < cfg.MaxRetries; attempts++ {
		nc, err = nats.Connect(cfg.URL,
			nats.Name("outbox-dispatcher"),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second),
			nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
				if err != nil {
					logger.Error("NATS disconnected", zap.Error(err))
				} else {
					logger.Warn("NATS disconnected")
				}
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
			}),
			nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
				logger.Error("NATS async error",
					zap.Error(err),
					zap.String("subject", getSubject(sub)))
			}),
		)

		if err == nil {
			break
		}

		if attempts < cfg.MaxRetries-1 {
			logger.Warn("Failed to connect to NATS, retrying...",
				zap.Error(err),
				zap.Int("attempt", attempts+1),
				zap.Duration("retry_delay", cfg.RetryDelay))
			time.Sleep(cfg.RetryDelay)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS after %d attempts: %w", cfg.MaxRetries, err)
	}

	logger.Info("Connected to NATS", zap.String("url", cfg.URL))

	publisher := &EventPublisher{
		nc:       nc,
		cfg:      cfg,
		logger:   logger,
		topicMap: cfg.TopicMap,
	}

	// Initialize JetStream if enabled
	if cfg.JetStream.Enabled {
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("failed to create JetStream context: %w", err)
		}

		// Verify stream exists
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := js.Stream(ctx, cfg.JetStream.StreamName)
		if err != nil {
			logger.Warn("JetStream stream not found, will use core NATS",
				zap.String("stream", cfg.JetStream.StreamName),
				zap.Error(err))
			// Continue without JetStream - fallback to core NATS
		} else {
			publisher.js = js
			logger.Info("JetStream enabled",
				zap.String("stream", cfg.JetStream.StreamName),
				zap.Strings("subjects", stream.CachedInfo().Config.Subjects))
		}
	}

	return publisher, nil
}

// Publish sends an event to NATS
func (p *EventPublisher) Publish(ctx context.Context, event OutboxEvent) error {
	startTime := time.Now()

	// Apply topic mapping
	mappedTopic := p.mapTopic(event.Topic)

	// Create NATS message
	msg := &nats.Msg{
		Subject: mappedTopic,
		Header:  nats.Header{},
	}

	// Track if event ID is provided in metadata
	var eventIDFromMetadata string

	// Add metadata as headers
	if len(event.Metadata) > 0 {
		var metadata map[string]interface{}
		if err := json.Unmarshal(event.Metadata, &metadata); err == nil {
			for key, value := range metadata {
				// Check for event ID in metadata (case-insensitive)
				if strings.ToLower(key) == "event-id" || strings.ToLower(key) == "event_id" {
					if strVal, ok := value.(string); ok && strVal != "" {
						eventIDFromMetadata = strVal
					}
				}

				if strVal, ok := value.(string); ok {
					msg.Header.Set(key, strVal)
				} else {
					// Convert non-string values to JSON
					if jsonVal, err := json.Marshal(value); err == nil {
						msg.Header.Set(key, string(jsonVal))
					}
				}
			}
		}
	}

	// Set Event-ID header: use metadata event ID if present, otherwise use database record ID
	if eventIDFromMetadata != "" {
		msg.Header.Set("Event-ID", eventIDFromMetadata)
		p.logger.Debug("Using event ID from metadata",
			zap.String("event_id", eventIDFromMetadata),
			zap.String("db_record_id", event.ID))
	} else {
		msg.Header.Set("Event-ID", event.ID)
		p.logger.Debug("Using database record ID as event ID",
			zap.String("event_id", event.ID))
	}
	msg.Header.Set("Event-Time", event.CreatedAt.Format(time.RFC3339))
	msg.Header.Set("Publisher", "outbox-dispatcher")

	// Set payload
	msg.Data = event.Payload

	// Publish using JetStream if available, otherwise use core NATS
	var err error
	if p.js != nil {
		// Use JetStream for durability
		pubAck, err := p.js.PublishMsg(ctx, msg)
		if err != nil {
			p.logger.Error("Failed to publish event via JetStream",
				zap.String("event_id", event.ID),
				zap.String("topic", event.Topic),
				zap.Error(err))
			return fmt.Errorf("JetStream publish failed: %w", err)
		}

		p.logger.Debug("Event published via JetStream",
			zap.String("event_id", event.ID),
			zap.String("original_topic", event.Topic),
			zap.String("mapped_topic", mappedTopic),
			zap.Uint64("sequence", pubAck.Sequence),
			zap.String("stream", pubAck.Stream),
			zap.Duration("latency", time.Since(startTime)))
	} else {
		// Use core NATS
		err = p.nc.PublishMsg(msg)
		if err != nil {
			p.logger.Error("Failed to publish event via core NATS",
				zap.String("event_id", event.ID),
				zap.String("topic", event.Topic),
				zap.Error(err))
			return fmt.Errorf("NATS publish failed: %w", err)
		}

		// Flush to ensure delivery
		if err := p.nc.FlushTimeout(2 * time.Second); err != nil {
			p.logger.Warn("Failed to flush NATS connection",
				zap.String("event_id", event.ID),
				zap.Error(err))
		}

		p.logger.Debug("Event published via core NATS",
			zap.String("event_id", event.ID),
			zap.String("original_topic", event.Topic),
			zap.String("mapped_topic", mappedTopic),
			zap.Duration("latency", time.Since(startTime)))
	}

	return nil
}

// Close gracefully closes the publisher
func (p *EventPublisher) Close() error {
	if p.nc != nil {
		// Drain connection to ensure all pending messages are sent
		if err := p.nc.Drain(); err != nil {
			p.logger.Warn("Failed to drain NATS connection", zap.Error(err))
		}
		p.nc.Close()
	}
	return nil
}

// Health checks if the NATS connection is healthy
func (p *EventPublisher) Health() error {
	if p.nc == nil {
		return fmt.Errorf("NATS connection is nil")
	}

	if p.nc.IsClosed() {
		return fmt.Errorf("NATS connection is closed")
	}

	if !p.nc.IsConnected() {
		return fmt.Errorf("NATS is not connected")
	}

	return nil
}

// mapTopic applies topic mapping if configured
func (p *EventPublisher) mapTopic(originalTopic string) string {
	if p.topicMap == nil {
		return originalTopic
	}

	if mappedTopic, exists := p.topicMap[originalTopic]; exists {
		return mappedTopic
	}

	return originalTopic
}

// getSubject safely extracts subject from subscription
func getSubject(sub *nats.Subscription) string {
	if sub != nil {
		return sub.Subject
	}
	return "unknown"
}
