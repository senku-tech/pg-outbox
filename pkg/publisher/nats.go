package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/senku-tech/pg-outbox/pkg/outbox"
)

// EventPublisher handles publishing events to NATS
type EventPublisher struct {
	js jetstream.JetStream
}

// New creates a new NATS event publisher
func New(js jetstream.JetStream) *EventPublisher {
	return &EventPublisher{js: js}
}

// Publish sends an event to NATS
func (p *EventPublisher) Publish(ctx context.Context, event outbox.OutboxEvent, systemMetadata map[string]string) error {
	// Create NATS message
	msg := &nats.Msg{
		Subject: event.Topic,
		Header:  nats.Header{},
		Data:    event.Payload,
	}

	// Add event metadata as headers
	var metadata map[string]string
	json.Unmarshal(event.Metadata, &metadata)
	for key, value := range metadata {
		msg.Header.Set(key, value)
	}

	// Add system metadata as headers
	for key, value := range systemMetadata {
		msg.Header.Set(key, value)
	}

	// Publish to JetStream
	_, err := p.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	return nil
}

