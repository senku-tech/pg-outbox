package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/senku-tech/pg-outbox/pkg/config"
	"github.com/senku-tech/pg-outbox/pkg/outbox"
	"github.com/senku-tech/pg-outbox/pkg/publisher"
)

// ProcessingMode represents the current processing mode
type ProcessingMode int

const (
	POLLING_MODE ProcessingMode = iota
	LISTEN_MODE
)

func (m ProcessingMode) String() string {
	switch m {
	case POLLING_MODE:
		return "POLLING"
	case LISTEN_MODE:
		return "LISTEN"
	default:
		return "UNKNOWN"
	}
}

// HybridProcessor handles the state-based hybrid processing
type HybridProcessor struct {
	pool      *pgxpool.Pool
	publisher *publisher.EventPublisher
	service   *Service
	cfg       *config.DispatcherConfig
	logger    *zap.Logger
	metrics   *Metrics

	// State management
	mu           sync.RWMutex
	currentMode  ProcessingMode
	listenConn   *pgxpool.Conn  // Pool connection, not raw conn

	// Control channels
	stopCh       chan struct{}
	runWg        sync.WaitGroup  // Track when run loop exits
	processingWg sync.WaitGroup  // Track in-flight batch processing
}

// NewHybridProcessor creates a new hybrid processor
func NewHybridProcessor(pool *pgxpool.Pool, pub *publisher.EventPublisher, cfg *config.DispatcherConfig, logger *zap.Logger, metrics *Metrics, service *Service) *HybridProcessor {
	// Ensure intervals are positive
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
		logger.Warn("Poll interval was zero or negative, using default", zap.Duration("default", pollInterval))
	}

	fallbackInterval := cfg.FallbackInterval
	if fallbackInterval <= 0 {
		fallbackInterval = 30 * time.Second
		logger.Warn("Fallback interval was zero or negative, using default", zap.Duration("default", fallbackInterval))
	}

	// Create a safe copy of config with validated intervals
	safeCfg := *cfg
	safeCfg.PollInterval = pollInterval
	safeCfg.FallbackInterval = fallbackInterval

	return &HybridProcessor{
		pool:           pool,
		publisher:      pub,
		service:        service,
		cfg:            &safeCfg,
		logger:         logger,
		metrics:        metrics,
		currentMode:    POLLING_MODE, // Always start in polling mode
		stopCh:         make(chan struct{}),
	}
}

// GetCurrentMode returns the current processing mode
func (h *HybridProcessor) GetCurrentMode() ProcessingMode {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.currentMode
}

// setMode updates the current processing mode
func (h *HybridProcessor) setMode(mode ProcessingMode) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.currentMode != mode {
		h.logger.Info("🔄 Switching processing mode",
			zap.String("from", h.currentMode.String()),
			zap.String("to", mode.String()))
		h.currentMode = mode
	}
}

// Start begins the hybrid processing
func (h *HybridProcessor) Start(ctx context.Context) error {
	h.logger.Info("🚀 Starting hybrid processor", zap.String("initial_mode", "POLLING"))

	// Start the main processing loop
	h.runWg.Add(1)
	go func() {
		defer h.runWg.Done()
		h.run(ctx)
	}()

	return nil
}

// Stop gracefully stops the hybrid processor
func (h *HybridProcessor) Stop(ctx context.Context) error {
	h.logger.Info("🛑 Stopping hybrid processor")

	// Signal stop first
	close(h.stopCh)

	// Wait for run loop to exit completely
	h.runWg.Wait()

	// Wait for any in-flight batch processing to complete
	h.logger.Info("⏳ Waiting for in-flight batch processing to complete")
	h.processingWg.Wait()
	h.logger.Info("✅ All in-flight batches completed")

	// Then clean up resources (run loop should have exited now)
	h.mu.Lock()
	if h.listenConn != nil {
		h.listenConn.Release()
		h.listenConn = nil
	}
	h.mu.Unlock()

	return nil
}

// run is the main processing loop - completely sequential, no goroutines
func (h *HybridProcessor) run(ctx context.Context) {
	pollTicker := time.NewTicker(h.cfg.PollInterval)
	defer pollTicker.Stop()
	
	fallbackTicker := time.NewTicker(h.cfg.FallbackInterval)
	defer fallbackTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("Hybrid processor stopping due to context cancellation")
			return
		case <-h.stopCh:
			h.logger.Info("Hybrid processor stopping due to shutdown signal")
			return
		case <-pollTicker.C:
			// In POLLING_MODE: process batch
			if h.GetCurrentMode() == POLLING_MODE {
				h.processEventsWithModeCheck(ctx)
			}
		case <-fallbackTicker.C:
			// In LISTEN_MODE: check for missed events
			if h.GetCurrentMode() == LISTEN_MODE {
				h.fallbackCheck(ctx)
			}
		default:
			// In LISTEN_MODE: wait for notification (with timeout)
			if h.GetCurrentMode() == LISTEN_MODE && h.listenConn != nil {
				h.waitForNotification(ctx)
			} else {
				// Small sleep to prevent busy loop
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// processEventsWithModeCheck processes events and checks if we should switch modes
func (h *HybridProcessor) processEventsWithModeCheck(ctx context.Context) {
	queueEmpty := h.processEvents(ctx)
	
	// Switch to LISTEN_MODE if queue is empty
	if queueEmpty && h.GetCurrentMode() == POLLING_MODE {
		if err := h.setupListenMode(ctx); err != nil {
			h.logger.Error("Failed to setup LISTEN mode, staying in POLLING mode", zap.Error(err))
		} else {
			h.setMode(LISTEN_MODE)
		}
	}
}

// processEvents processes a batch of events and returns true if queue is empty
func (h *HybridProcessor) processEvents(ctx context.Context) bool {
	// Track in-flight batch processing
	h.processingWg.Add(1)
	defer h.processingWg.Done()

	err := h.processBatch(ctx)
	if err != nil {
		h.logger.Error("Failed to process batch", zap.Error(err))
		return false
	}

	// Update lastProcessed timestamp after successful batch
	if h.service != nil {
		h.service.UpdateLastProcessed()
	}

	// Check if there are still pending events to determine if we processed any
	pendingCount, err := h.getPendingCount(ctx)
	if err != nil {
		h.logger.Error("Failed to get pending count", zap.Error(err))
		return false
	}

	// If there are no pending events, we can switch to LISTEN mode
	return pendingCount == 0
}

// setupListenMode establishes the LISTEN connection
func (h *HybridProcessor) setupListenMode(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Close existing connection if any
	if h.listenConn != nil {
		h.listenConn.Release()
		h.listenConn = nil
	}
	
	// Create new connection for LISTEN
	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection for LISTEN: %w", err)
	}
	
	h.listenConn = conn
	
	// Start listening for notifications
	_, err = conn.Exec(ctx, "LISTEN outbox_events")
	if err != nil {
		conn.Release()
		h.listenConn = nil
		return fmt.Errorf("failed to LISTEN: %w", err)
	}
	
	// NO GOROUTINE - everything is sequential now
	
	h.logger.Info("📡 LISTEN mode activated")
	return nil
}

// waitForNotification waits for a single notification (called from main loop)
func (h *HybridProcessor) waitForNotification(ctx context.Context) {
	// Check if we're stopping
	select {
	case <-h.stopCh:
		return
	default:
	}
	
	h.mu.RLock()
	conn := h.listenConn
	h.mu.RUnlock()
	
	if conn == nil {
		return
	}
	
	// Create a context with short timeout for non-blocking wait
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	
	// Wait for notification with timeout
	notification, err := conn.Conn().WaitForNotification(waitCtx)
	if err != nil {
		// Timeout is expected and ok
		if ctx.Err() != nil || waitCtx.Err() != nil {
			return
		}
		
		// Real error - switch back to polling
		h.logger.Error("Error waiting for notification", zap.Error(err))
		h.setMode(POLLING_MODE)
		return
	}
	
	if notification != nil {
		h.logger.Info("📨 Received notification",
			zap.String("channel", notification.Channel),
			zap.String("payload", notification.Payload))

		// Process immediately - don't wait for next poll cycle
		h.processEventsWithModeCheck(ctx)
	}
}

// fallbackCheck periodically checks for missed events in LISTEN_MODE
func (h *HybridProcessor) fallbackCheck(ctx context.Context) {
	h.logger.Debug("🔍 Performing fallback check")

	// Check if there are any pending events we missed
	pendingCount, err := h.getPendingCount(ctx)
	if err != nil {
		h.logger.Error("Failed to get pending count during fallback check", zap.Error(err))
		return
	}

	if pendingCount > 0 {
		h.logger.Warn("📨 Fallback check found missed events", zap.Int64("pending", pendingCount))
		// Close LISTEN connection and switch back to polling mode
		h.mu.Lock()
		if h.listenConn != nil {
			h.listenConn.Release()
			h.listenConn = nil
		}
		h.mu.Unlock()

		// Switch back to polling mode to clear the backlog
		h.setMode(POLLING_MODE)
	}
}

// processBatch claims and publishes a batch of events
func (h *HybridProcessor) processBatch(ctx context.Context) error {
	startTime := time.Now()

	h.logger.Info("🔍 processBatch: Attempting to claim events", zap.Int("batch_size", h.cfg.BatchSize))

	// Start transaction for atomic batch processing
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		h.metrics.RecordError()
		h.logger.Error("❌ processBatch: Failed to begin transaction", zap.Error(err))
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Claim events using CTE pattern (increment attempts, don't set published_at yet)
	query := `
		WITH claimed AS (
			SELECT id FROM outboxes
			WHERE published_at IS NULL
			  AND attempts < $2
			ORDER BY seq
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outboxes o
		SET attempts = o.attempts + 1
		FROM claimed c
		WHERE o.id = c.id
		RETURNING o.id, o.topic, o.metadata, o.payload
	`

	rows, err := tx.Query(ctx, query, h.cfg.BatchSize, h.cfg.MaxAttempts)
	if err != nil {
		h.metrics.RecordError()
		h.logger.Error("❌ processBatch: Failed to execute claim query", zap.Error(err))
		return fmt.Errorf("failed to claim events: %w", err)
	}
	defer rows.Close()

	// Process claimed events
	var eventCount int
	var publishedIDs []string

	h.logger.Info("🔍 processBatch: Query executed, scanning rows")
	for rows.Next() {
		var id string
		var event outbox.OutboxEvent

		if err := rows.Scan(&id, &event.Topic, &event.Metadata, &event.Payload); err != nil {
			h.metrics.RecordError()
			return fmt.Errorf("failed to scan event: %w", err)
		}

		// Add system metadata
		systemMetadata := map[string]string{
			"Nats-Event-Id":      id,
			"Nats-Dispatcher-Id": h.cfg.InstanceID,
			"Nats-Published-At":  time.Now().Format(time.RFC3339),
		}

		// Publish to NATS
		if err := h.publisher.Publish(ctx, event, systemMetadata); err != nil {
			h.metrics.RecordError()
			h.logger.Error("Failed to publish event to NATS",
				zap.String("event_id", id),
				zap.String("topic", event.Topic),
				zap.Error(err))
			// Don't mark as failed in transaction, just continue
			// Transaction will rollback and attempts will stay incremented
			continue
		}

		publishedIDs = append(publishedIDs, id)
		eventCount++
		h.metrics.RecordSuccess(time.Since(startTime))
	}

	if err := rows.Err(); err != nil {
		h.metrics.RecordError()
		return fmt.Errorf("error iterating events: %w", err)
	}

	h.logger.Info("🔍 processBatch: Finished scanning rows", zap.Int("event_count", eventCount))

	// Mark all successfully published events
	if len(publishedIDs) > 0 {
		markQuery := `UPDATE outboxes SET published_at = NOW() WHERE id = ANY($1)`
		if _, err := tx.Exec(ctx, markQuery, publishedIDs); err != nil {
			h.logger.Error("Failed to mark events as published",
				zap.Int("count", len(publishedIDs)),
				zap.Error(err))
			// Continue anyway, events were published to NATS
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		h.logger.Error("Failed to commit transaction", zap.Error(err))
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if eventCount > 0 {
		h.logger.Info("✅ Batch processed",
			zap.Int("events", eventCount),
			zap.Duration("duration", time.Since(startTime)))
	} else {
		h.logger.Info("ℹ️  processBatch: No events claimed (queue empty or events locked)")
	}

	return nil
}

// markFailed marks an event as failed
func (h *HybridProcessor) markFailed(ctx context.Context, id string, publishErr error) {
	query := `
		UPDATE outboxes
		SET attempts = attempts + 1,
		    last_error = $1,
		    published_at = NULL
		WHERE id = $2
		AND attempts < $3
	`

	_, err := h.pool.Exec(ctx, query, publishErr.Error(), id, h.cfg.MaxAttempts)
	if err != nil {
		h.logger.Error("Failed to mark event as failed",
			zap.String("event_id", id),
			zap.Error(err))
	}
}

// getPendingCount returns the count of pending events
func (h *HybridProcessor) getPendingCount(ctx context.Context) (int64, error) {
	var count int64
	query := `SELECT COUNT(*) FROM outboxes WHERE published_at IS NULL`

	err := h.pool.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get pending count: %w", err)
	}

	return count, nil
}