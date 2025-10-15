package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/senku-tech/pg-outbox/pkg/config"
	"github.com/senku-tech/pg-outbox/pkg/publisher"
	"github.com/senku-tech/pg-outbox/internal/queries/generated"
)

// BatchProcessor processes batches of outbox events
type BatchProcessor struct {
	pool        *pgxpool.Pool
	queries     *queries.Queries
	publisher   *publisher.EventPublisher
	retry       *publisher.RetryStrategy
	cfg         *config.DispatcherConfig
	logger      *zap.Logger
	metrics     *Metrics
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(
	pool *pgxpool.Pool,
	pub *publisher.EventPublisher,
	cfg *config.DispatcherConfig,
	logger *zap.Logger,
	metrics *Metrics,
) *BatchProcessor {
	return &BatchProcessor{
		pool:      pool,
		queries:   queries.New(pool),
		publisher: pub,
		retry: publisher.NewRetryStrategy(
			3,                    // max attempts per publish
			100*time.Millisecond, // base delay
			5*time.Second,        // max delay
			logger,
		),
		cfg:     cfg,
		logger:  logger,
		metrics: metrics,
	}
}

// ProcessBatch claims and processes a batch of events
func (bp *BatchProcessor) ProcessBatch(ctx context.Context) error {
	startTime := time.Now()
	
	// Start a transaction for atomic batch processing
	tx, err := bp.pool.Begin(ctx)
	if err != nil {
		bp.logger.Error("Failed to start transaction", zap.Error(err))
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			bp.logger.Warn("Failed to rollback transaction", zap.Error(err))
		}
	}()

	qtx := bp.queries.WithTx(tx)

	// Claim a batch of events using FOR UPDATE SKIP LOCKED
	// This provides automatic distributed coordination
	events, err := qtx.ClaimOutboxBatch(ctx, queries.ClaimOutboxBatchParams{
		MaxAttempts: int32(bp.cfg.MaxAttempts),
		BatchSize:   int32(bp.cfg.BatchSize),
	})
	if err != nil {
		if err == pgx.ErrNoRows {
			// No events available, this is normal
			bp.logger.Debug("No events available for processing")
			return nil
		}
		bp.logger.Error("Failed to claim outbox batch", zap.Error(err))
		return fmt.Errorf("claim batch: %w", err)
	}

	if len(events) == 0 {
		bp.logger.Debug("No events to process")
		return nil
	}

	bp.logger.Info("Processing event batch",
		zap.Int("batch_size", len(events)),
		zap.String("instance", bp.cfg.InstanceID))

	// Track processing results
	var successCount, failCount int

	// Process each event in the batch
	for _, event := range events {
		// Convert UUID to string for logging
		var eventID string
		if event.ID.Valid {
			eventID = uuid.UUID(event.ID.Bytes).String()
		} else {
			eventID = "invalid-uuid"
		}

		if err := bp.processEvent(ctx, qtx, event); err != nil {
			bp.logger.Error("Failed to process event",
				zap.String("event_id", eventID),
				zap.String("topic", event.Topic),
				zap.Error(err))
			failCount++
			
			// Record error in database
			if recordErr := qtx.RecordEventError(ctx, queries.RecordEventErrorParams{
				ID:        event.ID,
				CreatedAt: pgtype.Timestamptz{Time: event.CreatedAt, Valid: true},
				Error:     err.Error(),
			}); recordErr != nil {
				bp.logger.Error("Failed to record event error",
					zap.String("event_id", eventID),
					zap.Error(recordErr))
			}
		} else {
			successCount++
			
			// Mark event as published
			if err := qtx.MarkEventPublished(ctx, queries.MarkEventPublishedParams{
				ID:        event.ID,
				CreatedAt: pgtype.Timestamptz{Time: event.CreatedAt, Valid: true},
			}); err != nil {
				bp.logger.Error("Failed to mark event as published",
					zap.String("event_id", eventID),
					zap.Error(err))
				// Don't fail the whole batch for this
			}
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		bp.logger.Error("Failed to commit transaction", zap.Error(err))
		return fmt.Errorf("commit transaction: %w", err)
	}

	// Update metrics
	bp.metrics.RecordBatchProcessed(successCount, failCount, time.Since(startTime))

	bp.logger.Info("Batch processing completed",
		zap.Int("total", len(events)),
		zap.Int("success", successCount),
		zap.Int("failed", failCount),
		zap.Duration("duration", time.Since(startTime)))

	return nil
}

// processEvent publishes a single event to NATS
func (bp *BatchProcessor) processEvent(ctx context.Context, qtx *queries.Queries, event queries.ClaimOutboxBatchRow) error {
	// Convert UUID to string
	var eventID string
	if event.ID.Valid {
		eventID = uuid.UUID(event.ID.Bytes).String()
	} else {
		return fmt.Errorf("invalid event ID")
	}

	// Convert to publisher event format
	pubEvent := publisher.OutboxEvent{
		ID:        eventID,
		CreatedAt: event.CreatedAt,
		Topic:     event.Topic,
		Metadata:  json.RawMessage(event.Metadata),
		Payload:   json.RawMessage(event.Payload),
	}

	// Check if this is the last attempt
	isLastAttempt := event.Attempts >= int32(bp.cfg.MaxAttempts)

	// Publish with retry strategy
	err := bp.retry.PublishWithRetry(ctx, bp.publisher, pubEvent)
	if err != nil {
		if isLastAttempt {
			bp.logger.Error("Event exceeded max attempts, moving to dead letter",
				zap.String("event_id", eventID),
				zap.String("topic", event.Topic),
				zap.Int32("attempts", event.Attempts),
				zap.Error(err))
			// TODO: Implement dead letter queue handling
			bp.metrics.RecordDeadLetter(event.Topic)
		}
		return err
	}

	bp.logger.Debug("Event published successfully",
		zap.String("event_id", eventID),
		zap.String("topic", event.Topic),
		zap.Int32("attempts", event.Attempts))
	
	bp.metrics.RecordEventPublished(event.Topic)
	
	return nil
}

// GetMetrics returns current processing metrics
func (bp *BatchProcessor) GetMetrics(ctx context.Context) (*queries.GetPublishingMetricsRow, error) {
	result, err := bp.queries.GetPublishingMetrics(ctx, int32(bp.cfg.MaxAttempts))
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GetStuckEvents returns events that appear to be stuck
func (bp *BatchProcessor) GetStuckEvents(ctx context.Context, limit int) ([]queries.GetStuckEventsRow, error) {
	return bp.queries.GetStuckEvents(ctx, int32(limit))
}

// ResetStuckEvents resets stuck events for retry (admin operation)
func (bp *BatchProcessor) ResetStuckEvents(ctx context.Context, startTime, endTime time.Time) error {
	return bp.queries.ResetStuckEvents(ctx, queries.ResetStuckEventsParams{
		MaxAttempts: int32(bp.cfg.MaxAttempts),
		StartTime:   pgtype.Timestamptz{Time: startTime, Valid: true},
		EndTime:     pgtype.Timestamptz{Time: endTime, Valid: true},
	})
}