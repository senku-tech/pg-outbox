package publisher

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/senku-tech/pg-outbox/pkg/outbox"
	"go.uber.org/zap"
)

// RetryStrategy defines the retry behavior for failed publishes
type RetryStrategy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	logger      *zap.Logger
}

// NewRetryStrategy creates a new retry strategy
func NewRetryStrategy(maxAttempts int, baseDelay, maxDelay time.Duration, logger *zap.Logger) *RetryStrategy {
	return &RetryStrategy{
		MaxAttempts: maxAttempts,
		BaseDelay:   baseDelay,
		MaxDelay:    maxDelay,
		logger:      logger,
	}
}

// PublishWithRetry attempts to publish an event with exponential backoff
func (rs *RetryStrategy) PublishWithRetry(
	ctx context.Context,
	publisher *EventPublisher,
	event outbox.OutboxEvent,
	systemMetadata map[string]string,
) error {
	var lastErr error

	for attempt := 1; attempt <= rs.MaxAttempts; attempt++ {
		// Check context before attempting
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Attempt to publish
		err := publisher.Publish(ctx, event, systemMetadata)
		if err == nil {
			if attempt > 1 {
				rs.logger.Info("Event published after retry",
					zap.String("topic", event.Topic),
					zap.Int("attempt", attempt))
			}
			return nil
		}

		lastErr = err

		// Don't retry if we've exceeded max attempts
		if attempt >= rs.MaxAttempts {
			rs.logger.Error("Failed to publish event after max attempts",
				zap.String("topic", event.Topic),
				zap.Int("attempts", attempt),
				zap.Error(lastErr))
			break
		}

		// Calculate backoff delay
		delay := rs.calculateBackoff(attempt)

		rs.logger.Warn("Failed to publish event, will retry",
			zap.String("topic", event.Topic),
			zap.Int("attempt", attempt),
			zap.Duration("retry_in", delay),
			zap.Error(err))

		// Wait before retry
		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", rs.MaxAttempts, lastErr)
}

// calculateBackoff calculates the exponential backoff delay
func (rs *RetryStrategy) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: baseDelay * 2^(attempt-1)
	delay := time.Duration(float64(rs.BaseDelay) * math.Pow(2, float64(attempt-1)))

	// Add jitter (±10%)
	jitter := time.Duration(float64(delay) * 0.1 * (math.Sin(float64(time.Now().UnixNano()))))
	delay += jitter

	// Cap at max delay
	if delay > rs.MaxDelay {
		delay = rs.MaxDelay
	}

	return delay
}

// ShouldRetry determines if an error is retryable
func (rs *RetryStrategy) ShouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are not retryable
	if err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}

	// TODO: Add more sophisticated error classification
	// For now, treat all errors as retryable
	return true
}
