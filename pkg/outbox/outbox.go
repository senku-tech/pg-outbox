package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// OutboxEvent represents an event in the outbox table
// This is the CONTRACT between write side and read side
type OutboxEvent struct {
	Topic    string          `json:"topic"`
	Metadata json.RawMessage `json:"metadata"`
	Payload  json.RawMessage `json:"payload"`
}

// Publishable is an interface for entities that can be published to the outbox
type Publishable interface {
	ToOutbox() OutboxEvent
}

// Outbox provides methods to publish events to the outbox table
type Outbox struct{}

// New creates a new outbox
func New() *Outbox {
	return &Outbox{}
}

// Publish writes one or more publishable entities to the outbox table
func (o *Outbox) Publish(ctx context.Context, tx pgx.Tx, publishables ...Publishable) error {
	if len(publishables) == 0 {
		return nil
	}

	now := time.Now()

	// Prepare rows for CopyFrom
	rows := make([][]interface{}, len(publishables))
	for i, p := range publishables {
		event := p.ToOutbox()
		// Generate UUID for each event
		id := uuid.New()
		rows[i] = []interface{}{
			id,
			now,
			event.Topic,
			event.Metadata,
			event.Payload,
		}
	}

	// Use CopyFrom for bulk insert
	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"outboxes"},
		[]string{"id", "created_at", "topic", "metadata", "payload"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox events: %w", err)
	}

	return nil
}
