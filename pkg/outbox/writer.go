package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX is the interface for database operations (pgx.Conn, pgx.Tx, or pgxpool.Pool)
type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

// Writer provides methods to write events to the outbox table
type Writer struct {
	db DBTX
}

// NewWriter creates a new outbox writer
func NewWriter(db DBTX) *Writer {
	return &Writer{db: db}
}

// WithTx returns a new Writer with the transaction
func (w *Writer) WithTx(tx pgx.Tx) *Writer {
	return &Writer{db: tx}
}

// Event represents an outbox event to be published
type Event struct {
	Topic    string                 // NATS topic/subject
	Metadata map[string]interface{} // Event metadata (headers)
	Payload  interface{}            // Event payload (will be JSON marshaled)
}

// Publish writes an event to the outbox table
func (w *Writer) Publish(ctx context.Context, event Event) error {
	// Marshal metadata
	var metadataJSON []byte
	var err error
	if event.Metadata != nil {
		metadataJSON, err = json.Marshal(event.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
	} else {
		metadataJSON = []byte("{}")
	}

	// Marshal payload
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Insert into outbox table
	query := `
		INSERT INTO outboxes (id, created_at, topic, metadata, payload)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = w.db.Exec(ctx, query,
		uuid.New(),
		time.Now(),
		event.Topic,
		metadataJSON,
		payloadJSON,
	)

	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	return nil
}

// PublishBatch writes multiple events to the outbox table in a single operation
func (w *Writer) PublishBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Build bulk insert query
	query := `
		INSERT INTO outboxes (id, created_at, topic, metadata, payload)
		VALUES
	`

	args := make([]interface{}, 0, len(events)*5)
	for i, event := range events {
		if i > 0 {
			query += ","
		}

		// Marshal metadata
		var metadataJSON []byte
		var err error
		if event.Metadata != nil {
			metadataJSON, err = json.Marshal(event.Metadata)
			if err != nil {
				return fmt.Errorf("failed to marshal metadata for event %d: %w", i, err)
			}
		} else {
			metadataJSON = []byte("{}")
		}

		// Marshal payload
		payloadJSON, err := json.Marshal(event.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload for event %d: %w", i, err)
		}

		paramIndex := i*5 + 1
		query += fmt.Sprintf(" ($%d, $%d, $%d, $%d, $%d)", paramIndex, paramIndex+1, paramIndex+2, paramIndex+3, paramIndex+4)

		args = append(args,
			uuid.New(),
			time.Now(),
			event.Topic,
			metadataJSON,
			payloadJSON,
		)
	}

	_, err := w.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to insert batch outbox events: %w", err)
	}

	return nil
}

// Helper function to create an event with just topic and payload
func NewEvent(topic string, payload interface{}) Event {
	return Event{
		Topic:   topic,
		Payload: payload,
	}
}

// Helper function to create an event with metadata
func NewEventWithMetadata(topic string, metadata map[string]interface{}, payload interface{}) Event {
	return Event{
		Topic:    topic,
		Metadata: metadata,
		Payload:  payload,
	}
}
