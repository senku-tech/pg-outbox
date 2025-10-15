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

// Outbox writes messages to the outbox table
type Outbox struct {
	db DBTX
}

// New creates a new outbox
func New(db DBTX) *Outbox {
	return &Outbox{db: db}
}

// Write writes a message to the outbox table
// Pass tx to use a transaction, or nil to use the outbox's db (pool)
func (o *Outbox) Write(ctx context.Context, tx DBTX, topic string, metadata map[string]interface{}, payload interface{}) error {
	executor := tx
	if executor == nil {
		executor = o.db
	}

	// Marshal metadata
	var metadataJSON []byte
	var err error
	if metadata != nil {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
	} else {
		metadataJSON = []byte("{}")
	}

	// Marshal payload
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Insert into outbox table
	query := `
		INSERT INTO outboxes (id, created_at, topic, metadata, payload)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = executor.Exec(ctx, query,
		uuid.New(),
		time.Now(),
		topic,
		metadataJSON,
		payloadJSON,
	)

	if err != nil {
		return fmt.Errorf("failed to insert outbox message: %w", err)
	}

	return nil
}
