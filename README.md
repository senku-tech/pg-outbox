# pg-outbox

A reliable PostgreSQL-based transactional outbox pattern implementation for publishing events to NATS/JetStream.

**Dual-mode solution:**
1. **Library/SDK** - Import into your Go application to write events to the outbox table
2. **Standalone Dispatcher** - Run as an independent service to publish events from the outbox to NATS

## Features

- **Transactional Outbox Pattern**: Ensures events are published at-least-once with no data loss
- **NATS/JetStream Support**: Publishes to both NATS Core and JetStream
- **Distributed Coordination**: Uses PostgreSQL `FOR UPDATE SKIP LOCKED` for automatic distributed processing
- **FIFO Ordering**: Guarantees strict ordering using monotonic sequence numbers
- **Retry with Backoff**: Exponential backoff for failed publish attempts
- **Partitioned Tables**: Monthly partitioning for efficient data management
- **Topic Mapping**: Flexible topic transformation/routing
- **Health Checks**: Built-in health and readiness endpoints
- **Metrics**: Prometheus-compatible metrics for monitoring

## Architecture

```
┌─────────────────────┐
│  Your Application   │
│  (uses pg-outbox    │──┐  1. Insert event in transaction
│   library/SDK)      │  │     (same transaction as business logic)
└─────────────────────┘  │
                         ▼
                 ┌───────────────┐
                 │   Outbox      │
                 │   Table       │
                 │ (PostgreSQL)  │
                 └───────────────┘
                         │
                         │  2. pg-outbox dispatcher claims batch
                         │     (FOR UPDATE SKIP LOCKED)
                         ▼
                 ┌───────────────┐
                 │  pg-outbox    │
                 │  Dispatcher   │
                 │  (standalone) │
                 └───────────────┘
                         │
                         │  3. Publish events to NATS
                         ▼
                 ┌───────────────┐
                 │     NATS      │
                 │  JetStream    │
                 └───────────────┘
```

## Installation

### As a Library/SDK

```bash
go get github.com/senku-tech/pg-outbox
```

### As a Standalone Tool

#### From Source
```bash
git clone https://github.com/senku-tech/pg-outbox.git
cd pg-outbox
make build
# Binary will be in bin/pg-outbox-dispatcher
```

#### Using Docker
```bash
docker pull ghcr.io/senku-tech/pg-outbox-dispatcher:latest
```

#### Using Go Install
```bash
go install github.com/senku-tech/pg-outbox/cmd/dispatcher@latest
```

## Quick Start

### 1. Database Setup

Run the schema migration to create the outbox table:

```bash
psql your_database < sql/schema.sql
```

This creates:
- Partitioned `outboxes` table with monthly partitions
- Indexes for efficient querying
- Automatic partition creation function
- Optional cleanup function

### 2. Use the Library in Your Application

```go
package main

import (
    "context"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/senku-tech/pg-outbox/pkg/outbox"
)

func main() {
    // Connect to your database
    pool, _ := pgxpool.New(context.Background(), "postgresql://...")
    defer pool.Close()

    // Create outbox writer
    writer := outbox.NewWriter(pool)

    // Start a transaction for your business logic
    tx, _ := pool.Begin(context.Background())
    defer tx.Rollback(context.Background())

    // Your business logic
    user := createUser(tx, userData)

    // Publish event to outbox in the SAME transaction
    err := writer.WithTx(tx).Publish(context.Background(), outbox.Event{
        Topic: "user.created",
        Metadata: map[string]interface{}{
            "event-id":       "550e8400-e29b-41d4-a716-446655440000",
            "correlation-id": "request-123",
        },
        Payload: map[string]interface{}{
            "user_id": user.ID,
            "email":   user.Email,
            "name":    user.Name,
        },
    })

    if err != nil {
        // Handle error
        return
    }

    // Commit transaction (both user creation and outbox event)
    tx.Commit(context.Background())
}
```

### 3. Run the Dispatcher

The dispatcher reads events from the outbox table and publishes them to NATS.

#### Using Binary

```bash
# Create a config file (see examples/config.example.yaml)
./bin/pg-outbox-dispatcher -config config.yaml
```

#### Using Docker

```bash
docker run -d \
  -e DATABASE_DSN="postgresql://user:pass@host:5432/db" \
  -e NATS_URL="nats://nats:4222" \
  -p 8080:8080 \
  ghcr.io/senku-tech/pg-outbox-dispatcher:latest
```

#### Using Docker Compose

```yaml
version: '3.8'

services:
  pg-outbox-dispatcher:
    image: ghcr.io/senku-tech/pg-outbox-dispatcher:latest
    environment:
      DATABASE_DSN: "postgresql://user:pass@postgres:5432/mydb"
      NATS_URL: "nats://nats:4222"
      LOG_LEVEL: "info"
    ports:
      - "8080:8080"
    depends_on:
      - postgres
      - nats
    restart: unless-stopped
```

## Library Usage

### Simple Event Publishing

```go
import "github.com/senku-tech/pg-outbox/pkg/outbox"

// Create writer
writer := outbox.NewWriter(db)

// Publish single event
err := writer.Publish(ctx, outbox.Event{
    Topic: "order.created",
    Payload: orderData,
})
```

### With Transaction (Recommended)

```go
// Start transaction
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

// Your business logic
order := createOrder(tx, orderData)
inventory := updateInventory(tx, order.Items)

// Create writer with transaction
txWriter := writer.WithTx(tx)

// Publish events in same transaction
txWriter.Publish(ctx, outbox.Event{
    Topic: "order.created",
    Payload: order,
})

txWriter.Publish(ctx, outbox.Event{
    Topic: "inventory.updated",
    Payload: inventory,
})

// Commit (all-or-nothing)
tx.Commit(ctx)
```

### Batch Publishing

```go
events := []outbox.Event{
    {Topic: "user.created", Payload: user1},
    {Topic: "user.created", Payload: user2},
    {Topic: "user.created", Payload: user3},
}

// Publish all events in one database operation
writer.PublishBatch(ctx, events)
```

### Helper Functions

```go
// Simple event (topic + payload)
event := outbox.NewEvent("user.updated", userData)

// With metadata
event := outbox.NewEventWithMetadata(
    "order.placed",
    map[string]interface{}{
        "event-id": uuid.New().String(),
        "user-id": "user-123",
    },
    orderData,
)
```

## Dispatcher Configuration

Create a `config.yaml`:

```yaml
dispatcher:
  instance_id: "dispatcher-1"  # Unique ID for this instance
  batch_size: 100              # Events per batch
  max_attempts: 3              # Max retries before giving up
  poll_interval: 1s            # Polling frequency
  fallback_interval: 30s       # Fallback polling interval
  workers: 5                   # Concurrent batch processors
  shutdown_grace: 30s          # Graceful shutdown timeout

database:
  dsn: "postgresql://user:pass@localhost:5432/mydb"
  max_connections: 20
  max_idle: 10

nats:
  url: "nats://localhost:4222"
  max_retries: 5
  retry_delay: 2s
  jetstream:
    enabled: true
    stream_name: "EVENTS"
  # Optional: Transform topics before publishing
  topic_map:
    "user.created": "production.users.created"
    "order.placed": "production.orders.placed"

logging:
  level: "info"   # debug, info, warn, error
  format: "json"  # json, console

metrics:
  enabled: true
  port: 8080
  path: "/metrics"
```

### Environment Variables

Override configuration with environment variables:

- `DATABASE_DSN` - PostgreSQL connection string
- `NATS_URL` - NATS server URL
- `DISPATCHER_INSTANCE_ID` - Unique instance identifier
- `LOG_LEVEL` - Logging level

## Monitoring

### Health Checks

- `GET /health` - Overall health status
- `GET /ready` - Readiness check

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

### Metrics

Prometheus metrics available at `GET /metrics`:

- `outbox_events_published_total` - Total events published
- `outbox_events_failed_total` - Total failed events
- `outbox_batch_processing_duration_seconds` - Batch processing time
- `outbox_pending_events` - Current pending event count

## Scaling

### Horizontal Scaling

Run multiple dispatcher instances - PostgreSQL's `FOR UPDATE SKIP LOCKED` ensures no duplicate processing:

```yaml
# Kubernetes deployment with 3 replicas
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-outbox-dispatcher
spec:
  replicas: 3  # Multiple instances process in parallel
  selector:
    matchLabels:
      app: pg-outbox-dispatcher
  template:
    # ... container spec
```

Each instance claims different batches automatically - no coordination needed!

### Vertical Scaling

Increase `workers` in configuration for parallel batch processing within a single instance.

## Best Practices

### 1. Always Use Transactions

```go
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

// Business logic
createOrder(tx, order)

// Outbox event in same transaction
writer.WithTx(tx).Publish(ctx, event)

// Commit atomically
tx.Commit(ctx)
```

### 2. Include Correlation IDs

```go
event := outbox.NewEventWithMetadata(
    "order.created",
    map[string]interface{}{
        "correlation-id": requestID,
        "causation-id":   parentEventID,
        "user-id":        userID,
    },
    orderData,
)
```

### 3. Make Consumers Idempotent

Events are published **at-least-once**. Design consumers to handle duplicates:

```go
// In your consumer
if eventAlreadyProcessed(eventID) {
    return // Skip duplicate
}
processEvent(event)
markAsProcessed(eventID)
```

### 4. Monitor Stuck Events

```sql
-- Find events that might be stuck
SELECT * FROM outboxes
WHERE published_at IS NULL
  AND attempts > 0
  AND created_at < NOW() - INTERVAL '5 minutes';
```

### 5. Clean Up Old Events

```sql
-- Delete published events older than 30 days
SELECT cleanup_old_outbox_events(30);
```

## Database Schema

The outbox table structure:

```sql
CREATE TABLE outboxes (
    id UUID PRIMARY KEY,
    seq BIGSERIAL NOT NULL,              -- FIFO ordering
    created_at TIMESTAMPTZ NOT NULL,
    topic VARCHAR(255) NOT NULL,         -- NATS topic
    metadata JSONB,                      -- Event headers
    payload JSONB NOT NULL,              -- Event data
    published_at TIMESTAMPTZ,            -- NULL = not published
    attempts INTEGER DEFAULT 0,          -- Retry count
    last_error TEXT                      -- Error message if failed
) PARTITION BY RANGE (created_at);       -- Monthly partitions
```

## Deployment Examples

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-outbox-dispatcher
spec:
  replicas: 3
  selector:
    matchLabels:
      app: pg-outbox-dispatcher
  template:
    metadata:
      labels:
        app: pg-outbox-dispatcher
    spec:
      containers:
      - name: dispatcher
        image: ghcr.io/senku-tech/pg-outbox-dispatcher:latest
        env:
        - name: DATABASE_DSN
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: dsn
        - name: NATS_URL
          value: "nats://nats:4222"
        ports:
        - containerPort: 8080
          name: metrics
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
```

### Systemd Service

```ini
[Unit]
Description=pg-outbox Dispatcher
After=network.target postgresql.service

[Service]
Type=simple
User=outbox
Environment=DATABASE_DSN=postgresql://...
Environment=NATS_URL=nats://localhost:4222
ExecStart=/usr/local/bin/pg-outbox-dispatcher -config /etc/pg-outbox/config.yaml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

### Events not being published

1. Check dispatcher logs: `docker logs pg-outbox-dispatcher`
2. Verify NATS connection: `curl http://localhost:8080/health`
3. Check for stuck events (see SQL above)
4. Verify dispatcher is running: `curl http://localhost:8080/ready`

### High latency

1. Increase `workers` for parallel processing
2. Increase `batch_size` for higher throughput
3. Scale horizontally (add more dispatcher instances)
4. Check database connection pool settings

### Duplicate events

This is expected behavior (at-least-once delivery). Make your consumers idempotent:

```go
// Use unique event IDs from metadata
eventID := msg.Header.Get("Event-ID")
if alreadyProcessed(eventID) {
    msg.Ack()
    return
}
```

## Development

### Building

```bash
make build       # Build binary
make install     # Install to GOPATH/bin
make docker      # Build Docker image
```

### Testing

```bash
make test        # Run tests
make lint        # Run linter
make fmt         # Format code
```

### Running Locally

```bash
make run         # Run with example config
```

## License

MIT - See LICENSE file

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## Related Projects

- [NATS](https://nats.io/) - The messaging system
- [PostgreSQL](https://www.postgresql.org/) - The database
- [Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) - The pattern
