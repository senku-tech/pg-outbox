# pg-outbox

A reliable PostgreSQL-based transactional outbox pattern implementation for publishing events to NATS/JetStream.

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
┌─────────────┐
│ Application │
│   Service   │──┐
└─────────────┘  │
                 │ 1. Insert event in transaction
                 ▼
         ┌───────────────┐
         │   Outbox      │
         │   Table       │
         │ (PostgreSQL)  │
         └───────────────┘
                 │
                 │ 2. Dispatcher claims batch
                 ▼
         ┌───────────────┐
         │  pg-outbox    │
         │  Dispatcher   │
         └───────────────┘
                 │
                 │ 3. Publish events
                 ▼
         ┌───────────────┐
         │     NATS      │
         │  JetStream    │
         └───────────────┘
```

## Installation

```bash
go get github.com/senku-tech/pg-outbox
```

## Database Setup

### 1. Run the schema migration

```sql
-- See sql/schema.sql for the complete schema
-- This creates the partitioned outboxes table with indexes
```

### 2. Insert events from your application

```go
// In your application code, insert events in the same transaction as your business logic
tx, err := db.Begin()
defer tx.Rollback()

// Your business logic
user := createUser(tx, userData)

// Insert outbox event in the same transaction
_, err = tx.Exec(`
    INSERT INTO outboxes (topic, metadata, payload)
    VALUES ($1, $2, $3)
`, "user.created", metadata, payload)

tx.Commit()
```

## Dispatcher Setup

### Configuration

Create a `config.yaml`:

```yaml
dispatcher:
  instance_id: "dispatcher-1"
  batch_size: 100
  max_attempts: 3
  poll_interval: 1s
  fallback_interval: 30s
  workers: 5
  shutdown_grace: 30s

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
  topic_map:
    "user.created": "app.users.created"
    "order.placed": "app.orders.placed"

logging:
  level: "info"
  format: "json"

metrics:
  enabled: true
  port: 8080
  path: "/metrics"
```

### Running the Dispatcher

```go
package main

import (
    "context"
    "github.com/senku-tech/pg-outbox/pkg/config"
    "github.com/senku-tech/pg-outbox/pkg/dispatcher"
    "go.uber.org/zap"
)

func main() {
    logger, _ := zap.NewProduction()

    cfg, err := config.LoadConfig("config.yaml")
    if err != nil {
        logger.Fatal("Failed to load config", zap.Error(err))
    }

    service, err := dispatcher.NewService(cfg, logger)
    if err != nil {
        logger.Fatal("Failed to create service", zap.Error(err))
    }

    ctx := context.Background()
    if err := service.Start(ctx); err != nil {
        logger.Fatal("Failed to start service", zap.Error(err))
    }

    // Wait for shutdown signal...
}
```

## Environment Variables

Override configuration with environment variables:

- `DATABASE_DSN`: PostgreSQL connection string
- `NATS_URL`: NATS server URL
- `DISPATCHER_INSTANCE_ID`: Unique instance identifier
- `LOG_LEVEL`: Logging level (debug, info, warn, error)

## Event Structure

### Outbox Table Schema

```sql
CREATE TABLE outboxes (
    id UUID PRIMARY KEY,
    seq BIGSERIAL NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    topic VARCHAR(255) NOT NULL,
    metadata JSONB,              -- Event headers/metadata
    payload JSONB NOT NULL,      -- Event data
    published_at TIMESTAMPTZ,    -- NULL = not published
    attempts INTEGER DEFAULT 0,
    last_error TEXT
) PARTITION BY RANGE (created_at);
```

### Metadata Format

The `metadata` field can contain:

```json
{
  "event-id": "550e8400-e29b-41d4-a716-446655440000",
  "correlation-id": "request-123",
  "user-id": "user-456",
  "source": "user-service"
}
```

Metadata fields are automatically added as NATS message headers.

### Payload Format

The `payload` field contains your event data:

```json
{
  "user_id": "123",
  "email": "user@example.com",
  "name": "John Doe",
  "created_at": "2024-01-01T00:00:00Z"
}
```

## Topic Mapping

Transform topics before publishing to NATS:

```yaml
nats:
  topic_map:
    "user.created": "production.users.created"
    "order.*": "production.orders.*"
```

## Monitoring

### Health Checks

- `GET /health` - Overall health status
- `GET /ready` - Readiness check

### Metrics

- `GET /metrics` - Prometheus metrics

Key metrics:
- `outbox_events_published_total` - Total events published
- `outbox_events_failed_total` - Total failed events
- `outbox_batch_processing_duration_seconds` - Batch processing time
- `outbox_pending_events` - Current pending event count

## Best Practices

### 1. Always use transactions

```go
tx, _ := db.Begin()
defer tx.Rollback()

// Business logic
createOrder(tx, order)

// Outbox event in same transaction
insertOutboxEvent(tx, "order.created", orderData)

tx.Commit()
```

### 2. Include correlation IDs

```json
{
  "metadata": {
    "correlation-id": "request-abc-123",
    "causation-id": "event-xyz-456"
  }
}
```

### 3. Monitor stuck events

```sql
SELECT * FROM outboxes
WHERE published_at IS NULL
  AND attempts > 0
  AND created_at < NOW() - INTERVAL '5 minutes';
```

### 4. Clean up old events

```sql
-- Delete events older than 30 days
SELECT cleanup_old_outbox_events(30);
```

### 5. Partition maintenance

Create partitions ahead of time:

```sql
-- Create next month's partition
SELECT create_monthly_partition('outboxes', '2024-02-01');
```

## Deployment

### Docker

```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o dispatcher ./cmd/dispatcher

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/dispatcher /dispatcher
COPY config.yaml /config.yaml
CMD ["/dispatcher", "-config", "/config.yaml"]
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outbox-dispatcher
spec:
  replicas: 3  # Multiple instances for high availability
  selector:
    matchLabels:
      app: outbox-dispatcher
  template:
    metadata:
      labels:
        app: outbox-dispatcher
    spec:
      containers:
      - name: dispatcher
        image: your-registry/outbox-dispatcher:latest
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
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
```

## Scaling

- **Horizontal Scaling**: Run multiple dispatcher instances - `FOR UPDATE SKIP LOCKED` ensures no duplicate processing
- **Vertical Scaling**: Increase `workers` in configuration for parallel batch processing
- **Partition Pruning**: Old partitions can be dropped for efficient storage

## Troubleshooting

### Events not being published

1. Check dispatcher logs for errors
2. Verify NATS connection: `curl http://localhost:8080/health`
3. Check for stuck events: See monitoring SQL above

### High latency

1. Increase `workers` in configuration
2. Increase `batch_size` for higher throughput
3. Check database connection pool settings

### Duplicate events

Events are published **at-least-once**. Consumers should be idempotent or use deduplication:

```go
// In your consumer
if eventAlreadyProcessed(eventID) {
    return // Skip duplicate
}
processEvent(event)
markAsProcessed(eventID)
```

## License

MIT

## Contributing

Contributions welcome! Please open an issue or submit a pull request.
