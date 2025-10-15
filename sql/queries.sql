-- name: ClaimOutboxBatch :many
-- Claims a batch of unpublished events using FOR UPDATE SKIP LOCKED
-- This provides automatic distributed coordination without explicit locks
-- Orders by seq for strict FIFO ordering
WITH claimed AS (
    SELECT id, created_at
    FROM outboxes
    WHERE published_at IS NULL
      AND attempts < @max_attempts::int4
    ORDER BY seq
    LIMIT @batch_size::int4
    FOR UPDATE SKIP LOCKED
)
UPDATE outboxes o
SET attempts = o.attempts + 1
FROM claimed c
WHERE o.id = c.id AND o.created_at = c.created_at
RETURNING o.id, o.created_at, o.topic, o.metadata, o.payload, o.published_at, o.attempts, o.last_error;

-- name: MarkEventPublished :exec
-- Mark an event as successfully published
UPDATE outboxes
SET published_at = NOW()
WHERE id = @id::uuid
  AND created_at = @created_at::timestamptz;

-- name: RecordEventError :exec
-- Record an error for a failed event
UPDATE outboxes
SET last_error = @error::varchar
WHERE id = @id::uuid
  AND created_at = @created_at::timestamptz;

-- name: GetStuckEvents :many
-- Find events that have been attempted but not published (for monitoring)
SELECT id, created_at, topic, attempts, last_error
FROM outboxes
WHERE published_at IS NULL
  AND attempts > 0
  AND created_at < NOW() - INTERVAL '5 minutes'
ORDER BY seq
LIMIT sqlc.arg('max_limit')::int4;

-- name: GetPublishingMetrics :one
-- Get metrics for monitoring
SELECT
    COUNT(*) FILTER (WHERE published_at IS NULL)::int8 as pending_count,
    COUNT(*) FILTER (WHERE published_at IS NULL AND attempts > 0)::int8 as retry_count,
    COUNT(*) FILTER (WHERE attempts >= @max_attempts::int4)::int8 as failed_count,
    MIN(created_at) FILTER (WHERE published_at IS NULL) as oldest_unpublished,
    MAX(created_at) FILTER (WHERE published_at IS NOT NULL) as latest_published,
    COUNT(*) FILTER (WHERE published_at IS NOT NULL AND published_at > NOW() - INTERVAL '1 minute')::int8 as published_last_minute
FROM outboxes
WHERE created_at > NOW() - INTERVAL '1 hour';

-- name: ResetStuckEvents :exec
-- Reset stuck events for retry (admin operation)
UPDATE outboxes
SET attempts = 0,
    last_error = NULL
WHERE published_at IS NULL
  AND attempts >= @max_attempts::int4
  AND created_at BETWEEN @start_time::timestamptz AND @end_time::timestamptz;
