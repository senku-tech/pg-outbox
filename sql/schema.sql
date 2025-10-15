-- Outbox table for the transactional outbox pattern
-- This table stores events that need to be published to NATS
CREATE TABLE IF NOT EXISTS outboxes (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    seq BIGSERIAL NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    topic VARCHAR(255) NOT NULL,
    metadata JSONB,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_outboxes_seq ON outboxes (seq) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outboxes_published_at ON outboxes (published_at);
CREATE INDEX IF NOT EXISTS idx_outboxes_topic ON outboxes (topic);
CREATE INDEX IF NOT EXISTS idx_outboxes_attempts ON outboxes (attempts) WHERE published_at IS NULL;

-- Create a function to automatically create monthly partitions
CREATE OR REPLACE FUNCTION create_outbox_partition()
RETURNS TRIGGER AS $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    partition_date := DATE_TRUNC('month', NEW.created_at);
    partition_name := 'outboxes_' || TO_CHAR(partition_date, 'YYYY_MM');
    start_date := partition_date;
    end_date := partition_date + INTERVAL '1 month';

    -- Create partition if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM pg_class
        WHERE relname = partition_name
    ) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF outboxes FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );

        RAISE NOTICE 'Created partition: %', partition_name;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically create partitions
CREATE TRIGGER trigger_create_outbox_partition
    BEFORE INSERT ON outboxes
    FOR EACH ROW
    EXECUTE FUNCTION create_outbox_partition();

-- Create initial partitions for current and next month
DO $$
DECLARE
    current_month DATE := DATE_TRUNC('month', CURRENT_DATE);
    next_month DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month');
BEGIN
    -- Current month partition
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS outboxes_%s PARTITION OF outboxes FOR VALUES FROM (%L) TO (%L)',
        TO_CHAR(current_month, 'YYYY_MM'),
        current_month,
        next_month
    );

    -- Next month partition
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS outboxes_%s PARTITION OF outboxes FOR VALUES FROM (%L) TO (%L)',
        TO_CHAR(next_month, 'YYYY_MM'),
        next_month,
        next_month + INTERVAL '1 month'
    );
END $$;

-- Optional: Create a function to clean up old published events
CREATE OR REPLACE FUNCTION cleanup_old_outbox_events(retention_days INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM outboxes
    WHERE published_at IS NOT NULL
      AND published_at < NOW() - (retention_days || ' days')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Optional: Create a scheduled job to clean up old events (requires pg_cron extension)
-- SELECT cron.schedule('cleanup-outbox', '0 2 * * *', 'SELECT cleanup_old_outbox_events(30);');

COMMENT ON TABLE outboxes IS 'Transactional outbox pattern table for reliable event publishing';
COMMENT ON COLUMN outboxes.id IS 'Unique identifier for the outbox event';
COMMENT ON COLUMN outboxes.seq IS 'Monotonically increasing sequence number for FIFO ordering';
COMMENT ON COLUMN outboxes.created_at IS 'Timestamp when the event was created';
COMMENT ON COLUMN outboxes.topic IS 'NATS topic/subject to publish to';
COMMENT ON COLUMN outboxes.metadata IS 'Event metadata (headers, event_id, etc.)';
COMMENT ON COLUMN outboxes.payload IS 'Event payload data';
COMMENT ON COLUMN outboxes.published_at IS 'Timestamp when successfully published to NATS';
COMMENT ON COLUMN outboxes.attempts IS 'Number of publish attempts';
COMMENT ON COLUMN outboxes.last_error IS 'Last error message if publish failed';
