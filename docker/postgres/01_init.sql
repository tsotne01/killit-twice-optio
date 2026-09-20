-- 01_init.sql: Schema initialization for Kill It Twice replication pipeline

-- 1. Source Table: customers
CREATE TABLE IF NOT EXISTS customers (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(64) UNIQUE NOT NULL,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    version INT NOT NULL DEFAULT 1,
    balance NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Backfill Keyset Pagination Index (Crucial for O(log N) streaming)
CREATE INDEX IF NOT EXISTS idx_customers_keyset ON customers (id ASC);

-- Incremental Polling Watermark Index
CREATE INDEX IF NOT EXISTS idx_customers_incremental ON customers (updated_at ASC, id ASC);

-- 2. Checkpoints Table: replication_checkpoints
CREATE TABLE IF NOT EXISTS replication_checkpoints (
    pipeline_mode VARCHAR(32) PRIMARY KEY, -- 'BACKFILL', 'INCREMENTAL'
    last_processed_id BIGINT NOT NULL DEFAULT 0,
    last_processed_timestamp TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    status VARCHAR(32) NOT NULL DEFAULT 'IDLE', -- 'IDLE', 'RUNNING', 'PAUSED', 'COMPLETED', 'FAILED'
    records_processed BIGINT NOT NULL DEFAULT 0,
    total_records BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Initialize default checkpoint records if not present
INSERT INTO replication_checkpoints (pipeline_mode, last_processed_id, last_processed_timestamp, status, records_processed, total_records)
VALUES 
    ('BACKFILL', 0, '1970-01-01 00:00:00+00', 'IDLE', 0, 0),
    ('INCREMENTAL', 0, NOW(), 'IDLE', 0, 0)
ON CONFLICT (pipeline_mode) DO NOTHING;

-- 3. Dead Letter Queue Table: replication_dlq
CREATE TABLE IF NOT EXISTS replication_dlq (
    id BIGSERIAL PRIMARY KEY,
    pipeline_mode VARCHAR(32) NOT NULL, -- 'BACKFILL', 'INCREMENTAL'
    record_id BIGINT NOT NULL,
    target_sink VARCHAR(32) NOT NULL,   -- 'ELASTICSEARCH', 'RABBITMQ'
    error_reason TEXT NOT NULL,
    error_details JSONB,
    payload JSONB NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'REPLAYED', 'DISCARDED'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON replication_dlq (status, target_sink);
