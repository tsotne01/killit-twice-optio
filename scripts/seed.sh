#!/usr/bin/env bash
set -euo pipefail

TOTAL_RECORDS="${1:-1000000}"
BATCH_SIZE="${2:-100000}"

echo "========================================================="
echo " Seeding ${TOTAL_RECORDS} customer records into PostgreSQL..."
echo "========================================================="

START_TIME=$(date +%s)

docker exec -i optio-postgres psql -U optio_user -d optio_db <<EOSQL
-- Reset existing tables for clean seed
TRUNCATE TABLE customers RESTART IDENTITY CASCADE;
TRUNCATE TABLE replication_dlq RESTART IDENTITY;

-- High-throughput bulk generation using generate_series and batch looping
DO \$\$
DECLARE
    v_total INT := ${TOTAL_RECORDS};
    v_batch INT := ${BATCH_SIZE};
    v_start INT := 1;
    v_end INT;
BEGIN
    WHILE v_start <= v_total LOOP
        v_end := LEAST(v_start + v_batch - 1, v_total);
        RAISE NOTICE 'Inserting rows % to %...', v_start, v_end;

        INSERT INTO customers (
            external_id, email, first_name, last_name, status, version, balance, attributes, created_at, updated_at
        )
        SELECT 
            'cust_' || i,
            'user' || i || '@optio.platform',
            'FirstName_' || (i % 1000),
            'LastName_' || (i % 1000),
            CASE 
                WHEN i % 20 = 0 THEN 'INACTIVE'
                WHEN i % 100 = 0 THEN 'SUSPENDED'
                ELSE 'ACTIVE'
            END,
            1,
            ROUND(((i * 17) % 50000 + 10.5)::numeric, 2),
            json_build_object(
                'tier', CASE WHEN i % 3 = 0 THEN 'ENTERPRISE' WHEN i % 2 = 0 THEN 'PRO' ELSE 'STANDARD' END,
                'score', (i % 100),
                'verified', (i % 2 = 0)
            )::jsonb,
            NOW() - (interval '1 second' * (v_total - i)),
            NOW() - (interval '1 second' * (v_total - i))
        FROM generate_series(v_start, v_end) AS s(i);

        v_start := v_end + 1;
    END LOOP;
END \$\$;

-- Reset replication checkpoints
UPDATE replication_checkpoints 
SET total_records = (SELECT count(*) FROM customers), 
    records_processed = 0, 
    last_processed_id = 0, 
    status = 'IDLE',
    updated_at = NOW()
WHERE pipeline_mode = 'BACKFILL';

UPDATE replication_checkpoints 
SET last_processed_id = 0,
    last_processed_timestamp = NOW(),
    status = 'IDLE',
    updated_at = NOW()
WHERE pipeline_mode = 'INCREMENTAL';

EOSQL

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

COUNT=$(docker exec -i optio-postgres psql -U optio_user -d optio_db -t -A -c 'SELECT count(*) FROM customers;')

echo "========================================================="
echo " Seeding completed successfully in ${DURATION} seconds!"
echo " Total records in PostgreSQL: ${COUNT}"
echo "========================================================="
