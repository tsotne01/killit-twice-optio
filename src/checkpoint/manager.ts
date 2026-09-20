import { query } from '../db/postgres';

export interface Customer {
  id: number;
  external_id: string;
  email: string;
  first_name: string;
  last_name: string;
  status: string;
  version: number;
  balance: number;
  attributes: Record<string, any>;
  created_at: Date;
  updated_at: Date;
}

export type PipelineMode = 'BACKFILL' | 'INCREMENTAL';
export type CheckpointStatus = 'IDLE' | 'RUNNING' | 'PAUSED' | 'COMPLETED' | 'FAILED';

export interface Checkpoint {
  pipeline_mode: PipelineMode;
  last_processed_id: number;
  last_processed_timestamp: Date;
  status: CheckpointStatus;
  records_processed: number;
  total_records: number;
  updated_at: Date;
}

export class CheckpointManager {
  /**
   * Retrieves the current checkpoint state for the specified pipeline mode.
   */
  public static async getCheckpoint(mode: PipelineMode): Promise<Checkpoint> {
    const res = await query<Checkpoint>(
      `SELECT pipeline_mode, 
              last_processed_id::bigint, 
              last_processed_timestamp, 
              status, 
              records_processed::bigint, 
              total_records::bigint, 
              updated_at 
       FROM replication_checkpoints 
       WHERE pipeline_mode = $1`,
      [mode]
    );

    if (res.rows.length === 0) {
      throw new Error(`[CheckpointManager] No checkpoint found for pipeline mode: ${mode}`);
    }

    const row = res.rows[0];
    return {
      ...row,
      last_processed_id: Number(row.last_processed_id),
      records_processed: Number(row.records_processed),
      total_records: Number(row.total_records),
    };
  }

  /**
   * Durably commits a checkpoint after sinks have acknowledged the batch.
   */
  public static async commitCheckpoint(
    mode: PipelineMode,
    lastId: number,
    lastTimestamp: Date,
    recordsIncrement: number,
    newStatus?: CheckpointStatus
  ): Promise<void> {
    const statusClause = newStatus ? ', status = $5' : '';
    const params: any[] = [lastId, lastTimestamp, recordsIncrement, mode];
    if (newStatus) {
      params.push(newStatus);
    }

    await query(
      `UPDATE replication_checkpoints 
       SET last_processed_id = $1,
           last_processed_timestamp = $2,
           records_processed = records_processed + $3,
           updated_at = NOW()
           ${statusClause}
       WHERE pipeline_mode = $4`,
      params
    );
  }

  /**
   * Updates pipeline operating status.
   */
  public static async updateStatus(mode: PipelineMode, status: CheckpointStatus): Promise<void> {
    await query(
      `UPDATE replication_checkpoints 
       SET status = $1, updated_at = NOW() 
       WHERE pipeline_mode = $2`,
      [status, mode]
    );
  }

  /**
   * High-throughput keyset cursor query for backfill.
   * STRICT INVARIANT: Never use OFFSET / LIMIT.
   */
  public static async fetchKeysetBatch(lastId: number, limit: number): Promise<Customer[]> {
    const res = await query<Customer>(
      `SELECT id, external_id, email, first_name, last_name, status, version, balance::float, attributes, created_at, updated_at
       FROM customers
       WHERE id > $1
       ORDER BY id ASC
       LIMIT $2`,
      [lastId, limit]
    );
    return res.rows.map((row) => ({
      ...row,
      id: Number(row.id),
      version: Number(row.version),
      balance: Number(row.balance),
    }));
  }

  /**
   * Watermark polling query for incremental changes.
   */
  public static async fetchIncrementalBatch(
    lastTimestamp: Date,
    lastId: number,
    limit: number
  ): Promise<Customer[]> {
    const res = await query<Customer>(
      `SELECT id, external_id, email, first_name, last_name, status, version, balance::float, attributes, created_at, updated_at
       FROM customers
       WHERE (updated_at > $1) OR (updated_at = $1 AND id > $2)
       ORDER BY updated_at ASC, id ASC
       LIMIT $3`,
      [lastTimestamp, lastId, limit]
    );
    return res.rows.map((row) => ({
      ...row,
      id: Number(row.id),
      version: Number(row.version),
      balance: Number(row.balance),
    }));
  }

  /**
   * Returns the total number of customer records in source.
   */
  public static async getTotalSourceCount(): Promise<number> {
    const res = await query<{ count: string }>('SELECT count(*)::text as count FROM customers');
    return parseInt(res.rows[0]?.count || '0', 10);
  }

  /**
   * Computes replication lag (time lag in seconds and un-replicated record count).
   */
  public static async getIncrementalLag(): Promise<{ lagSeconds: number; lagRecords: number }> {
    const checkpoint = await this.getCheckpoint('INCREMENTAL');
    const lagQuery = await query<{ max_ts: Date | null; pending_records: string }>(
      `SELECT 
         MAX(updated_at) as max_ts,
         COUNT(*) FILTER (WHERE updated_at > $1 OR (updated_at = $1 AND id > $2)) as pending_records
       FROM customers`,
      [checkpoint.last_processed_timestamp, checkpoint.last_processed_id]
    );

    const row = lagQuery.rows[0];
    const maxTs = row.max_ts ? new Date(row.max_ts).getTime() : Date.now();
    const checkpointTs = new Date(checkpoint.last_processed_timestamp).getTime();
    const lagSeconds = Math.max(0, Math.round((maxTs - checkpointTs) / 1000));
    const lagRecords = parseInt(row.pending_records || '0', 10);

    return { lagSeconds, lagRecords };
  }
}
