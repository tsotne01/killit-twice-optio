import { query } from '../db/postgres';
import { ElasticsearchSink } from '../sinks/elasticsearch';
import { RabbitMQSink } from '../sinks/rabbitmq';

export interface DLQRecord {
  id: number;
  pipeline_mode: string;
  record_id: number;
  target_sink: string;
  error_reason: string;
  error_details: any;
  payload: any;
  retry_count: number;
  status: 'PENDING' | 'REPLAYED' | 'DISCARDED';
  created_at: Date;
  updated_at: Date;
}

export interface RouteToDLQItem {
  pipeline_mode: string;
  record_id: number;
  target_sink: 'ELASTICSEARCH' | 'RABBITMQ';
  error_reason: string;
  error_details?: any;
  payload: any;
}

export class DLQManager {
  /**
   * Persists rejected items into replication_dlq with full failure context.
   */
  public static async routeBatchToDLQ(items: RouteToDLQItem[]): Promise<void> {
    if (items.length === 0) return;

    for (const item of items) {
      await query(
        `INSERT INTO replication_dlq (
          pipeline_mode, record_id, target_sink, error_reason, error_details, payload, status
        ) VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')`,
        [
          item.pipeline_mode,
          item.record_id,
          item.target_sink,
          item.error_reason,
          JSON.stringify(item.error_details || {}),
          JSON.stringify(item.payload),
        ]
      );
    }
  }

  /**
   * Returns count of unresolved items in DLQ.
   */
  public static async getPendingCount(): Promise<number> {
    const res = await query<{ count: string }>(
      `SELECT count(*)::text as count FROM replication_dlq WHERE status = 'PENDING'`
    );
    return parseInt(res.rows[0]?.count || '0', 10);
  }

  /**
   * Lists records residing in the Dead Letter Queue.
   */
  public static async list(limit: number = 50, offset: number = 0): Promise<{ total: number; items: DLQRecord[] }> {
    const countRes = await query<{ count: string }>('SELECT count(*)::text as count FROM replication_dlq');
    const total = parseInt(countRes.rows[0]?.count || '0', 10);

    const itemsRes = await query<DLQRecord>(
      `SELECT id, pipeline_mode, record_id::bigint, target_sink, error_reason, error_details, payload, retry_count, status, created_at, updated_at
       FROM replication_dlq
       ORDER BY id DESC
       LIMIT $1 OFFSET $2`,
      [limit, offset]
    );

    const items = itemsRes.rows.map((row) => ({
      ...row,
      id: Number(row.id),
      record_id: Number(row.record_id),
    }));

    return { total, items };
  }

  /**
   * Replays a specific failed record from DLQ into its target sink.
   */
  public static async replay(
    id: number,
    esSink: ElasticsearchSink,
    rmqSink: RabbitMQSink
  ): Promise<{ success: boolean; message: string }> {
    const res = await query<DLQRecord>(
      `SELECT * FROM replication_dlq WHERE id = $1`,
      [id]
    );

    if (res.rows.length === 0) {
      return { success: false, message: `DLQ record ${id} not found` };
    }

    const dlqItem = res.rows[0];
    const customer = typeof dlqItem.payload === 'string' ? JSON.parse(dlqItem.payload) : dlqItem.payload;

    try {
      if (dlqItem.target_sink === 'ELASTICSEARCH') {
        const result = await esSink.bulkIndex([customer]);
        if (result.rejected.length > 0) {
          const reason = result.rejected[0].errorReason;
          await query(
            `UPDATE replication_dlq 
             SET retry_count = retry_count + 1, error_reason = $1, updated_at = NOW() 
             WHERE id = $2`,
            [reason, id]
          );
          return { success: false, message: `Replay rejected by Elasticsearch: ${reason}` };
        }
      } else if (dlqItem.target_sink === 'RABBITMQ') {
        await rmqSink.publishBatch([customer]);
      }

      await query(
        `UPDATE replication_dlq 
         SET status = 'REPLAYED', retry_count = retry_count + 1, updated_at = NOW() 
         WHERE id = $1`,
        [id]
      );
      return { success: true, message: `Record ${id} successfully replayed to ${dlqItem.target_sink}` };
    } catch (err: any) {
      await query(
        `UPDATE replication_dlq 
         SET retry_count = retry_count + 1, error_reason = $1, updated_at = NOW() 
         WHERE id = $2`,
        [err.message || 'Replay error', id]
      );
      return { success: false, message: `Replay failed: ${err.message}` };
    }
  }

  /**
   * Marks a DLQ record as discarded.
   */
  public static async discard(id: number): Promise<void> {
    await query(
      `UPDATE replication_dlq SET status = 'DISCARDED', updated_at = NOW() WHERE id = $1`,
      [id]
    );
  }
}
