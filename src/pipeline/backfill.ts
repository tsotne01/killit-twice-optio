import { CheckpointManager, Customer } from '../checkpoint/manager';
import { ElasticsearchSink } from '../sinks/elasticsearch';
import { RabbitMQSink } from '../sinks/rabbitmq';
import { CircuitBreaker } from './circuitBreaker';
import { DLQManager } from '../dlq/manager';
import { config } from '../config';

export interface BackfillProgress {
  status: 'IDLE' | 'RUNNING' | 'PAUSED' | 'COMPLETED' | 'FAILED';
  lastProcessedId: number;
  recordsProcessed: number;
  totalRecords: number;
  progressPct: number;
  currentThroughputEps: number;
  startedAt: string | null;
  lastBatchDurationMs: number;
}

export class BackfillWorker {
  private esSink: ElasticsearchSink;
  private rmqSink: RabbitMQSink;
  private esBreaker: CircuitBreaker;
  private rmqBreaker: CircuitBreaker;
  private batchSize: number;

  private isRunning: boolean = false;
  private isPaused: boolean = false;
  private shouldStop: boolean = false;

  private startedAt: Date | null = null;
  private recentProcessedSamples: Array<{ count: number; timestamp: number }> = [];
  private lastBatchDurationMs: number = 0;

  constructor(
    esSink: ElasticsearchSink,
    rmqSink: RabbitMQSink,
    esBreaker: CircuitBreaker,
    rmqBreaker: CircuitBreaker
  ) {
    this.esSink = esSink;
    this.rmqSink = rmqSink;
    this.esBreaker = esBreaker;
    this.rmqBreaker = rmqBreaker;
    this.batchSize = config.pipeline.batchSize;
  }

  public async start(): Promise<void> {
    if (this.isRunning) {
      if (this.isPaused) {
        this.isPaused = false;
        await CheckpointManager.updateStatus('BACKFILL', 'RUNNING');
        console.log('[BackfillWorker] Resumed backfill from paused state');
      }
      return;
    }

    this.isRunning = true;
    this.isPaused = false;
    this.shouldStop = false;
    this.startedAt = new Date();

    await CheckpointManager.updateStatus('BACKFILL', 'RUNNING');
    console.log('[BackfillWorker] Starting keyset backfill ingestion loop...');

    // Run backfill loop asynchronously
    this.runLoop().catch(async (err) => {
      console.error('[BackfillWorker] Fatal backfill loop error:', err);
      this.isRunning = false;
      await CheckpointManager.updateStatus('BACKFILL', 'FAILED');
    });
  }

  public async pause(): Promise<void> {
    if (!this.isRunning || this.isPaused) return;
    this.isPaused = true;
    await CheckpointManager.updateStatus('BACKFILL', 'PAUSED');
    console.log('[BackfillWorker] Backfill worker paused');
  }

  public async stop(): Promise<void> {
    this.shouldStop = true;
    this.isRunning = false;
    this.isPaused = false;
    await CheckpointManager.updateStatus('BACKFILL', 'IDLE');
    console.log('[BackfillWorker] Backfill worker stopped');
  }

  private async runLoop(): Promise<void> {
    while (this.isRunning && !this.shouldStop) {
      if (this.isPaused) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        continue;
      }

      const checkpoint = await CheckpointManager.getCheckpoint('BACKFILL');
      const cursor = checkpoint.last_processed_id;

      // 1. Fetch bounded chunk using Keyset Cursor (Strictly NO OFFSET)
      const batchStart = Date.now();
      const batch = await CheckpointManager.fetchKeysetBatch(cursor, this.batchSize);

      if (batch.length === 0) {
        // Backfill completed!
        this.isRunning = false;
        await CheckpointManager.updateStatus('BACKFILL', 'COMPLETED');
        console.log('[BackfillWorker] Keyset backfill completed successfully. Reached end of dataset.');
        break;
      }

      const maxBatchId = batch[batch.length - 1].id;
      const maxBatchTimestamp = batch[batch.length - 1].updated_at;

      // 2. Dispatch concurrently to Elasticsearch and RabbitMQ protected by Circuit Breakers
      let rejectedItems: any[] = [];

      await Promise.all([
        this.esBreaker.executeWithResilience(
          async () => {
            const res = await this.esSink.bulkIndex(batch);
            if (res.rejected.length > 0) {
              rejectedItems = res.rejected;
            }
          },
          () => this.esSink.checkHealth()
        ),
        this.rmqBreaker.executeWithResilience(
          () => this.rmqSink.publishBatch(batch),
          () => this.rmqSink.checkHealth()
        ),
      ]);

      // 3. Route rejected items to DLQ (G4)
      if (rejectedItems.length > 0) {
        await DLQManager.routeBatchToDLQ(
          rejectedItems.map((r) => ({
            pipeline_mode: 'BACKFILL',
            record_id: r.customer.id,
            target_sink: 'ELASTICSEARCH',
            error_reason: r.errorReason,
            error_details: r.errorDetails,
            payload: r.customer,
          }))
        );
      }

      // 4. Durably commit checkpoint ONLY AFTER sinks acknowledge
      const validItemsCount = batch.length - rejectedItems.length;
      await CheckpointManager.commitCheckpoint(
        'BACKFILL',
        maxBatchId,
        new Date(maxBatchTimestamp),
        validItemsCount
      );

      this.lastBatchDurationMs = Date.now() - batchStart;
      this.recordProcessedSample(batch.length);
    }
  }

  private recordProcessedSample(count: number): void {
    const now = Date.now();
    this.recentProcessedSamples.push({ count, timestamp: now });
    // Keep only last 10 seconds of samples
    const tenSecondsAgo = now - 10000;
    this.recentProcessedSamples = this.recentProcessedSamples.filter(
      (s) => s.timestamp >= tenSecondsAgo
    );
  }

  public async getProgress(): Promise<BackfillProgress> {
    const cp = await CheckpointManager.getCheckpoint('BACKFILL');
    const now = Date.now();
    const tenSecondsAgo = now - 10000;
    const activeSamples = this.recentProcessedSamples.filter((s) => s.timestamp >= tenSecondsAgo);
    const totalRecentCount = activeSamples.reduce((acc, s) => acc + s.count, 0);
    const throughput = activeSamples.length > 0 ? Math.round(totalRecentCount / 10) : 0;

    const progressPct =
      cp.total_records > 0
        ? Math.min(100, Math.round((cp.records_processed / cp.total_records) * 1000) / 10)
        : 0;

    return {
      status: cp.status,
      lastProcessedId: cp.last_processed_id,
      recordsProcessed: cp.records_processed,
      totalRecords: cp.total_records,
      progressPct,
      currentThroughputEps: throughput,
      startedAt: this.startedAt ? this.startedAt.toISOString() : null,
      lastBatchDurationMs: this.lastBatchDurationMs,
    };
  }
}
