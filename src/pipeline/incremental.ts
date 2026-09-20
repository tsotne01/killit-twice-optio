import { CheckpointManager } from '../checkpoint/manager';
import { ElasticsearchSink } from '../sinks/elasticsearch';
import { RabbitMQSink } from '../sinks/rabbitmq';
import { CircuitBreaker } from './circuitBreaker';
import { DLQManager } from '../dlq/manager';
import { config } from '../config';

export interface IncrementalProgress {
  status: 'IDLE' | 'RUNNING' | 'PAUSED' | 'FAILED';
  lastProcessedTimestamp: string;
  lastProcessedId: number;
  lagSeconds: number;
  lagRecords: number;
  recordsProcessed: number;
  pollIntervalMs: number;
}

export class IncrementalWorker {
  private esSink: ElasticsearchSink;
  private rmqSink: RabbitMQSink;
  private esBreaker: CircuitBreaker;
  private rmqBreaker: CircuitBreaker;
  private pollIntervalMs: number;

  private isRunning: boolean = false;
  private isPaused: boolean = false;
  private shouldStop: boolean = false;

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
    this.pollIntervalMs = config.pipeline.pollIntervalMs;
  }

  public async start(): Promise<void> {
    if (this.isRunning) {
      if (this.isPaused) {
        this.isPaused = false;
        await CheckpointManager.updateStatus('INCREMENTAL', 'RUNNING');
        console.log('[IncrementalWorker] Resumed incremental sync from paused state');
      }
      return;
    }

    this.isRunning = true;
    this.isPaused = false;
    this.shouldStop = false;

    await CheckpointManager.updateStatus('INCREMENTAL', 'RUNNING');
    console.log('[IncrementalWorker] Starting continuous incremental sync polling loop...');

    this.runLoop().catch(async (err) => {
      console.error('[IncrementalWorker] Fatal incremental sync loop error:', err);
      this.isRunning = false;
      await CheckpointManager.updateStatus('INCREMENTAL', 'FAILED');
    });
  }

  public async pause(): Promise<void> {
    if (!this.isRunning || this.isPaused) return;
    this.isPaused = true;
    await CheckpointManager.updateStatus('INCREMENTAL', 'PAUSED');
    console.log('[IncrementalWorker] Incremental worker paused');
  }

  public async stop(): Promise<void> {
    this.shouldStop = true;
    this.isRunning = false;
    this.isPaused = false;
    await CheckpointManager.updateStatus('INCREMENTAL', 'IDLE');
    console.log('[IncrementalWorker] Incremental worker stopped');
  }

  private async runLoop(): Promise<void> {
    while (this.isRunning && !this.shouldStop) {
      if (this.isPaused) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        continue;
      }

      try {
        const cp = await CheckpointManager.getCheckpoint('INCREMENTAL');
        const batch = await CheckpointManager.fetchIncrementalBatch(
          cp.last_processed_timestamp,
          cp.last_processed_id,
          100
        );

        if (batch.length > 0) {
          const maxBatchId = batch[batch.length - 1].id;
          const maxBatchTimestamp = batch[batch.length - 1].updated_at;

          let rejectedItems: any[] = [];

          // Dispatch with Circuit Breaker resilience
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

          // Route rejected items to DLQ (G4)
          if (rejectedItems.length > 0) {
            await DLQManager.routeBatchToDLQ(
              rejectedItems.map((r) => ({
                pipeline_mode: 'INCREMENTAL',
                record_id: r.customer.id,
                target_sink: 'ELASTICSEARCH',
                error_reason: r.errorReason,
                error_details: r.errorDetails,
                payload: r.customer,
              }))
            );
          }

          // Advance incremental watermark
          const validItemsCount = batch.length - rejectedItems.length;
          await CheckpointManager.commitCheckpoint(
            'INCREMENTAL',
            maxBatchId,
            new Date(maxBatchTimestamp),
            validItemsCount
          );
        }
      } catch (err: any) {
        console.error('[IncrementalWorker] Error during incremental poll iteration:', err.message);
      }

      // Respect poll interval
      await new Promise((resolve) => setTimeout(resolve, this.pollIntervalMs));
    }
  }

  public async getProgress(): Promise<IncrementalProgress> {
    const cp = await CheckpointManager.getCheckpoint('INCREMENTAL');
    const { lagSeconds, lagRecords } = await CheckpointManager.getIncrementalLag();

    return {
      status: cp.status as any,
      lastProcessedTimestamp: cp.last_processed_timestamp.toISOString(),
      lastProcessedId: cp.last_processed_id,
      lagSeconds,
      lagRecords,
      recordsProcessed: cp.records_processed,
      pollIntervalMs: this.pollIntervalMs,
    };
  }
}
