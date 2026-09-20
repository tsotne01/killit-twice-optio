import { ElasticsearchSink } from '../sinks/elasticsearch';
import { RabbitMQSink } from '../sinks/rabbitmq';
import { AuditConsumer } from '../consumer/auditConsumer';
import { CircuitBreaker } from './circuitBreaker';
import { BackfillWorker, BackfillProgress } from './backfill';
import { IncrementalWorker, IncrementalProgress } from './incremental';
import { DLQManager } from '../dlq/manager';
import { CheckpointManager } from '../checkpoint/manager';

export interface PipelineTelemetry {
  timestamp: string;
  uptimeSeconds: number;
  memory: {
    rssMb: number;
    heapUsedMb: number;
    heapTotalMb: number;
    limitMb: number;
  };
  backfill: BackfillProgress;
  incremental: IncrementalProgress;
  dlq: {
    pendingCount: number;
  };
  sinks: {
    elasticsearch: {
      isAvailable: boolean;
      documentCount: number;
      circuitBreaker: any;
    };
    rabbitmq: {
      isAvailable: boolean;
      circuitBreaker: any;
    };
  };
  auditConsumer: any;
  source: {
    totalRecords: number;
  };
}

export class PipelineManager {
  private static instance: PipelineManager | null = null;

  public esSink: ElasticsearchSink;
  public rmqSink: RabbitMQSink;
  public auditConsumer: AuditConsumer;
  public esBreaker: CircuitBreaker;
  public rmqBreaker: CircuitBreaker;
  public backfillWorker: BackfillWorker;
  public incrementalWorker: IncrementalWorker;

  private startTime: Date = new Date();

  private constructor() {
    this.esSink = new ElasticsearchSink();
    this.rmqSink = new RabbitMQSink();
    this.auditConsumer = new AuditConsumer();
    this.esBreaker = new CircuitBreaker('elasticsearch');
    this.rmqBreaker = new CircuitBreaker('rabbitmq');

    this.backfillWorker = new BackfillWorker(
      this.esSink,
      this.rmqSink,
      this.esBreaker,
      this.rmqBreaker
    );

    this.incrementalWorker = new IncrementalWorker(
      this.esSink,
      this.rmqSink,
      this.esBreaker,
      this.rmqBreaker
    );
  }

  public static getInstance(): PipelineManager {
    if (!this.instance) {
      this.instance = new PipelineManager();
    }
    return this.instance;
  }

  public async init(): Promise<void> {
    console.log('[PipelineManager] Initializing pipeline dependencies...');
    await this.rmqSink.init();
    await this.auditConsumer.start();
    console.log('[PipelineManager] All dependencies initialized.');
  }

  public async startAll(): Promise<void> {
    await this.init();
    await this.backfillWorker.start();
    await this.incrementalWorker.start();
  }

  public async getTelemetry(): Promise<PipelineTelemetry> {
    const mem = process.memoryUsage();
    const backfill = await this.backfillWorker.getProgress();
    const incremental = await this.incrementalWorker.getProgress();
    const dlqPending = await DLQManager.getPendingCount();
    const totalSource = await CheckpointManager.getTotalSourceCount();

    let docCount = 0;
    let esAvailable = false;
    try {
      esAvailable = await this.esSink.checkHealth();
      if (esAvailable) {
        docCount = await this.esSink.getDocumentCount();
      }
    } catch {
      esAvailable = false;
    }

    const rmqAvailable = await this.rmqSink.checkHealth();

    return {
      timestamp: new Date().toISOString(),
      uptimeSeconds: Math.floor((Date.now() - this.startTime.getTime()) / 1000),
      memory: {
        rssMb: Math.round((mem.rss / (1024 * 1024)) * 10) / 10,
        heapUsedMb: Math.round((mem.heapUsed / (1024 * 1024)) * 10) / 10,
        heapTotalMb: Math.round((mem.heapTotal / (1024 * 1024)) * 10) / 10,
        limitMb: 512,
      },
      backfill,
      incremental,
      dlq: {
        pendingCount: dlqPending,
      },
      sinks: {
        elasticsearch: {
          isAvailable: esAvailable,
          documentCount: docCount,
          circuitBreaker: this.esBreaker.getState(),
        },
        rabbitmq: {
          isAvailable: rmqAvailable,
          circuitBreaker: this.rmqBreaker.getState(),
        },
      },
      auditConsumer: this.auditConsumer.getStats(),
      source: {
        totalRecords: totalSource,
      },
    };
  }

  public async shutdown(): Promise<void> {
    console.log('[PipelineManager] Gracefully shutting down...');
    await this.backfillWorker.stop();
    await this.incrementalWorker.stop();
    await this.auditConsumer.stop();
    await this.esSink.close();
    await this.rmqSink.close();
    console.log('[PipelineManager] Shutdown completed.');
  }
}
