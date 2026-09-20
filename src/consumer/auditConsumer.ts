import amqp, { ChannelModel, Channel, ConsumeMessage } from 'amqplib';
import { config } from '../config';
import { CustomerEventEnvelope } from '../sinks/rabbitmq';

export interface AuditStats {
  totalReceived: number;
  uniqueProcessed: number;
  duplicatesDetected: number;
  lastReceivedAt: string | null;
  isRunning: boolean;
}

export class AuditConsumer {
  private connection: ChannelModel | null = null;
  private channel: Channel | null = null;
  private queueName: string;
  private exchange: string;
  private isRunning: boolean = false;

  private stats: AuditStats = {
    totalReceived: 0,
    uniqueProcessed: 0,
    duplicatesDetected: 0,
    lastReceivedAt: null,
    isRunning: false,
  };

  // LRU / bounded cache for idempotency verification (keys: id:version)
  private seenKeys: Set<string> = new Set();
  private maxSeenKeys: number = 100000;

  constructor() {
    this.queueName = config.rabbitmq.auditQueue;
    this.exchange = config.rabbitmq.exchange;
  }

  public async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    this.connection = await amqp.connect(config.rabbitmq.url);
    this.channel = await this.connection.createChannel();

    // Prefetch for high-throughput consumption
    await this.channel.prefetch(100);

    // Assert durable exchange and queue
    await this.channel.assertExchange(this.exchange, 'topic', { durable: true });
    await this.channel.assertQueue(this.queueName, { durable: true });
    await this.channel.bindQueue(this.queueName, this.exchange, 'customer.replicated.#');

    this.isRunning = true;
    this.stats.isRunning = true;

    await this.channel.consume(
      this.queueName,
      (msg: ConsumeMessage | null) => {
        if (!msg) return;

        try {
          const content = msg.content.toString();
          const event: CustomerEventEnvelope = JSON.parse(content);
          const idempotencyKey = `${event.payload.id}:${event.version}`;

          this.stats.totalReceived++;
          this.stats.lastReceivedAt = new Date().toISOString();

          if (this.seenKeys.has(idempotencyKey)) {
            this.stats.duplicatesDetected++;
          } else {
            this.stats.uniqueProcessed++;
            this.seenKeys.add(idempotencyKey);

            // Evict oldest keys if set exceeds bounds to keep memory under 512MB limit
            if (this.seenKeys.size > this.maxSeenKeys) {
              const iterator = this.seenKeys.values();
              for (let i = 0; i < 5000; i++) {
                const next = iterator.next();
                if (!next.done) {
                  this.seenKeys.delete(next.value);
                }
              }
            }
          }

          this.channel?.ack(msg);
        } catch (err) {
          console.error('[AuditConsumer] Failed to process message:', err);
          this.channel?.nack(msg, false, false); // Drop corrupt message
        }
      },
      { noAck: false }
    );

    console.log(`[AuditConsumer] Listening on queue: ${this.queueName}`);
  }

  public getStats(): AuditStats {
    return { ...this.stats };
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    this.stats.isRunning = false;
    try {
      if (this.channel) await this.channel.close();
      if (this.connection) await this.connection.close();
    } catch {
      // Ignore
    } finally {
      this.channel = null;
      this.connection = null;
    }
  }
}
