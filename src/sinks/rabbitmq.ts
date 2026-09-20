import amqp, { ChannelModel, ConfirmChannel } from 'amqplib';
import { config } from '../config';
import { Customer } from '../checkpoint/manager';

export interface CustomerEventEnvelope {
  event_id: string;
  event_type: 'CUSTOMER_SYNCED';
  timestamp: string;
  version: number;
  payload: Customer;
}

export class RabbitMQSink {
  private connection: ChannelModel | null = null;
  private channel: ConfirmChannel | null = null;
  private exchange: string;
  private isConnecting: boolean = false;

  constructor() {
    this.exchange = config.rabbitmq.exchange;
  }

  /**
   * Initializes durable topic exchange and confirm channel.
   */
  public async init(): Promise<void> {
    if (this.channel) {
      return;
    }

    if (this.isConnecting) {
      // Wait for ongoing connection attempt
      while (this.isConnecting) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      return;
    }

    this.isConnecting = true;
    try {
      this.connection = await amqp.connect(config.rabbitmq.url);
      this.connection.on('error', (err) => {
        console.error('[RabbitMQSink] Connection error:', err.message);
        this.reset();
      });
      this.connection.on('close', () => {
        console.warn('[RabbitMQSink] Connection closed');
        this.reset();
      });

      this.channel = await this.connection.createConfirmChannel();
      await this.channel.assertExchange(this.exchange, 'topic', { durable: true });
    } finally {
      this.isConnecting = false;
    }
  }

  private reset(): void {
    this.channel = null;
    this.connection = null;
  }

  /**
   * Health check verifying broker connectivity.
   */
  public async checkHealth(): Promise<boolean> {
    try {
      if (!this.channel) {
        await this.init();
      }
      return this.channel !== null;
    } catch {
      return false;
    }
  }

  /**
   * Dispatches a batch of events with publisher confirms.
   * Guarantees all messages are safely accepted by broker before proceeding.
   */
  public async publishBatch(customers: Customer[]): Promise<void> {
    if (customers.length === 0) {
      return;
    }

    if (!this.channel) {
      await this.init();
    }

    if (!this.channel) {
      throw new Error('[RabbitMQSink] Failed to acquire RabbitMQ channel');
    }

    for (const customer of customers) {
      const routingKey = `customer.replicated.${(customer.status || 'UNKNOWN').toLowerCase()}`;
      const envelope: CustomerEventEnvelope = {
        event_id: `evt_${customer.id}_${customer.version}_${Date.now()}`,
        event_type: 'CUSTOMER_SYNCED',
        timestamp: new Date().toISOString(),
        version: customer.version,
        payload: customer,
      };

      const content = Buffer.from(JSON.stringify(envelope));
      this.channel.publish(this.exchange, routingKey, content, {
        persistent: true,
        contentType: 'application/json',
      });
    }

    // Await broker confirmation for all messages in the batch
    await this.channel.waitForConfirms();
  }

  public async close(): Promise<void> {
    try {
      if (this.channel) {
        await this.channel.close();
      }
      if (this.connection) {
        await this.connection.close();
      }
    } catch {
      // Ignore cleanup errors
    } finally {
      this.reset();
    }
  }
}
