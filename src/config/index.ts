import dotenv from 'dotenv';
dotenv.config();

export interface AppConfig {
  postgres: {
    host: string;
    port: number;
    database: string;
    user: string;
    password: string;
    maxConnections: number;
    idleTimeoutMillis: number;
  };
  elasticsearch: {
    node: string;
    index: string;
    requestTimeout: number;
    maxRetries: number;
  };
  rabbitmq: {
    url: string;
    exchange: string;
    auditQueue: string;
  };
  pipeline: {
    batchSize: number;
    pollIntervalMs: number;
    maxBackoffMs: number;
    initialBackoffMs: number;
  };
  api: {
    port: number;
  };
}

export const config: AppConfig = {
  postgres: {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'optio_db',
    user: process.env.POSTGRES_USER || 'optio_user',
    password: process.env.POSTGRES_PASSWORD || 'optio_password',
    maxConnections: parseInt(process.env.POSTGRES_MAX_CONN || '20', 10),
    idleTimeoutMillis: 30000,
  },
  elasticsearch: {
    node: process.env.ELASTICSEARCH_NODE || 'http://localhost:9200',
    index: process.env.ELASTICSEARCH_INDEX || 'customers',
    requestTimeout: 10000,
    maxRetries: 3,
  },
  rabbitmq: {
    url: process.env.RABBITMQ_URL || 'amqp://optio_user:optio_password@localhost:5672',
    exchange: process.env.RABBITMQ_EXCHANGE || 'customer.events',
    auditQueue: process.env.RABBITMQ_QUEUE || 'customer.events.audit_queue',
  },
  pipeline: {
    batchSize: parseInt(process.env.PIPELINE_BATCH_SIZE || '500', 10),
    pollIntervalMs: parseInt(process.env.PIPELINE_POLL_INTERVAL_MS || '1000', 10),
    maxBackoffMs: 30000,
    initialBackoffMs: 1000,
  },
  api: {
    port: parseInt(process.env.API_PORT || '4000', 10),
  },
};
