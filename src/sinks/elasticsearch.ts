import { Client } from '@elastic/elasticsearch';
import { config } from '../config';
import { Customer } from '../checkpoint/manager';

export interface RejectedItem {
  customer: Customer;
  errorReason: string;
  errorDetails: any;
}

export interface BulkIndexResult {
  total: number;
  succeeded: number;
  conflicts: number;
  rejected: RejectedItem[];
}

export class ElasticsearchSink {
  private client: Client;
  private indexName: string;

  constructor() {
    this.client = new Client({
      node: config.elasticsearch.node,
      requestTimeout: config.elasticsearch.requestTimeout,
      maxRetries: config.elasticsearch.maxRetries,
    });
    this.indexName = config.elasticsearch.index;
  }

  /**
   * Health check verifying cluster connectivity.
   */
  public async checkHealth(): Promise<boolean> {
    try {
      const health = await this.client.cluster.health({ timeout: '3s' });
      return health.status === 'green' || health.status === 'yellow';
    } catch {
      return false;
    }
  }

  /**
   * Dispatches a batch of customer records using the Elasticsearch _bulk API.
   * Enforces external_gte versioning for Last-Write-Wins and isolates item-level errors.
   */
  public async bulkIndex(customers: Customer[]): Promise<BulkIndexResult> {
    if (customers.length === 0) {
      return { total: 0, succeeded: 0, conflicts: 0, rejected: [] };
    }

    // Prepare NDJSON operations
    const operations = customers.flatMap((customer) => [
      {
        index: {
          _index: this.indexName,
          _id: customer.id.toString(),
          version: customer.version,
          version_type: 'external_gte',
        },
      },
      {
        id: customer.id,
        external_id: customer.external_id,
        email: customer.email,
        first_name: customer.first_name,
        last_name: customer.last_name,
        status: customer.status,
        version: customer.version,
        balance: customer.balance,
        attributes: customer.attributes,
        created_at: customer.created_at,
        updated_at: customer.updated_at,
      },
    ]);

    const response = await this.client.bulk({
      operations,
      refresh: false, // Async refresh for high backfill throughput
    });

    let succeeded = 0;
    let conflicts = 0;
    const rejected: RejectedItem[] = [];

    if (!response.errors) {
      return {
        total: customers.length,
        succeeded: customers.length,
        conflicts: 0,
        rejected: [],
      };
    }

    // Analyze individual item results
    for (let i = 0; i < response.items.length; i++) {
      const item = response.items[i];
      const action = item.index || item.create || item.update;
      const customer = customers[i];

      if (!action) {
        continue;
      }

      const status = action.status;

      if (status === 200 || status === 201) {
        succeeded++;
      } else if (status === 409) {
        // Version conflict: Stale backfill write superseded by newer incremental update.
        // Benign rejection, discard safely.
        conflicts++;
      } else {
        // Fatal item rejection (e.g. 400 mapping type mismatch) -> Route to DLQ
        rejected.push({
          customer,
          errorReason: action.error?.reason || 'Unknown Elasticsearch rejection',
          errorDetails: action.error,
        });
      }
    }

    return {
      total: customers.length,
      succeeded,
      conflicts,
      rejected,
    };
  }

  /**
   * Returns total count of documents in Elasticsearch index.
   */
  public async getDocumentCount(): Promise<number> {
    try {
      const res = await this.client.count({ index: this.indexName });
      return res.count;
    } catch (err: any) {
      if (err.meta?.statusCode === 404) {
        return 0;
      }
      throw err;
    }
  }

  public async close(): Promise<void> {
    await this.client.close();
  }
}
