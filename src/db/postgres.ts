import { Pool, QueryResult, QueryResultRow } from 'pg';
import { config } from '../config';

export const pgPool = new Pool({
  host: config.postgres.host,
  port: config.postgres.port,
  database: config.postgres.database,
  user: config.postgres.user,
  password: config.postgres.password,
  max: config.postgres.maxConnections,
  idleTimeoutMillis: config.postgres.idleTimeoutMillis,
});

pgPool.on('error', (err) => {
  console.error('[PostgresPool] Unexpected error on idle client:', err);
});

export async function query<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const start = Date.now();
  const res = await pgPool.query<T>(text, params);
  const duration = Date.now() - start;
  if (duration > 2000) {
    console.warn(`[PostgresPool] Slow query (${duration}ms): ${text.slice(0, 100)}...`);
  }
  return res;
}

export async function testConnection(): Promise<boolean> {
  try {
    const res = await query('SELECT 1 as alive;');
    return res.rows.length > 0;
  } catch (err) {
    console.error('[PostgresPool] Connection test failed:', err);
    return false;
  }
}

export async function closePool(): Promise<void> {
  await pgPool.end();
}
