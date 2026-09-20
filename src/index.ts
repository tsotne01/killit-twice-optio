import { PipelineManager } from './pipeline/manager';
import { createApiServer } from './api/server';
import { config } from './config';
import { testConnection } from './db/postgres';

async function bootstrap() {
  console.log('=====================================================');
  console.log('  Kill It Twice: Fault-Tolerant Replication Pipeline  ');
  console.log('=====================================================');

  // Verify PostgreSQL connection
  const dbOk = await testConnection();
  if (!dbOk) {
    console.error('[Bootstrap] Failed to connect to PostgreSQL. Exiting...');
    process.exit(1);
  }
  console.log('[Bootstrap] Connected to PostgreSQL.');

  const pipeline = PipelineManager.getInstance();
  await pipeline.init();

  // Start continuous incremental synchronization worker automatically
  await pipeline.incrementalWorker.start();
  console.log('[Bootstrap] Incremental sync engine started.');

  // Create and start API Server
  const app = createApiServer(pipeline);
  const server = app.listen(config.api.port, () => {
    console.log(`[API Server] Running on http://localhost:${config.api.port}`);
    console.log(`[API Server] Telemetry metrics available at http://localhost:${config.api.port}/api/metrics`);
  });

  // Graceful shutdown handling
  const shutdown = async (signal: string) => {
    console.log(`\n[Bootstrap] Received ${signal}. Shutting down gracefully...`);
    server.close();
    await pipeline.shutdown();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

bootstrap().catch((err) => {
  console.error('[Bootstrap] Fatal startup error:', err);
  process.exit(1);
});
