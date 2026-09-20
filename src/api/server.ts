import express, { Request, Response } from 'express';
import cors from 'cors';
import path from 'path';
import { PipelineManager } from '../pipeline/manager';
import { DLQManager } from '../dlq/manager';
import { query } from '../db/postgres';
import { config } from '../config';

export function createApiServer(pipeline: PipelineManager): express.Application {
  const app = express();
  app.use(cors());
  app.use(express.json());

  // Static UI Serving
  const uiDistPath = path.resolve(__dirname, '../../ui/dist');
  app.use(express.static(uiDistPath));

  // 1. Telemetry & Metrics (Invariant G5)
  app.get('/api/metrics', async (_req: Request, res: Response) => {
    try {
      const telemetry = await pipeline.getTelemetry();
      res.json(telemetry);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/status', async (_req: Request, res: Response) => {
    try {
      const telemetry = await pipeline.getTelemetry();
      res.json({
        healthy: telemetry.sinks.elasticsearch.isAvailable && telemetry.sinks.rabbitmq.isAvailable,
        backfillStatus: telemetry.backfill.status,
        incrementalStatus: telemetry.incremental.status,
        progressPct: telemetry.backfill.progressPct,
        currentThroughputEps: telemetry.backfill.currentThroughputEps,
        lagSeconds: telemetry.incremental.lagSeconds,
        lagRecords: telemetry.incremental.lagRecords,
        dlqPending: telemetry.dlq.pendingCount,
        memoryUsageMb: telemetry.memory.rssMb,
      });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // 2. Pipeline Controls
  app.post('/api/pipeline/backfill/start', async (_req: Request, res: Response) => {
    try {
      await pipeline.backfillWorker.start();
      res.json({ success: true, message: 'Backfill started' });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/pipeline/backfill/pause', async (_req: Request, res: Response) => {
    try {
      await pipeline.backfillWorker.pause();
      res.json({ success: true, message: 'Backfill paused' });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/pipeline/backfill/resume', async (_req: Request, res: Response) => {
    try {
      await pipeline.backfillWorker.start();
      res.json({ success: true, message: 'Backfill resumed' });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/pipeline/incremental/start', async (_req: Request, res: Response) => {
    try {
      await pipeline.incrementalWorker.start();
      res.json({ success: true, message: 'Incremental sync started' });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/pipeline/incremental/pause', async (_req: Request, res: Response) => {
    try {
      await pipeline.incrementalWorker.pause();
      res.json({ success: true, message: 'Incremental sync paused' });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // 3. Replicated Data Browser (Query Elasticsearch)
  app.get('/api/customers', async (req: Request, res: Response) => {
    try {
      const q = (req.query.q as string) || '';
      const size = Math.min(parseInt((req.query.size as string) || '20', 10), 100);
      const from = parseInt((req.query.from as string) || '0', 10);

      const esQuery = q
        ? {
            multi_match: {
              query: q,
              fields: ['email', 'first_name', 'last_name', 'external_id', 'status'],
            },
          }
        : { match_all: {} };

      const esResponse = await pipeline.esSink['client'].search({
        index: config.elasticsearch.index,
        from,
        size,
        query: esQuery,
        sort: [{ id: { order: 'desc' } }],
      });

      const total = typeof esResponse.hits.total === 'number'
        ? esResponse.hits.total
        : esResponse.hits.total?.value || 0;

      const items = esResponse.hits.hits.map((hit) => hit._source);

      res.json({ total, items });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // 4. DLQ Management & Replay
  app.get('/api/dlq', async (req: Request, res: Response) => {
    try {
      const limit = parseInt((req.query.limit as string) || '50', 10);
      const offset = parseInt((req.query.offset as string) || '0', 10);
      const list = await DLQManager.list(limit, offset);
      res.json(list);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/dlq/:id/replay', async (req: Request, res: Response) => {
    try {
      const id = parseInt(req.params.id, 10);
      const result = await DLQManager.replay(id, pipeline.esSink, pipeline.rmqSink);
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/dlq/:id/discard', async (req: Request, res: Response) => {
    try {
      const id = parseInt(req.params.id, 10);
      await DLQManager.discard(id);
      res.json({ success: true, message: `Record ${id} discarded` });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // 5. Chaos & Simulation Controls
  app.post('/api/simulate/corrupt-records', async (_req: Request, res: Response) => {
    try {
      // Injects 3 records with invalid balance types to trigger Elasticsearch mapping rejection (G4)
      const insertedIds: number[] = [];
      for (let i = 1; i <= 3; i++) {
        const extId = `corrupt_${Date.now()}_${i}`;
        const insertRes = await query<{ id: string }>(
          `INSERT INTO customers (external_id, email, first_name, last_name, status, version, balance, attributes, created_at, updated_at)
           VALUES ($1, $2, 'Corrupt', 'User', 'ACTIVE', 1, 999.99, json_build_object('score', 'MALFORMED_NON_NUMERIC_STRING'), NOW(), NOW())
           RETURNING id`,
          [extId, `${extId}@corrupt.test`]
        );
        insertedIds.push(parseInt(insertRes.rows[0].id, 10));
      }
      res.json({
        success: true,
        message: 'Injected 3 test records with mapping conflict attributes for G4 simulation',
        insertedIds,
      });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/simulate/mutate-records', async (req: Request, res: Response) => {
    try {
      const count = parseInt((req.body.count as string) || '100', 10);
      await query(
        `UPDATE customers
         SET version = version + 1,
             balance = balance + 10.0,
             status = CASE WHEN random() > 0.5 THEN 'ACTIVE' ELSE 'INACTIVE' END,
             updated_at = NOW()
         WHERE id IN (
           SELECT id FROM customers ORDER BY random() LIMIT $1
         )`,
        [count]
      );
      res.json({
        success: true,
        message: `Successfully mutated ${count} random records in PostgreSQL to trigger incremental sync`,
      });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // SPA Fallback
  app.get('*', (req: Request, res: Response, next) => {
    if (req.path.startsWith('/api')) {
      return next();
    }
    const indexPath = path.join(uiDistPath, 'index.html');
    res.sendFile(indexPath, (err) => {
      if (err) {
        res.status(200).send('Kill It Twice API Server running. UI assets not built yet.');
      }
    });
  });

  return app;
}
