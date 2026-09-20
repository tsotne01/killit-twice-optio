import { useState, useEffect } from 'react';
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Database,
  Flame,
  Layers,
  Pause,
  Play,
  RefreshCw,
  RotateCcw,
  Search,
  ShieldCheck,
  Trash2,
  Zap,
  Server,
  Radio,
  Cpu
} from 'lucide-react';

interface Telemetry {
  timestamp: string;
  uptimeSeconds: number;
  memory: {
    rssMb: number;
    heapUsedMb: number;
    limitMb: number;
  };
  backfill: {
    status: 'IDLE' | 'RUNNING' | 'PAUSED' | 'COMPLETED' | 'FAILED';
    lastProcessedId: number;
    recordsProcessed: number;
    totalRecords: number;
    progressPct: number;
    currentThroughputEps: number;
    startedAt: string | null;
    lastBatchDurationMs: number;
  };
  incremental: {
    status: 'IDLE' | 'RUNNING' | 'PAUSED' | 'FAILED';
    lastProcessedTimestamp: string;
    lastProcessedId: number;
    lagSeconds: number;
    lagRecords: number;
    recordsProcessed: number;
    pollIntervalMs: number;
  };
  dlq: {
    pendingCount: number;
  };
  sinks: {
    elasticsearch: {
      isAvailable: boolean;
      documentCount: number;
      circuitBreaker: {
        consecutiveFailures: number;
        currentBackoffMs: number;
      };
    };
    rabbitmq: {
      isAvailable: boolean;
      circuitBreaker: {
        consecutiveFailures: number;
      };
    };
  };
  auditConsumer: {
    totalReceived: number;
    uniqueProcessed: number;
    duplicatesDetected: number;
    isRunning: boolean;
  };
  source: {
    totalRecords: number;
  };
}

interface CustomerDoc {
  id: number;
  external_id: string;
  email: string;
  first_name: string;
  last_name: string;
  status: string;
  version: number;
  balance: number;
  updated_at: string;
}

interface DLQItem {
  id: number;
  pipeline_mode: string;
  record_id: number;
  target_sink: string;
  error_reason: string;
  status: string;
  retry_count: number;
  created_at: string;
}

export default function App() {
  const [activeTab, setActiveTab] = useState<'overview' | 'browser' | 'controls' | 'chaos'>('overview');
  const [telemetry, setTelemetry] = useState<Telemetry | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Data Browser state
  const [customers, setCustomers] = useState<CustomerDoc[]>([]);
  const [customerCount, setCustomerCount] = useState(0);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchLoading, setSearchLoading] = useState(false);

  // DLQ state
  const [dlqItems, setDlqItems] = useState<DLQItem[]>([]);
  const [toastMessage, setToastMessage] = useState<string | null>(null);

  // Show Toast
  const showToast = (msg: string) => {
    setToastMessage(msg);
    setTimeout(() => setToastMessage(null), 4000);
  };

  // Poll Telemetry every 1.5 seconds
  useEffect(() => {
    const fetchTelemetry = async () => {
      try {
        const res = await fetch('/api/metrics');
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setTelemetry(data);
        setError(null);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchTelemetry();
    const interval = setInterval(fetchTelemetry, 1500);
    return () => clearInterval(interval);
  }, []);

  // Fetch Customers when entering Browser tab
  useEffect(() => {
    if (activeTab === 'browser') {
      loadCustomers(searchQuery);
    } else if (activeTab === 'controls') {
      loadDLQ();
    }
  }, [activeTab]);

  const loadCustomers = async (q = '') => {
    setSearchLoading(true);
    try {
      const res = await fetch(`/api/customers?q=${encodeURIComponent(q)}&size=15`);
      const data = await res.json();
      setCustomers(data.items || []);
      setCustomerCount(data.total || 0);
    } catch (err) {
      console.error(err);
    } finally {
      setSearchLoading(false);
    }
  };

  const loadDLQ = async () => {
    try {
      const res = await fetch('/api/dlq?limit=20');
      const data = await res.json();
      setDlqItems(data.items || []);
    } catch (err) {
      console.error(err);
    }
  };

  // Actions
  const handleStartBackfill = async () => {
    await fetch('/api/pipeline/backfill/start', { method: 'POST' });
    showToast('Backfill engine started');
  };

  const handlePauseBackfill = async () => {
    await fetch('/api/pipeline/backfill/pause', { method: 'POST' });
    showToast('Backfill engine paused');
  };

  const handleStartIncremental = async () => {
    await fetch('/api/pipeline/incremental/start', { method: 'POST' });
    showToast('Incremental sync started');
  };

  const handlePauseIncremental = async () => {
    await fetch('/api/pipeline/incremental/pause', { method: 'POST' });
    showToast('Incremental sync paused');
  };

  const handleReplayDLQ = async (id: number) => {
    const res = await fetch(`/api/dlq/${id}/replay`, { method: 'POST' });
    const data = await res.json();
    showToast(data.message || 'Replay executed');
    loadDLQ();
  };

  const handleDiscardDLQ = async (id: number) => {
    await fetch(`/api/dlq/${id}/discard`, { method: 'POST' });
    showToast(`DLQ item #${id} marked as discarded`);
    loadDLQ();
  };

  // Chaos triggers
  const handleInjectCorrupt = async () => {
    const res = await fetch('/api/simulate/corrupt-records', { method: 'POST' });
    const data = await res.json();
    showToast(data.message || '3 corrupt records injected');
    setTimeout(loadDLQ, 1000);
  };

  const handleMutateRecords = async () => {
    const res = await fetch('/api/simulate/mutate-records', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count: 100 }),
    });
    const data = await res.json();
    showToast(data.message || '100 records updated');
  };

  if (loading && !telemetry) {
    return (
      <div className="flex h-screen w-full items-center justify-center bg-zinc-950 text-zinc-400">
        <RefreshCw className="mr-3 h-6 w-6 animate-spin text-zinc-200" />
        <span>Connecting to Optio Replication Control Server...</span>
      </div>
    );
  }

  const isHealthy =
    telemetry?.sinks.elasticsearch.isAvailable && telemetry?.sinks.rabbitmq.isAvailable;

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 antialiased selection:bg-zinc-800">
      {/* Toast Notification */}
      {toastMessage && (
        <div className="fixed bottom-5 right-5 z-50 flex items-center rounded-lg border border-zinc-700 bg-zinc-900/95 px-4 py-3 text-sm text-zinc-100 shadow-2xl backdrop-blur">
          <CheckCircle2 className="mr-2 h-4 w-4 text-emerald-400" />
          <span>{toastMessage}</span>
        </div>
      )}

      {/* Top Header */}
      <header className="sticky top-0 z-40 border-b border-zinc-800 bg-zinc-950/80 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
          <div className="flex items-center space-x-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-zinc-100 text-zinc-900 font-bold">
              <Layers className="h-5 w-5 text-zinc-900" />
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <span className="font-semibold text-lg tracking-tight">Kill It Twice</span>
                <span className="rounded-full bg-zinc-800 px-2 py-0.5 text-xs text-zinc-400">
                  Optio Platform
                </span>
              </div>
              <p className="text-xs text-zinc-400">Fault-Tolerant Dual-Sink Data Replication</p>
            </div>
          </div>

          <div className="flex items-center space-x-6">
            {/* Health Badge */}
            <div className="flex items-center space-x-2">
              <span
                className={`h-2.5 w-2.5 rounded-full ${
                  isHealthy ? 'bg-emerald-500 animate-pulse' : 'bg-red-500'
                }`}
              />
              <span className="text-xs font-medium uppercase tracking-wider text-zinc-300">
                {isHealthy ? 'Cluster Healthy' : 'Degraded Outage'}
              </span>
            </div>

            {/* Memory Gauge */}
            <div className="hidden sm:flex items-center space-x-2 border-l border-zinc-800 pl-6 text-xs text-zinc-400">
              <Cpu className="h-3.5 w-3.5 text-zinc-500" />
              <span>RAM:</span>
              <span className="font-mono text-zinc-200">
                {telemetry?.memory.rssMb}MB / {telemetry?.memory.limitMb}MB
              </span>
            </div>

            {/* Throughput */}
            <div className="hidden md:flex items-center space-x-2 border-l border-zinc-800 pl-6 text-xs text-zinc-400">
              <Zap className="h-3.5 w-3.5 text-amber-400" />
              <span>Throughput:</span>
              <span className="font-mono font-semibold text-zinc-100">
                {telemetry?.backfill.currentThroughputEps.toLocaleString()} eps
              </span>
            </div>
          </div>
        </div>

        {/* Navigation Tabs */}
        <div className="mx-auto flex max-w-7xl space-x-8 px-6 text-sm">
          <button
            onClick={() => setActiveTab('overview')}
            className={`flex items-center space-x-2 border-b-2 py-3 font-medium transition ${
              activeTab === 'overview'
                ? 'border-zinc-100 text-zinc-100'
                : 'border-transparent text-zinc-400 hover:text-zinc-200'
            }`}
          >
            <Activity className="h-4 w-4" />
            <span>Overview & Observability</span>
          </button>
          <button
            onClick={() => setActiveTab('browser')}
            className={`flex items-center space-x-2 border-b-2 py-3 font-medium transition ${
              activeTab === 'browser'
                ? 'border-zinc-100 text-zinc-100'
                : 'border-transparent text-zinc-400 hover:text-zinc-200'
            }`}
          >
            <Database className="h-4 w-4" />
            <span>Data Browser (ES)</span>
          </button>
          <button
            onClick={() => setActiveTab('controls')}
            className={`flex items-center space-x-2 border-b-2 py-3 font-medium transition ${
              activeTab === 'controls'
                ? 'border-zinc-100 text-zinc-100'
                : 'border-transparent text-zinc-400 hover:text-zinc-200'
            }`}
          >
            <ShieldCheck className="h-4 w-4" />
            <span>Pipeline & DLQ Controls</span>
            {telemetry && telemetry.dlq.pendingCount > 0 && (
              <span className="ml-1 rounded-full bg-amber-500/20 px-1.5 py-0.2 text-[10px] font-semibold text-amber-400 border border-amber-500/30">
                {telemetry.dlq.pendingCount}
              </span>
            )}
          </button>
          <button
            onClick={() => setActiveTab('chaos')}
            className={`flex items-center space-x-2 border-b-2 py-3 font-medium transition ${
              activeTab === 'chaos'
                ? 'border-zinc-100 text-zinc-100'
                : 'border-transparent text-zinc-400 hover:text-zinc-200'
            }`}
          >
            <Flame className="h-4 w-4 text-orange-400" />
            <span>Chaos Simulator</span>
          </button>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="mx-auto max-w-7xl p-6">
        {error && (
          <div className="mb-6 flex items-center rounded-lg border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-400">
            <AlertTriangle className="mr-3 h-5 w-5" />
            <span>Failed to connect to API backend: {error}</span>
          </div>
        )}

        {/* TAB 1: OVERVIEW */}
        {activeTab === 'overview' && telemetry && (
          <div className="space-y-6">
            {/* Metric KPI Cards */}
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
              {/* Backfill Progress */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-5 shadow-sm">
                <div className="flex items-center justify-between text-xs text-zinc-400">
                  <span>Backfill Progress</span>
                  <span
                    className={`rounded-full px-2 py-0.5 text-[10px] font-medium uppercase ${
                      telemetry.backfill.status === 'RUNNING'
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                        : telemetry.backfill.status === 'PAUSED'
                        ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                        : 'bg-zinc-800 text-zinc-400'
                    }`}
                  >
                    {telemetry.backfill.status}
                  </span>
                </div>
                <div className="mt-3 flex items-baseline justify-between">
                  <span className="text-2xl font-bold tracking-tight">
                    {telemetry.backfill.progressPct}%
                  </span>
                  <span className="font-mono text-xs text-zinc-400">
                    {telemetry.backfill.recordsProcessed.toLocaleString()} /{' '}
                    {telemetry.backfill.totalRecords.toLocaleString()}
                  </span>
                </div>
                {/* Progress bar */}
                <div className="mt-3 h-1.5 w-full rounded-full bg-zinc-800 overflow-hidden">
                  <div
                    className="h-full bg-zinc-100 transition-all duration-500"
                    style={{ width: `${Math.min(100, telemetry.backfill.progressPct)}%` }}
                  />
                </div>
                <div className="mt-3 flex justify-between text-[11px] text-zinc-500">
                  <span>Cursor: #{telemetry.backfill.lastProcessedId}</span>
                  <span>Batch: {telemetry.backfill.lastBatchDurationMs}ms</span>
                </div>
              </div>

              {/* Ingestion Throughput */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-5 shadow-sm">
                <div className="flex items-center justify-between text-xs text-zinc-400">
                  <span>Ingestion Throughput</span>
                  <Zap className="h-4 w-4 text-amber-400" />
                </div>
                <div className="mt-3 flex items-baseline space-x-2">
                  <span className="text-2xl font-bold tracking-tight">
                    {telemetry.backfill.currentThroughputEps.toLocaleString()}
                  </span>
                  <span className="text-xs text-zinc-400">events / sec</span>
                </div>
                <p className="mt-3 text-xs text-zinc-400">
                  Rolling 10s backpressured bulk window (500-item chunks)
                </p>
              </div>

              {/* Incremental Lag */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-5 shadow-sm">
                <div className="flex items-center justify-between text-xs text-zinc-400">
                  <span>Incremental Sync Lag</span>
                  <Radio className="h-4 w-4 text-blue-400" />
                </div>
                <div className="mt-3 flex items-baseline space-x-2">
                  <span className="text-2xl font-bold tracking-tight">
                    {telemetry.incremental.lagSeconds}s
                  </span>
                  <span className="font-mono text-xs text-zinc-400">
                    ({telemetry.incremental.lagRecords} pending rows)
                  </span>
                </div>
                <div className="mt-3 flex items-center justify-between text-[11px] text-zinc-500">
                  <span>Status: {telemetry.incremental.status}</span>
                  <span>Interval: {telemetry.incremental.pollIntervalMs}ms</span>
                </div>
              </div>

              {/* Dead Letter Queue */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-5 shadow-sm">
                <div className="flex items-center justify-between text-xs text-zinc-400">
                  <span>Dead Letter Queue (G4)</span>
                  <AlertTriangle
                    className={`h-4 w-4 ${
                      telemetry.dlq.pendingCount > 0 ? 'text-amber-400' : 'text-zinc-600'
                    }`}
                  />
                </div>
                <div className="mt-3 flex items-baseline space-x-2">
                  <span
                    className={`text-2xl font-bold tracking-tight ${
                      telemetry.dlq.pendingCount > 0 ? 'text-amber-400' : 'text-zinc-100'
                    }`}
                  >
                    {telemetry.dlq.pendingCount}
                  </span>
                  <span className="text-xs text-zinc-400">unresolved errors</span>
                </div>
                <p className="mt-3 text-xs text-zinc-400">
                  Partial failures safely isolated with full payload context
                </p>
              </div>
            </div>

            {/* Infrastructure Topology Grid */}
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
              {/* PostgreSQL */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/30 p-5">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2.5">
                    <Server className="h-4 w-4 text-zinc-400" />
                    <span className="font-medium text-sm">PostgreSQL Source</span>
                  </div>
                  <span className="h-2 w-2 rounded-full bg-emerald-500" />
                </div>
                <div className="mt-4 space-y-2 text-xs">
                  <div className="flex justify-between text-zinc-400">
                    <span>Source Rows:</span>
                    <span className="font-mono text-zinc-200">
                      {telemetry.source.totalRecords.toLocaleString()}
                    </span>
                  </div>
                  <div className="flex justify-between text-zinc-400">
                    <span>Keyset Seek:</span>
                    <span className="font-mono text-emerald-400">O(log N) Indexed</span>
                  </div>
                </div>
              </div>

              {/* Elasticsearch */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/30 p-5">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2.5">
                    <Database className="h-4 w-4 text-zinc-400" />
                    <span className="font-medium text-sm">Elasticsearch Sink</span>
                  </div>
                  <span
                    className={`h-2 w-2 rounded-full ${
                      telemetry.sinks.elasticsearch.isAvailable
                        ? 'bg-emerald-500'
                        : 'bg-red-500 animate-ping'
                    }`}
                  />
                </div>
                <div className="mt-4 space-y-2 text-xs">
                  <div className="flex justify-between text-zinc-400">
                    <span>Indexed Documents:</span>
                    <span className="font-mono text-zinc-200">
                      {telemetry.sinks.elasticsearch.documentCount.toLocaleString()}
                    </span>
                  </div>
                  <div className="flex justify-between text-zinc-400">
                    <span>Versioning:</span>
                    <span className="font-mono text-zinc-200">external_gte (LWW)</span>
                  </div>
                </div>
              </div>

              {/* RabbitMQ */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/30 p-5">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2.5">
                    <Radio className="h-4 w-4 text-zinc-400" />
                    <span className="font-medium text-sm">RabbitMQ Broker</span>
                  </div>
                  <span
                    className={`h-2 w-2 rounded-full ${
                      telemetry.sinks.rabbitmq.isAvailable ? 'bg-emerald-500' : 'bg-red-500'
                    }`}
                  />
                </div>
                <div className="mt-4 space-y-2 text-xs">
                  <div className="flex justify-between text-zinc-400">
                    <span>Topic Exchange:</span>
                    <span className="font-mono text-zinc-200">customer.events</span>
                  </div>
                  <div className="flex justify-between text-zinc-400">
                    <span>Durability:</span>
                    <span className="font-mono text-zinc-200">Publisher Confirms</span>
                  </div>
                </div>
              </div>

              {/* Audit Consumer */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/30 p-5">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2.5">
                    <ShieldCheck className="h-4 w-4 text-zinc-400" />
                    <span className="font-medium text-sm">Audit Consumer</span>
                  </div>
                  <span className="h-2 w-2 rounded-full bg-emerald-500" />
                </div>
                <div className="mt-4 space-y-2 text-xs">
                  <div className="flex justify-between text-zinc-400">
                    <span>Received Events:</span>
                    <span className="font-mono text-zinc-200">
                      {telemetry.auditConsumer.totalReceived.toLocaleString()}
                    </span>
                  </div>
                  <div className="flex justify-between text-zinc-400">
                    <span>Duplicates Filtered:</span>
                    <span className="font-mono text-emerald-400">
                      {telemetry.auditConsumer.duplicatesDetected.toLocaleString()} (G2)
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* TAB 2: DATA BROWSER */}
        {activeTab === 'browser' && (
          <div className="space-y-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <h2 className="text-lg font-semibold">Elasticsearch Replicated Records</h2>
                <p className="text-xs text-zinc-400">
                  Search and inspect real-time replicated customer profiles directly in the index ({customerCount.toLocaleString()} total documents)
                </p>
              </div>

              <div className="flex items-center space-x-2">
                <div className="relative">
                  <Search className="absolute left-3 top-2.5 h-4 w-4 text-zinc-500" />
                  <input
                    type="text"
                    placeholder="Search name, email, ID..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && loadCustomers(searchQuery)}
                    className="h-9 w-64 rounded-lg border border-zinc-800 bg-zinc-900 pl-9 pr-4 text-xs text-zinc-100 placeholder:text-zinc-500 focus:border-zinc-500 focus:outline-none"
                  />
                </div>
                <button
                  onClick={() => loadCustomers(searchQuery)}
                  className="flex h-9 items-center rounded-lg border border-zinc-800 bg-zinc-900 px-3 text-xs font-medium hover:bg-zinc-800"
                >
                  <RefreshCw className={`h-3.5 w-3.5 ${searchLoading ? 'animate-spin' : ''}`} />
                </button>
              </div>
            </div>

            {/* Customers Table */}
            <div className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900/40">
              <table className="w-full text-left text-xs">
                <thead className="border-b border-zinc-800 bg-zinc-900/80 text-zinc-400">
                  <tr>
                    <th className="px-4 py-3 font-medium">ID</th>
                    <th className="px-4 py-3 font-medium">External ID</th>
                    <th className="px-4 py-3 font-medium">Customer Name</th>
                    <th className="px-4 py-3 font-medium">Email</th>
                    <th className="px-4 py-3 font-medium">Status</th>
                    <th className="px-4 py-3 font-medium">Version</th>
                    <th className="px-4 py-3 font-medium">Balance</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-zinc-800/60 font-mono">
                  {customers.map((c) => (
                    <tr key={c.id} className="hover:bg-zinc-900/60 transition">
                      <td className="px-4 py-3 text-zinc-300">#{c.id}</td>
                      <td className="px-4 py-3 text-zinc-400">{c.external_id}</td>
                      <td className="px-4 py-3 font-sans font-medium text-zinc-200">
                        {c.first_name} {c.last_name}
                      </td>
                      <td className="px-4 py-3 font-sans text-zinc-300">{c.email}</td>
                      <td className="px-4 py-3">
                        <span
                          className={`rounded-full px-2 py-0.5 text-[10px] font-sans font-medium ${
                            c.status === 'ACTIVE'
                              ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                              : 'bg-zinc-800 text-zinc-400'
                          }`}
                        >
                          {c.status}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-zinc-400">v{c.version}</td>
                      <td className="px-4 py-3 text-zinc-200">${c.balance.toFixed(2)}</td>
                    </tr>
                  ))}
                  {customers.length === 0 && (
                    <tr>
                      <td colSpan={7} className="py-8 text-center text-zinc-500 font-sans">
                        No customer documents found in Elasticsearch. Trigger Backfill to begin ingestion.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* TAB 3: CONTROLS & DLQ */}
        {activeTab === 'controls' && (
          <div className="space-y-6">
            {/* Engine Control Panels */}
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-5">
                <h3 className="font-semibold text-sm">Backfill Engine Control</h3>
                <p className="mt-1 text-xs text-zinc-400">
                  Stream 1,000,000 historical records via keyset pagination with crash recovery
                </p>
                <div className="mt-4 flex space-x-3">
                  <button
                    onClick={handleStartBackfill}
                    className="flex items-center space-x-1.5 rounded-lg bg-zinc-100 px-4 py-2 text-xs font-semibold text-zinc-900 hover:bg-zinc-200 transition"
                  >
                    <Play className="h-3.5 w-3.5" />
                    <span>Start / Resume Backfill</span>
                  </button>
                  <button
                    onClick={handlePauseBackfill}
                    className="flex items-center space-x-1.5 rounded-lg border border-zinc-700 bg-zinc-800 px-4 py-2 text-xs font-semibold text-zinc-200 hover:bg-zinc-700 transition"
                  >
                    <Pause className="h-3.5 w-3.5" />
                    <span>Pause</span>
                  </button>
                </div>
              </div>

              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-5">
                <h3 className="font-semibold text-sm">Incremental Polling Control</h3>
                <p className="mt-1 text-xs text-zinc-400">
                  Continuous low-latency polling for changes and external version conflict resolution
                </p>
                <div className="mt-4 flex space-x-3">
                  <button
                    onClick={handleStartIncremental}
                    className="flex items-center space-x-1.5 rounded-lg bg-zinc-100 px-4 py-2 text-xs font-semibold text-zinc-900 hover:bg-zinc-200 transition"
                  >
                    <Play className="h-3.5 w-3.5" />
                    <span>Start Polling</span>
                  </button>
                  <button
                    onClick={handlePauseIncremental}
                    className="flex items-center space-x-1.5 rounded-lg border border-zinc-700 bg-zinc-800 px-4 py-2 text-xs font-semibold text-zinc-200 hover:bg-zinc-700 transition"
                  >
                    <Pause className="h-3.5 w-3.5" />
                    <span>Pause</span>
                  </button>
                </div>
              </div>
            </div>

            {/* DLQ Table */}
            <div>
              <div className="flex items-center justify-between mb-3">
                <div>
                  <h3 className="font-semibold text-sm">Dead Letter Queue Inspector</h3>
                  <p className="text-xs text-zinc-400">
                    Failed items isolated at record-level with remediation and replay capability (G4)
                  </p>
                </div>
                <button
                  onClick={loadDLQ}
                  className="flex h-8 items-center space-x-1 rounded-lg border border-zinc-800 bg-zinc-900 px-3 text-xs text-zinc-300 hover:bg-zinc-800"
                >
                  <RefreshCw className="h-3 w-3" />
                  <span>Refresh DLQ</span>
                </button>
              </div>

              <div className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900/40">
                <table className="w-full text-left text-xs">
                  <thead className="border-b border-zinc-800 bg-zinc-900/80 text-zinc-400">
                    <tr>
                      <th className="px-4 py-3 font-medium">DLQ ID</th>
                      <th className="px-4 py-3 font-medium">Record ID</th>
                      <th className="px-4 py-3 font-medium">Sink</th>
                      <th className="px-4 py-3 font-medium">Error Cause</th>
                      <th className="px-4 py-3 font-medium">Status</th>
                      <th className="px-4 py-3 font-medium text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-zinc-800/60 font-mono">
                    {dlqItems.map((item) => (
                      <tr key={item.id} className="hover:bg-zinc-900/60 transition">
                        <td className="px-4 py-3 text-zinc-300">#{item.id}</td>
                        <td className="px-4 py-3 text-zinc-300">Customer #{item.record_id}</td>
                        <td className="px-4 py-3 text-zinc-400">{item.target_sink}</td>
                        <td className="px-4 py-3 font-sans text-xs text-red-400 max-w-xs truncate">
                          {item.error_reason}
                        </td>
                        <td className="px-4 py-3">
                          <span
                            className={`rounded-full px-2 py-0.5 text-[10px] font-sans font-medium ${
                              item.status === 'PENDING'
                                ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                                : item.status === 'REPLAYED'
                                ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                                : 'bg-zinc-800 text-zinc-400'
                            }`}
                          >
                            {item.status}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-right space-x-2 font-sans">
                          <button
                            onClick={() => handleReplayDLQ(item.id)}
                            className="rounded bg-zinc-800 px-2 py-1 text-[11px] font-medium text-zinc-200 hover:bg-zinc-700 transition"
                          >
                            <RotateCcw className="inline h-3 w-3 mr-1" />
                            Replay
                          </button>
                          <button
                            onClick={() => handleDiscardDLQ(item.id)}
                            className="rounded bg-red-500/10 px-2 py-1 text-[11px] font-medium text-red-400 hover:bg-red-500/20 transition"
                          >
                            <Trash2 className="inline h-3 w-3 mr-1" />
                            Discard
                          </button>
                        </td>
                      </tr>
                    ))}
                    {dlqItems.length === 0 && (
                      <tr>
                        <td colSpan={6} className="py-8 text-center text-zinc-500 font-sans">
                          No items in Dead Letter Queue. System running cleanly.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* TAB 4: CHAOS SIMULATOR */}
        {activeTab === 'chaos' && (
          <div className="space-y-6">
            <div>
              <h2 className="text-lg font-semibold flex items-center space-x-2">
                <Flame className="h-5 w-5 text-orange-400" />
                <span>Interactive Chaos & Failure Testing Panel</span>
              </h2>
              <p className="mt-1 text-xs text-zinc-400">
                Trigger simulated crash scenarios, network outages, and data corruption directly to verify G1–G5 invariants
              </p>
            </div>

            <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
              {/* Gate 4 Trigger */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-5">
                <div className="flex items-center space-x-2 text-amber-400 font-semibold text-sm">
                  <AlertTriangle className="h-4 w-4" />
                  <span>Gate 4: Inject Corrupt Records (DLQ Test)</span>
                </div>
                <p className="mt-2 text-xs text-zinc-400 leading-relaxed">
                  Inserts 3 records with invalid types into PostgreSQL. When the pipeline processes them, Elasticsearch rejects only the 3 invalid items, indices the remaining valid items, and routes the 3 rejects to DLQ without rolling back the batch.
                </p>
                <button
                  onClick={handleInjectCorrupt}
                  className="mt-4 rounded-lg bg-amber-500/20 border border-amber-500/30 px-4 py-2 text-xs font-semibold text-amber-300 hover:bg-amber-500/30 transition"
                >
                  Inject 3 Malformed Records
                </button>
              </div>

              {/* Incremental Source Mutation */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-5">
                <div className="flex items-center space-x-2 text-blue-400 font-semibold text-sm">
                  <Zap className="h-4 w-4" />
                  <span>Incremental Sync: Mutate 100 Records</span>
                </div>
                <p className="mt-2 text-xs text-zinc-400 leading-relaxed">
                  Updates 100 random customer rows in PostgreSQL (`version = version + 1`, `updated_at = NOW()`). Watch the incremental sync worker catch up in sub-second latency and update Elasticsearch.
                </p>
                <button
                  onClick={handleMutateRecords}
                  className="mt-4 rounded-lg bg-blue-500/20 border border-blue-500/30 px-4 py-2 text-xs font-semibold text-blue-300 hover:bg-blue-500/30 transition"
                >
                  Mutate 100 Source Records
                </button>
              </div>

              {/* Gate 1 Terminal Chaos */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-5">
                <div className="flex items-center space-x-2 text-zinc-200 font-semibold text-sm">
                  <Server className="h-4 w-4" />
                  <span>Gate 1: Crash Recovery (`docker kill`)</span>
                </div>
                <p className="mt-2 text-xs text-zinc-400 leading-relaxed">
                  Simulate sudden pod termination during high-throughput backfill:
                </p>
                <pre className="mt-3 rounded bg-zinc-950 p-2.5 font-mono text-[11px] text-zinc-300 border border-zinc-800">
                  docker kill optio-pipeline
                </pre>
                <p className="mt-2 text-[11px] text-zinc-500">
                  Container auto-restarts and resumes strictly from `last_processed_id`.
                </p>
              </div>

              {/* Gate 3 Outage Chaos */}
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-5">
                <div className="flex items-center space-x-2 text-zinc-200 font-semibold text-sm">
                  <Radio className="h-4 w-4" />
                  <span>Gate 3: Sink Outage & Backoff Test</span>
                </div>
                <p className="mt-2 text-xs text-zinc-400 leading-relaxed">
                  Stop Elasticsearch during active replication to observe exponential backoff without CPU busy-loops:
                </p>
                <pre className="mt-3 rounded bg-zinc-950 p-2.5 font-mono text-[11px] text-zinc-300 border border-zinc-800">
                  docker stop optio-elasticsearch
                </pre>
                <p className="mt-2 text-[11px] text-zinc-500">
                  CPU stays under 5%. Restart container (`docker start optio-elasticsearch`) to observe instant recovery.
                </p>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
