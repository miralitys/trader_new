"use client";

import { useState } from "react";

import { CandleQueryForm } from "@/components/forms/candle-query-form";
import { DataSyncForm } from "@/components/forms/data-sync-form";
import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useCandles, useSyncJobs } from "@/lib/query-hooks";
import type { CandleFilters } from "@/lib/types";
import { formatCurrency, formatDateTime, formatInteger, getErrorMessage } from "@/lib/utils";

export default function DataPage() {
  const [statusFilter, setStatusFilter] = useState("");
  const [symbolFilter, setSymbolFilter] = useState("");
  const [timeframeFilter, setTimeframeFilter] = useState("");
  const [candleFilters, setCandleFilters] = useState<CandleFilters | null>(null);

  const syncJobsQuery = useSyncJobs({
    status: statusFilter || undefined,
    symbol: symbolFilter || undefined,
    timeframe: timeframeFilter || undefined,
    limit: 100,
  });
  const candlesQuery = useCandles(candleFilters, Boolean(candleFilters));

  const candles = candlesQuery.data ?? [];
  const coverage = candles.length
    ? {
        candleCount: candles.length,
        first: candles[0].open_time,
        last: candles[candles.length - 1].open_time,
        minLow: Math.min(...candles.map((candle) => Number(candle.low))),
        maxHigh: Math.max(...candles.map((candle) => Number(candle.high))),
      }
    : null;

  if (syncJobsQuery.isLoading && !syncJobsQuery.data) {
    return <LoadingState label="Loading data operations..." />;
  }

  if (syncJobsQuery.error) {
    return <ErrorState message={getErrorMessage(syncJobsQuery.error, "Unable to load sync jobs.")} />;
  }

  if (candlesQuery.error) {
    return <ErrorState message={getErrorMessage(candlesQuery.error, "Unable to load candles.")} />;
  }

  const syncJobs = syncJobsQuery.data ?? [];
  const runningCount = syncJobs.filter((job) => job.status === "running").length;
  const failedCount = syncJobs.filter((job) => job.status === "failed").length;
  const latestJob = syncJobs[0] ?? null;

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Data Operations"
        title="Market data manager"
        description="Trigger historical sync jobs, review ingestion status, and query stored candles for coverage validation."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Jobs loaded" value={formatInteger(syncJobs.length)} />
        <MetricCard label="Running jobs" value={formatInteger(runningCount)} tone={runningCount ? "warning" : "default"} />
        <MetricCard label="Failed jobs" value={formatInteger(failedCount)} tone={failedCount ? "danger" : "default"} />
        <MetricCard
          label="Latest insert"
          value={latestJob ? formatInteger(latestJob.rows_inserted) : "0"}
          hint={latestJob ? `${latestJob.symbol} · ${latestJob.timeframe}` : "No jobs yet"}
          tone={latestJob?.status === "completed" ? "positive" : latestJob?.status === "failed" ? "danger" : "default"}
        />
      </section>

      <SectionCard title="Run data sync" eyebrow="Manual and incremental ingestion">
        <DataSyncForm />
      </SectionCard>

      <SectionCard title="Sync jobs status" eyebrow="Recent ingestion history">
        <div className="mb-5 grid gap-4 border-b border-white/6 pb-5 md:grid-cols-2 xl:grid-cols-3">
          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Status</span>
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)} className={inputClassName}>
              <option value="">All statuses</option>
              <option value="queued">queued</option>
              <option value="running">running</option>
              <option value="completed">completed</option>
              <option value="failed">failed</option>
            </select>
          </label>

          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Symbol</span>
            <input value={symbolFilter} onChange={(event) => setSymbolFilter(event.target.value)} className={inputClassName} placeholder="BTC-USDT or ARB-USDT" />
          </label>

          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Timeframe</span>
            <input value={timeframeFilter} onChange={(event) => setTimeframeFilter(event.target.value)} className={inputClassName} placeholder="5m" />
          </label>
        </div>

        <DataTable
          rows={syncJobs}
          rowKey={(job) => job.id}
          emptyTitle="No sync jobs found"
          emptyDescription="Run a sync job to populate ingestion history."
          columns={[
            {
              key: "job",
              title: "Job",
              render: (job) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">#{job.id}</span>
                  <span className="text-xs text-slate-400">{formatDateTime(job.updated_at)}</span>
                </div>
              ),
            },
            {
              key: "market",
              title: "Market",
              render: (job) => `${job.symbol} · ${job.timeframe}`,
            },
            {
              key: "range",
              title: "Range",
              render: (job) => `${formatDateTime(job.start_at)} -> ${formatDateTime(job.end_at)}`,
            },
            {
              key: "rows",
              title: "Rows",
              render: (job) => formatInteger(job.rows_inserted),
            },
            {
              key: "status",
              title: "Status",
              render: (job) => <StatusBadge status={job.status} />,
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="Candles query" eyebrow="Coverage and spot checks">
        <CandleQueryForm onSubmit={setCandleFilters} />

        {candlesQuery.isLoading ? (
          <div className="mt-5">
            <LoadingState label="Loading candles..." />
          </div>
        ) : null}

        {coverage ? (
          <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Candle count" value={formatInteger(coverage.candleCount)} />
            <MetricCard label="First candle" value={formatDateTime(coverage.first)} />
            <MetricCard label="Last candle" value={formatDateTime(coverage.last)} />
            <MetricCard label="Price range" value={`${formatCurrency(coverage.minLow)} - ${formatCurrency(coverage.maxHigh)}`} />
          </div>
        ) : candleFilters ? (
          <p className="mt-5 text-sm text-slate-400">No candles matched the requested range.</p>
        ) : (
          <p className="mt-5 text-sm text-slate-400">Submit a candle query to inspect historical coverage.</p>
        )}

        <div className="mt-5">
          <DataTable
            rows={candles}
            rowKey={(candle) => candle.id}
            emptyTitle="No candles loaded"
            emptyDescription="Run a candle query to populate this table."
            columns={[
              {
                key: "time",
                title: "Open time",
                render: (candle) => formatDateTime(candle.open_time),
              },
              {
                key: "ohlc",
                title: "OHLC",
                render: (candle) =>
                  `${formatCurrency(candle.open)} / ${formatCurrency(candle.high)} / ${formatCurrency(candle.low)} / ${formatCurrency(candle.close)}`,
              },
              {
                key: "volume",
                title: "Volume",
                render: (candle) => formatInteger(candle.volume),
              },
            ]}
          />
        </div>
      </SectionCard>
    </div>
  );
}

const inputClassName =
  "h-11 rounded-xl border border-white/10 bg-slate-950/60 px-3 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-sky-400/40";
