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
import { useCandleCoverage, useCandles, useSyncJobs } from "@/lib/query-hooks";
import type { CandleCoverage, CandleFilters } from "@/lib/types";
import { formatCurrency, formatDateTime, formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

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
  const coverageQuery = useCandleCoverage(candleFilters, Boolean(candleFilters));

  const candles = candlesQuery.data ?? [];
  const displayedSlice = candles.length
    ? {
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

  if (coverageQuery.error) {
    return <ErrorState message={getErrorMessage(coverageQuery.error, "Unable to load candle coverage.")} />;
  }

  const syncJobs = syncJobsQuery.data ?? [];
  const runningCount = syncJobs.filter((job) => job.status === "running").length;
  const failedCount = syncJobs.filter((job) => job.status === "failed").length;
  const latestJob = syncJobs[0] ?? null;
  const coverage = coverageQuery.data ?? null;

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
          hint={latestJob ? `${latestJob.symbol} · ${latestJob.timeframe} · new rows inserted` : "No jobs yet"}
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
            <input
              value={symbolFilter}
              onChange={(event) => setSymbolFilter(event.target.value)}
              className={inputClassName}
              placeholder="BTC-USDT, ICP-USDT, GALA-USDT, ONDO-USDT or FIL-USDT"
            />
          </label>

          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Timeframe</span>
            <input value={timeframeFilter} onChange={(event) => setTimeframeFilter(event.target.value)} className={inputClassName} placeholder="5m, 1h or 4h" />
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
              key: "requestedRange",
              title: "Requested range",
              render: (job) => `${formatDateTime(job.start_at)} -> ${formatDateTime(job.end_at)}`,
            },
            {
              key: "loadedRange",
              title: "Actual loaded range",
              render: (job) => formatLoadedRange(job.coverage),
            },
            {
              key: "coverage",
              title: "Coverage",
              render: (job) => formatCoverage(job.coverage),
            },
            {
              key: "rows",
              title: "New rows",
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

        {candlesQuery.isLoading || coverageQuery.isLoading ? (
          <div className="mt-5">
            <LoadingState label="Loading candles and coverage..." />
          </div>
        ) : null}

        {coverage ? (
          <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Loaded candles" value={formatInteger(coverage.candle_count)} />
            <MetricCard label="Loaded range start" value={formatDateTime(coverage.loaded_start_at)} />
            <MetricCard label="Loaded range end" value={formatDateTime(coverage.loaded_end_at)} />
            <MetricCard
              label="Coverage quality"
              value={`${formatInteger(coverage.candle_count)} / ${formatInteger(coverage.expected_candle_count)}`}
              hint={`${formatPercent(coverage.completion_pct)} loaded`}
              tone={coverage.missing_candle_count === 0 ? "positive" : "warning"}
            />
            <MetricCard label="Requested start" value={formatDateTime(coverage.requested_start_at)} />
            <MetricCard label="Requested end" value={formatDateTime(coverage.requested_end_at)} />
            <MetricCard label="Missing candles" value={formatInteger(coverage.missing_candle_count)} />
            <MetricCard
              label="Displayed slice range"
              value={
                displayedSlice
                  ? `${formatCurrency(displayedSlice.minLow)} - ${formatCurrency(displayedSlice.maxHigh)}`
                  : "N/A"
              }
              hint={displayedSlice ? "Based on the rows shown below" : "Run a candle query to inspect prices"}
            />
          </div>
        ) : candleFilters ? (
          <p className="mt-5 text-sm text-slate-400">No candles matched the requested range.</p>
        ) : (
          <p className="mt-5 text-sm text-slate-400">Submit a candle query to inspect historical coverage.</p>
        )}

        {coverage ? (
          <p className="mt-4 text-sm text-slate-400">
            Coverage metrics are aggregated over the full requested window. The table below is a spot-check slice and can
            be limited separately.
          </p>
        ) : null}

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

function formatLoadedRange(coverage: CandleCoverage | null) {
  if (!coverage?.loaded_start_at || !coverage?.loaded_end_at) {
    return "No candles loaded";
  }

  return `${formatDateTime(coverage.loaded_start_at)} -> ${formatDateTime(coverage.loaded_end_at)}`;
}

function formatCoverage(coverage: CandleCoverage | null) {
  if (!coverage) {
    return "N/A";
  }

  return `${formatInteger(coverage.candle_count)} / ${formatInteger(coverage.expected_candle_count)} · ${formatPercent(coverage.completion_pct)}`;
}
