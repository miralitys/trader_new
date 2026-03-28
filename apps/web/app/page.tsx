"use client";

import Link from "next/link";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useHealth, useResearchSummary, useSyncJobs } from "@/lib/query-hooks";
import { formatDateTime, formatInteger, formatNumber, formatPercent, getErrorMessage } from "@/lib/utils";

export default function ResearchOverviewPage() {
  const healthQuery = useHealth();
  const researchQuery = useResearchSummary();
  const syncJobsQuery = useSyncJobs({ limit: 5 });

  if ((healthQuery.isLoading || researchQuery.isLoading || syncJobsQuery.isLoading) && !researchQuery.data) {
    return <LoadingState label="Loading research workspace..." />;
  }

  const error = healthQuery.error ?? researchQuery.error ?? syncJobsQuery.error;
  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load research workspace.")} />;
  }

  const health = healthQuery.data;
  const research = researchQuery.data;
  const syncJobs = syncJobsQuery.data ?? [];

  if (!health || !research) {
    return <ErrorState message="Research payload is empty." />;
  }

  const readySeries = research.coverage.filter((item) => item.ready_for_pattern_scan);
  const candidatePatterns = research.patterns.filter((item) => item.verdict === "candidate");
  const latestJob = syncJobs[0] ?? null;

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Research Workspace"
        title="Recurring pattern discovery"
        description="We removed the old strategy, paper, and backtest layer. This workspace is now focused on building a two-year multi-timeframe dataset, mining repeated patterns, and validating whether those patterns survive realistic friction."
        actions={
          <>
            <StatusBadge status={health.status} />
            <Link
              href="/data"
              className="rounded-2xl border border-sky-400/20 bg-sky-500/[0.08] px-4 py-3 text-sm font-medium text-sky-100 transition hover:bg-sky-500/[0.14]"
            >
              Open data desk
            </Link>
          </>
        }
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Tracked series" value={formatInteger(research.coverage.length)} hint={`${formatInteger(readySeries.length)} ready for scanning`} tone={readySeries.length ? "positive" : "warning"} />
        <MetricCard label="Candidate patterns" value={formatInteger(candidatePatterns.length)} hint={`${formatInteger(research.patterns.length)} scored summaries`} tone={candidatePatterns.length ? "positive" : "warning"} />
        <MetricCard label="Latest sync" value={latestJob ? `${latestJob.symbol} ${latestJob.timeframe}` : "No jobs"} hint={latestJob ? formatDateTime(latestJob.updated_at) : "Run manual syncs from Data"} tone={latestJob?.status === "failed" ? "danger" : latestJob ? "positive" : "default"} />
        <MetricCard label="Research window" value={`${research.lookback_days}d`} hint={`${research.forward_bars} forward bars · ${research.max_bars_per_series} bars max per series`} />
      </section>

      <div className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <SectionCard title="Top pattern candidates" eyebrow="Friction-adjusted leaderboard">
          <DataTable
            rows={research.patterns.slice(0, 12)}
            rowKey={(row) => `${row.symbol}-${row.timeframe}-${row.pattern_code}`}
            emptyTitle="No patterns scored yet"
            emptyDescription="Load more history in the Data tab, then refresh this page."
            columns={[
              {
                key: "pattern",
                title: "Pattern",
                render: (row) => (
                  <div className="grid gap-1">
                    <span className="font-medium text-white">{row.pattern_name}</span>
                    <span className="text-xs text-slate-400">{row.pattern_code}</span>
                  </div>
                ),
              },
              {
                key: "market",
                title: "Market",
                render: (row) => `${row.symbol} · ${row.timeframe}`,
              },
              {
                key: "sample",
                title: "Samples",
                render: (row) => formatInteger(row.sample_size),
              },
              {
                key: "winrate",
                title: "Win rate",
                render: (row) => formatPercent(row.win_rate_pct),
              },
              {
                key: "forward",
                title: "Avg forward",
                render: (row) => formatPercent(row.avg_forward_return_pct),
              },
              {
                key: "net",
                title: "Avg net",
                render: (row) => formatPercent(row.avg_net_return_pct),
              },
              {
                key: "verdict",
                title: "Verdict",
                render: (row) => <StatusBadge status={row.verdict} />,
              },
            ]}
          />
        </SectionCard>

        <SectionCard title="Research assumptions" eyebrow="Kept explicit">
          <div className="grid gap-4">
            <MetricCard label="Fee assumption" value={formatPercent(research.fee_pct)} hint="Per side" />
            <MetricCard label="Slippage assumption" value={formatPercent(research.slippage_pct)} hint="Per side" />
            <MetricCard label="Exchange scope" value={research.exchange_code} hint={health.environment} />
            <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Method notes</p>
              <ul className="mt-3 grid gap-2 text-sm leading-6 text-slate-300">
                {research.notes.map((note) => (
                  <li key={note}>{note}</li>
                ))}
              </ul>
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="Coverage matrix" eyebrow="Loaded history by symbol and timeframe">
        <DataTable
          rows={research.coverage}
          rowKey={(row) => `${row.symbol}-${row.timeframe}`}
          emptyTitle="No history loaded"
          emptyDescription="Start with Data -> Run data sync and load 1m / 5m / 15m / 1h history."
          columns={[
            {
              key: "market",
              title: "Market",
              render: (row) => `${row.symbol} · ${row.timeframe}`,
            },
            {
              key: "candles",
              title: "Candles",
              render: (row) => formatInteger(row.candle_count),
            },
            {
              key: "range",
              title: "Loaded range",
              render: (row) =>
                row.loaded_start_at && row.loaded_end_at
                  ? `${formatDateTime(row.loaded_start_at)} -> ${formatDateTime(row.loaded_end_at)}`
                  : "No data",
            },
            {
              key: "completion",
              title: "Completion",
              render: (row) => formatNumber(row.completion_pct, 2),
            },
            {
              key: "ready",
              title: "Ready",
              render: (row) => <StatusBadge status={row.ready_for_pattern_scan ? "ready" : "insufficient_history"} />,
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}
