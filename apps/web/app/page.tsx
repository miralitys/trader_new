"use client";

import Link from "next/link";

import { StrategyCard } from "@/components/dashboard/strategy-card";
import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useDashboardSummary, useHealth, usePositions, useStrategyRuns } from "@/lib/query-hooks";
import { formatCurrency, formatDateTime, formatInteger, formatNumber, formatPercent, formatStatusLabel, getErrorMessage } from "@/lib/utils";

export default function DashboardPage() {
  const dashboardQuery = useDashboardSummary();
  const healthQuery = useHealth();
  const runsQuery = useStrategyRuns({ mode: "paper", limit: 200 });
  const positionsQuery = usePositions({ status: "open", limit: 500 });

  const isLoading =
    dashboardQuery.isLoading ||
    healthQuery.isLoading ||
    runsQuery.isLoading ||
    positionsQuery.isLoading;
  const error = dashboardQuery.error ?? healthQuery.error ?? runsQuery.error ?? positionsQuery.error;

  if (isLoading && !dashboardQuery.data) {
    return <LoadingState label="Loading dashboard..." />;
  }

  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load dashboard data.")} />;
  }

  const dashboard = dashboardQuery.data;
  const health = healthQuery.data;

  if (!dashboard || !health) {
    return <ErrorState message="Dashboard payload is empty." />;
  }

  const runs = runsQuery.data ?? [];
  const positions = positionsQuery.data ?? [];
  const performanceByCode = new Map(dashboard.key_performance_metrics.map((metric) => [metric.strategy_code, metric]));
  const runById = new Map(runs.map((run) => [run.id, run]));
  const latestRunByStrategy = new Map<string, (typeof runs)[number]>();

  for (const run of runs) {
    const current = latestRunByStrategy.get(run.strategy_code);
    if (!current || run.status === "running") {
      latestRunByStrategy.set(run.strategy_code, run);
    }
  }

  const openPositionsByStrategy = new Map<string, number>();
  for (const position of positions) {
    const run = runById.get(position.strategy_run_id);
    if (!run) {
      continue;
    }

    openPositionsByStrategy.set(run.strategy_code, (openPositionsByStrategy.get(run.strategy_code) ?? 0) + 1);
  }

  const strategyCards = dashboard.strategies.map((strategy) => {
    const performance = performanceByCode.get(strategy.code);
    const run = latestRunByStrategy.get(strategy.code);

    return {
      code: strategy.code,
      name: strategy.name,
      description: strategy.description,
      status: strategy.active_paper_status ?? run?.status ?? "idle",
      totalReturnPct: performance?.total_return_pct ?? 0,
      winRatePct: performance?.win_rate_pct ?? 0,
      totalTrades: performance?.total_trades ?? 0,
      maxDrawdownPct: performance?.max_drawdown_pct ?? 0,
      openPositions: openPositionsByStrategy.get(strategy.code) ?? 0,
      symbols: run?.symbols ?? [],
      timeframes: run?.timeframes ?? [],
    };
  });

  const latestJob = dashboard.data_sync_status.latest_job;

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Quant Command Center"
        title="Portfolio dashboard"
        description="Monitor paper execution, inspect recent backtests, and keep data coverage healthy from one operator console."
        actions={
          <>
            <StatusBadge status={health.status} />
            <div className="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.18em] text-slate-400">Environment</p>
              <p className="mt-2 text-sm text-white">{health.environment}</p>
            </div>
          </>
        }
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="Active paper runs"
          value={formatInteger(dashboard.run_status.active_paper_runs)}
          hint={`${formatInteger(dashboard.run_status.failed_paper_runs)} failed`}
          tone={dashboard.run_status.failed_paper_runs ? "warning" : "positive"}
        />
        <MetricCard
          label="Open positions"
          value={formatInteger(dashboard.open_positions_count)}
          hint="Focused strategy only"
          tone={dashboard.open_positions_count ? "positive" : "default"}
        />
        <MetricCard
          label="Recent backtests"
          value={formatInteger(dashboard.run_status.recent_backtests)}
          hint={`${dashboard.recent_backtests.length} shown on dashboard`}
        />
        <MetricCard
          label="Latest sync"
          value={latestJob ? `${latestJob.symbol} ${latestJob.timeframe}` : "No jobs"}
          hint={latestJob ? `${formatInteger(latestJob.rows_inserted)} rows · ${formatStatusLabel(latestJob.status)}` : "Run data sync"}
          tone={latestJob?.status === "failed" ? "danger" : latestJob ? "positive" : "default"}
        />
      </section>

      <SectionCard
        title="Strategy"
        eyebrow="Isolated runtime snapshots"
        actions={
          <Link href="/backtests" className="text-sm text-sky-300 transition hover:text-sky-200">
            Open research desk
          </Link>
        }
      >
        <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-4">
          {strategyCards.map((strategy) => (
            <StrategyCard key={strategy.code} strategy={strategy} />
          ))}
        </div>
      </SectionCard>

      <div className="grid gap-6 xl:grid-cols-[1.2fr_0.95fr]">
        <SectionCard title="Recent trades" eyebrow="Paper execution">
          <DataTable
            rows={dashboard.recent_trades}
            rowKey={(trade) => trade.id}
            emptyTitle="No trades yet"
            emptyDescription="Start paper trading and close positions to populate the blotter."
            columns={[
              {
                key: "market",
                title: "Market",
                render: (trade) => (
                  <div className="grid gap-1">
                    {trade.strategy_code ? (
                      <Link href={`/strategies/${trade.strategy_code}`} className="font-medium text-white transition hover:text-sky-300">
                        {trade.symbol}
                      </Link>
                    ) : (
                      <span className="font-medium text-white">{trade.symbol}</span>
                    )}
                    <span className="text-xs text-slate-400">{trade.strategy_code ?? "unknown strategy"}</span>
                  </div>
                ),
              },
              {
                key: "pnl",
                title: "PnL",
                render: (trade) => (
                  <span className={Number(trade.pnl) >= 0 ? "text-emerald-300" : "text-rose-300"}>{formatCurrency(trade.pnl)}</span>
                ),
              },
              {
                key: "qty",
                title: "Qty",
                render: (trade) => formatNumber(trade.qty, 6),
              },
              {
                key: "opened",
                title: "Opened",
                render: (trade) => formatDateTime(trade.opened_at),
              },
              {
                key: "closed",
                title: "Closed",
                render: (trade) => formatDateTime(trade.closed_at),
              },
            ]}
          />
        </SectionCard>

        <SectionCard title="Sync status" eyebrow="Historical ingestion">
          <div className="grid gap-4 sm:grid-cols-2">
            <MetricCard
              label="Latest job"
              value={latestJob ? `${latestJob.symbol} · ${latestJob.timeframe}` : "No data"}
              hint={latestJob ? formatDateTime(latestJob.updated_at) : "Queue is empty"}
              tone={latestJob?.status === "failed" ? "danger" : latestJob ? "positive" : "default"}
            />
            <MetricCard
              label="Latest inserted"
              value={latestJob ? formatInteger(latestJob.rows_inserted) : "0"}
              hint={latestJob ? formatStatusLabel(latestJob.status) : "No sync jobs"}
            />
          </div>

          <div className="mt-5 space-y-3">
            {dashboard.data_sync_status.recent_jobs.length ? (
              dashboard.data_sync_status.recent_jobs.map((job) => (
                <div key={job.id} className="flex items-center justify-between gap-4 rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="min-w-0">
                    <p className="font-medium text-white">
                      {job.symbol} · {job.timeframe}
                    </p>
                    <p className="text-sm text-slate-400">{formatDateTime(job.updated_at)}</p>
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-sm text-slate-300">{formatInteger(job.rows_inserted)} rows</span>
                    <StatusBadge status={job.status} />
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-slate-400">No sync jobs have been recorded yet.</p>
            )}
          </div>
        </SectionCard>
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.15fr_1fr]">
        <SectionCard title="Recent backtests" eyebrow="Research activity">
          <DataTable
            rows={dashboard.recent_backtests}
            rowKey={(backtest) => backtest.id}
            emptyTitle="No backtests yet"
            emptyDescription="Run a backtest to populate this table."
            columns={[
              {
                key: "run",
                title: "Run",
                render: (backtest) => (
                  <div className="grid gap-1">
                    <Link href={`/backtests/${backtest.id}`} className="font-medium text-white transition hover:text-sky-300">
                      #{backtest.id}
                    </Link>
                    <span className="text-xs text-slate-400">{backtest.strategy_name}</span>
                  </div>
                ),
              },
              {
                key: "market",
                title: "Market",
                render: (backtest) => `${backtest.symbol} · ${backtest.timeframe}`,
              },
              {
                key: "return",
                title: "Return",
                render: (backtest) => formatPercent(backtest.total_return_pct),
              },
              {
                key: "equity",
                title: "Final equity",
                render: (backtest) => formatCurrency(backtest.final_equity),
              },
              {
                key: "status",
                title: "Status",
                render: (backtest) => <StatusBadge status={backtest.status} />,
              },
            ]}
          />
        </SectionCard>

        <SectionCard title="Key metrics" eyebrow="Latest completed backtests">
          <div className="space-y-3">
            {dashboard.key_performance_metrics.length ? (
              dashboard.key_performance_metrics.map((metric) => (
                <Link
                  key={metric.backtest_run_id}
                  href={`/backtests/${metric.backtest_run_id}`}
                  className="flex items-center justify-between gap-4 rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 transition hover:border-sky-400/20 hover:bg-white/[0.05]"
                >
                  <div className="min-w-0">
                    <p className="font-medium text-white">{metric.strategy_code}</p>
                    <p className="text-sm text-slate-400">
                      {metric.symbol} · {metric.timeframe}
                    </p>
                  </div>
                  <div className="grid justify-items-end gap-1 text-sm">
                    <span className="text-emerald-300">{formatPercent(metric.total_return_pct)}</span>
                    <span className="text-slate-400">
                      WR {formatPercent(metric.win_rate_pct)} · DD {formatPercent(metric.max_drawdown_pct)}
                    </span>
                  </div>
                </Link>
              ))
            ) : (
              <p className="text-sm text-slate-400">Performance snapshots will appear here after your first completed backtests.</p>
            )}
          </div>
        </SectionCard>
      </div>
    </div>
  );
}
