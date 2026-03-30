"use client";

import { useState } from "react";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useBacktests, usePatternScans, useRunBacktest, useStrategies } from "@/lib/query-hooks";
import { aggregateApprovedStrategyCandidates } from "@/lib/strategy-layer";
import { formatCurrency, formatDateTime, formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

export default function BacktestsPage() {
  const runsQuery = usePatternScans(200, true);
  const strategiesQuery = useStrategies(true);
  const backtestsQuery = useBacktests({ limit: 100 }, true);
  const runBacktestMutation = useRunBacktest();
  const [activeStrategyCode, setActiveStrategyCode] = useState<string | null>(null);

  const isLoading = (runsQuery.isLoading && !runsQuery.data) || (strategiesQuery.isLoading && !strategiesQuery.data) || (backtestsQuery.isLoading && !backtestsQuery.data);
  const error = runsQuery.error ?? strategiesQuery.error ?? backtestsQuery.error;

  if (isLoading) {
    return <LoadingState label="Loading backtest layer..." />;
  }

  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load backtest layer.")} />;
  }

  const runs = runsQuery.data ?? [];
  const strategies = strategiesQuery.data ?? [];
  const backtests = backtestsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const candidates = aggregateApprovedStrategyCandidates(completedRuns);
  const registeredCodes = new Set(strategies.map((row) => row.code));
  const latestBacktestByStrategy = new Map<string, (typeof backtests)[number]>();

  for (const row of backtests) {
    if (!latestBacktestByStrategy.has(row.strategy_code)) {
      latestBacktestByStrategy.set(row.strategy_code, row);
    }
  }

  const executableCandidates = candidates.filter((row) => registeredCodes.has(row.strategyCode));
  const liveBacktests = backtests.filter((row) => row.status === "queued" || row.status === "running").length;

  async function handleRunBaseline(strategyCode: string, symbol: string, timeframe: string, lookbackDays: number) {
    setActiveStrategyCode(strategyCode);
    const endAt = new Date();
    const startAt = new Date(endAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
    try {
      await runBacktestMutation.mutateAsync({
        strategy_code: strategyCode,
        symbol,
        timeframe,
        start_at: startAt.toISOString(),
        end_at: endAt.toISOString(),
        exchange_code: "binance_us",
        initial_capital: 10000,
        fee: 0.001,
        slippage: 0.0005,
        position_size_pct: 0.1,
        strategy_config_override: {},
      });
    } finally {
      setActiveStrategyCode(null);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Replay Layer"
        title="Backtests"
        description="We can now launch baseline replay runs directly from the approved pool. Each row maps a validated setup to a real backend strategy code and a first historical replay window."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Executable setups" value={formatInteger(executableCandidates.length)} hint="Approved candidates with registered backend strategies" tone={executableCandidates.length ? "positive" : "warning"} />
        <MetricCard label="Backtests in flight" value={formatInteger(liveBacktests)} hint="Queued or running replay jobs" tone={liveBacktests ? "warning" : "default"} />
        <MetricCard label="Finished backtests" value={formatInteger(backtests.filter((row) => row.status === "completed").length)} hint="Completed replay runs already stored" />
        <MetricCard label="Baseline mode" value="One-click" hint="Each row can launch its first replay directly from this page" tone="positive" />
      </section>

      {runBacktestMutation.error ? (
        <ErrorState message={getErrorMessage(runBacktestMutation.error, "Unable to start backtest.")} />
      ) : null}

      <SectionCard title="Replay Queue" eyebrow="Approved pool with live run control">
        <DataTable
          rows={executableCandidates}
          rowKey={(row) => row.key}
          emptyTitle="No executable backtest candidates yet"
          emptyDescription="Executable approved setups will appear here once registered backend strategies are available."
          columns={[
            { key: "priority", title: "Priority", render: (row) => formatInteger(row.priority) },
            {
              key: "setup",
              title: "Setup",
              render: (row) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{row.patternName}</span>
                  <span className="text-xs text-slate-400">
                    {row.symbol} · {row.timeframe}
                  </span>
                  <span className="text-[11px] text-slate-500">{row.strategyCode}</span>
                </div>
              ),
            },
            {
              key: "baseline",
              title: "Baseline replay",
              render: (row) => (
                <div className="grid gap-1 text-sm text-slate-200">
                  <span>
                    {row.bestLookbackDays}d · +{row.bestForwardBars} bars
                  </span>
                  <span className="text-xs text-slate-400">{formatInteger(row.bestMaxBarsPerSeries)} max bars</span>
                </div>
              ),
            },
            {
              key: "latest",
              title: "Latest run",
              render: (row) => {
                const latest = latestBacktestByStrategy.get(row.strategyCode);
                if (!latest) {
                  return <span className="text-sm text-slate-400">No run yet</span>;
                }
                return (
                  <div className="grid gap-1">
                    <StatusBadge status={latest.status} />
                    <span className="text-xs text-slate-400">{formatDateTime(latest.started_at)}</span>
                  </div>
                );
              },
            },
            {
              key: "result",
              title: "Latest result",
              render: (row) => {
                const latest = latestBacktestByStrategy.get(row.strategyCode);
                if (!latest) {
                  return <span className="text-sm text-slate-400">Pending first replay</span>;
                }
                return (
                  <div className="grid gap-1">
                    <span>{formatPercent(latest.total_return_pct)}</span>
                    <span className="text-xs text-slate-400">{formatInteger(latest.total_trades)} trades</span>
                  </div>
                );
              },
            },
            {
              key: "action",
              title: "Action",
              render: (row) => {
                const isActive = activeStrategyCode === row.strategyCode && runBacktestMutation.isPending;
                return (
                  <button
                    type="button"
                    onClick={() => handleRunBaseline(row.strategyCode, row.symbol, row.timeframe, row.bestLookbackDays)}
                    disabled={runBacktestMutation.isPending}
                    className="rounded-xl border border-emerald-400/25 bg-emerald-400/10 px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-emerald-100 transition hover:border-emerald-300/40 hover:bg-emerald-300/15 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {isActive ? "Starting..." : "Run baseline"}
                  </button>
                );
              },
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="Recent Backtests" eyebrow="Stored replay history">
        <DataTable
          rows={backtests}
          rowKey={(row) => row.id}
          emptyTitle="No backtests yet"
          emptyDescription="Once we start replay runs, they will show up here automatically."
          columns={[
            { key: "run", title: "Run", render: (row) => `#${row.id}` },
            {
              key: "strategy",
              title: "Strategy",
              render: (row) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{row.strategy_name}</span>
                  <span className="text-xs text-slate-400">{row.strategy_code}</span>
                </div>
              ),
            },
            { key: "market", title: "Market", render: (row) => `${row.symbol} · ${row.timeframe}` },
            { key: "status", title: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "started", title: "Started", render: (row) => formatDateTime(row.started_at) },
            {
              key: "return",
              title: "Return / DD",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatPercent(row.total_return_pct)}</span>
                  <span className="text-xs text-slate-400">DD {formatPercent(row.max_drawdown_pct)}</span>
                </div>
              ),
            },
            {
              key: "equity",
              title: "Final equity",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatCurrency(row.final_equity)}</span>
                  <span className="text-xs text-slate-400">{formatInteger(row.total_trades)} trades</span>
                </div>
              ),
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}
