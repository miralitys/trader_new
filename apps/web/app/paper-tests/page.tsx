"use client";

import { useState } from "react";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import {
  usePatternScans,
  useStartStrategyPaperRun,
  useStopStrategyPaperRun,
  useStrategies,
  useStrategyRuns,
} from "@/lib/query-hooks";
import { aggregateApprovedStrategyCandidates, timeframePaperWindow } from "@/lib/strategy-layer";
import { formatCurrency, formatDateTime, formatInteger, getErrorMessage } from "@/lib/utils";

export default function PaperTestsPage() {
  const runsQuery = usePatternScans(200, true);
  const strategiesQuery = useStrategies(true);
  const paperRunsQuery = useStrategyRuns({ mode: "paper", limit: 100 }, true);
  const startPaperMutation = useStartStrategyPaperRun();
  const stopPaperMutation = useStopStrategyPaperRun();
  const [activeStrategyCode, setActiveStrategyCode] = useState<string | null>(null);

  const isLoading = (runsQuery.isLoading && !runsQuery.data) || (strategiesQuery.isLoading && !strategiesQuery.data) || (paperRunsQuery.isLoading && !paperRunsQuery.data);
  const error = runsQuery.error ?? strategiesQuery.error ?? paperRunsQuery.error;

  if (isLoading) {
    return <LoadingState label="Loading paper-test layer..." />;
  }

  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load paper-test layer.")} />;
  }

  const runs = runsQuery.data ?? [];
  const strategies = strategiesQuery.data ?? [];
  const paperRuns = paperRunsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const candidates = aggregateApprovedStrategyCandidates(completedRuns);
  const strategyByCode = new Map(strategies.map((row) => [row.code, row]));
  const executableCandidates = candidates.filter((row) => strategyByCode.has(row.strategyCode));
  const activePaperRuns = paperRuns.filter((row) => row.status === "running" || row.status === "created").length;

  async function handleStartPaper(strategyCode: string, symbol: string, timeframe: string) {
    setActiveStrategyCode(strategyCode);
    try {
      await startPaperMutation.mutateAsync({
        strategyCode,
        payload: {
          symbols: [symbol],
          timeframes: [timeframe],
          exchange_code: "binance_us",
          start_from_latest: true,
          initial_balance: 10000,
          currency: "USD",
          fee: 0.001,
          slippage: 0.0005,
          strategy_config_override: {},
          metadata: {
            launched_from: "paper_tests_page",
          },
        },
      });
    } finally {
      setActiveStrategyCode(null);
    }
  }

  async function handleStopPaper(strategyCode: string) {
    setActiveStrategyCode(strategyCode);
    try {
      await stopPaperMutation.mutateAsync({
        strategyCode,
        payload: {
          reason: "manual_stop_from_paper_tests_page",
        },
      });
    } finally {
      setActiveStrategyCode(null);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Forward Layer"
        title="Paper Tests"
        description="We can now start and stop latest-only paper runs directly from the approved pool. This page is where we watch whether the live post-nightly tape still behaves like the setup that earned promotion."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Executable setups" value={formatInteger(executableCandidates.length)} hint="Approved candidates with registered backend strategies" tone={executableCandidates.length ? "positive" : "warning"} />
        <MetricCard label="Paper runs live" value={formatInteger(activePaperRuns)} hint="Currently active forward-paper runs" tone={activePaperRuns ? "warning" : "default"} />
        <MetricCard label="Swing tempo" value={formatInteger(executableCandidates.filter((row) => row.timeframe === "1h" || row.timeframe === "4h").length)} hint="Watch these over weeks, not hours" />
        <MetricCard label="Latest-only mode" value="Enabled" hint="Every forward run starts from the freshest nightly slice" tone="positive" />
      </section>

      {startPaperMutation.error ? <ErrorState message={getErrorMessage(startPaperMutation.error, "Unable to start paper run.")} /> : null}
      {stopPaperMutation.error ? <ErrorState message={getErrorMessage(stopPaperMutation.error, "Unable to stop paper run.")} /> : null}

      <SectionCard title="Forward Queue" eyebrow="Approved pool with live paper control">
        <DataTable
          rows={executableCandidates}
          rowKey={(row) => row.key}
          emptyTitle="No executable paper candidates yet"
          emptyDescription="Executable approved setups will appear here once registered backend strategies are available."
          columns={[
            { key: "priority", title: "Priority", render: (row) => formatInteger(row.priority) },
            {
              key: "setup",
              title: "Setup",
              render: (row) => {
                const strategy = strategyByCode.get(row.strategyCode);
                return (
                  <div className="grid gap-1">
                    <span className="font-medium text-white">{row.patternName}</span>
                    <span className="text-xs text-slate-400">
                      {row.symbol} · {row.timeframe}
                    </span>
                    <span className="text-[11px] text-slate-500">{strategy?.code ?? row.strategyCode}</span>
                  </div>
                );
              },
            },
            {
              key: "plan",
              title: "Paper plan",
              render: (row) => (
                <div className="grid gap-1 text-sm text-slate-200">
                  <span>Latest-only forward paper</span>
                  <span className="text-xs text-slate-400">{timeframePaperWindow(row.timeframe)} observation window</span>
                </div>
              ),
            },
            {
              key: "active",
              title: "Current state",
              render: (row) => {
                const strategy = strategyByCode.get(row.strategyCode);
                if (!strategy) {
                  return <StatusBadge status="idle" />;
                }
                return <StatusBadge status={strategy.active_paper_status ?? "idle"} />;
              },
            },
            {
              key: "balance",
              title: "Active run",
              render: (row) => {
                const strategy = strategyByCode.get(row.strategyCode);
                if (!strategy?.active_paper_run_id) {
                  return <span className="text-sm text-slate-400">No active run</span>;
                }
                return (
                  <div className="grid gap-1">
                    <span>Run #{strategy.active_paper_run_id}</span>
                    <span className="text-xs text-slate-400">{strategy.active_paper_status}</span>
                  </div>
                );
              },
            },
            {
              key: "action",
              title: "Action",
              render: (row) => {
                const strategy = strategyByCode.get(row.strategyCode);
                const hasActive = Boolean(strategy?.active_paper_run_id);
                const isBusy = activeStrategyCode === row.strategyCode && (startPaperMutation.isPending || stopPaperMutation.isPending);
                return hasActive ? (
                  <button
                    type="button"
                    onClick={() => handleStopPaper(row.strategyCode)}
                    disabled={startPaperMutation.isPending || stopPaperMutation.isPending}
                    className="rounded-xl border border-rose-400/25 bg-rose-400/10 px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-rose-100 transition hover:border-rose-300/40 hover:bg-rose-300/15 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {isBusy ? "Stopping..." : "Stop"}
                  </button>
                ) : (
                  <button
                    type="button"
                    onClick={() => handleStartPaper(row.strategyCode, row.symbol, row.timeframe)}
                    disabled={startPaperMutation.isPending || stopPaperMutation.isPending}
                    className="rounded-xl border border-emerald-400/25 bg-emerald-400/10 px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-emerald-100 transition hover:border-emerald-300/40 hover:bg-emerald-300/15 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {isBusy ? "Starting..." : "Start"}
                  </button>
                );
              },
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="Recent Paper Runs" eyebrow="Stored forward-paper history">
        <DataTable
          rows={paperRuns}
          rowKey={(row) => row.id}
          emptyTitle="No paper runs yet"
          emptyDescription="Once we start paper runs, they will show up here automatically."
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
            { key: "market", title: "Streams", render: (row) => `${row.symbols.join(", ")} · ${row.timeframes.join(", ")}` },
            { key: "status", title: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "started", title: "Started", render: (row) => formatDateTime(row.started_at ?? row.created_at) },
            {
              key: "balance",
              title: "Account",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatCurrency(row.account_balance ?? 0, row.currency ?? "USD")}</span>
                  <span className="text-xs text-slate-400">{formatInteger(row.open_positions_count)} open positions</span>
                </div>
              ),
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}
