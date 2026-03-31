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
  useBacktests,
  useRunBacktest,
  useStartStrategyPaperRun,
  useStopStrategyPaperRun,
  useStrategy,
  useStrategyRuns,
} from "@/lib/query-hooks";
import { formatCurrency, formatDateTime, formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

const LOOKBACK_OPTIONS = [180, 365, 720] as const;
const STRATEGY_TIMEFRAME = "1h";

type ConfigRow = {
  key: string;
  label: string;
  value: string;
};

type ExperimentDefinition = {
  code: string;
  symbol: string;
  shortLabel: string;
  title: string;
  eyebrow: string;
  stance: string;
  note: string;
  whyItExists: string;
  interpretation: string;
  tone: "default" | "positive" | "warning";
};

const EXPERIMENTS: ExperimentDefinition[] = [
  {
    code: "ondo_short_delta_fade_v7",
    symbol: "ONDO-USDT",
    shortLabel: "ONDO",
    title: "ONDO v7",
    eyebrow: "Strict anchor branch",
    stance: "Still the anchor thesis, but only if we keep it selective.",
    note: "This branch stays strict on rejection quality and next-bar weakness because ONDO only showed a tiny edge when we avoided noisy entries.",
    whyItExists: "We want to know whether ONDO still has a real short-fade edge once it is isolated from the weaker basket names.",
    interpretation: "If ONDO cannot improve even as a symbol-specific branch, we should stop treating it as the anchor for this thesis.",
    tone: "warning",
  },
  {
    code: "alpine_short_delta_fade_v7",
    symbol: "ALPINE-USDT",
    shortLabel: "ALPINE",
    title: "ALPINE v7",
    eyebrow: "Looser survivor branch",
    stance: "The cleanest non-ONDO survivor, but still too sparse.",
    note: "This branch is slightly more permissive so we can test whether ALPINE's cleaner stability can survive a little more signal density.",
    whyItExists: "ALPINE survived the narrowed basket without degrading, so it deserves its own symbol-specific pass rather than being bundled with ONDO.",
    interpretation: "If ALPINE stays clean while adding a little density, it may become the more promising short watchlist than ONDO.",
    tone: "positive",
  },
];

export default function ShortFadeLabPage() {
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Symbol-Specific v7"
        title="Short Fade Lab"
        description="We split the old shared short-fade round into two independent branches: one strict `ONDO` experiment and one slightly more permissive `ALPINE` experiment. Each now has its own replay history and paper lane."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Active branches" value="2" hint="ONDO v7 and ALPINE v7" tone="positive" />
        <MetricCard label="Timeframe" value="1h only" hint="No more mixed timeframe noise in this round" tone="default" />
        <MetricCard label="Paper lanes" value="2" hint="One independent paper run per symbol-specific strategy" tone="warning" />
        <MetricCard label="Research stance" value="Watchlist" hint="We are testing signal quality, not promoting these yet" tone="default" />
      </section>

      <SectionCard title="Why We Split The Lab" eyebrow="This is the right kind of narrowing">
        <div className="grid gap-4 md:grid-cols-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">What changed</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              `v6` told us the broad thesis was gone. Only `ONDO` and `ALPINE` stayed alive, so `v7` stops pretending they should share one generic branch.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">What we removed</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              `GALA`, `IOTA`, `AXS`, `FIL`, and `ONDO 15m` stay out. They already told us enough, and keeping them in the lab would only add noise.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">What we are checking</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              We now care about one thing: whether either symbol-specific branch can produce a cleaner edge once its own rules are allowed to diverge.
            </p>
          </div>
        </div>
      </SectionCard>

      {EXPERIMENTS.map((experiment) => (
        <ExperimentPanel key={experiment.code} experiment={experiment} />
      ))}
    </div>
  );
}

function ExperimentPanel({ experiment }: { experiment: ExperimentDefinition }) {
  const strategyQuery = useStrategy(experiment.code, true);
  const backtestsQuery = useBacktests({ strategyCode: experiment.code, limit: 100 }, true);
  const paperRunsQuery = useStrategyRuns({ strategyCode: experiment.code, mode: "paper", limit: 100 }, true);
  const runBacktestMutation = useRunBacktest();
  const startPaperMutation = useStartStrategyPaperRun();
  const stopPaperMutation = useStopStrategyPaperRun();
  const [activeLookback, setActiveLookback] = useState<number | null>(null);
  const [paperBusy, setPaperBusy] = useState(false);

  const isLoading =
    (strategyQuery.isLoading && !strategyQuery.data) ||
    (backtestsQuery.isLoading && !backtestsQuery.data) ||
    (paperRunsQuery.isLoading && !paperRunsQuery.data);
  const error = strategyQuery.error ?? backtestsQuery.error ?? paperRunsQuery.error;

  if (isLoading) {
    return (
      <SectionCard title={experiment.title} eyebrow={experiment.eyebrow}>
        <LoadingState label={`Loading ${experiment.shortLabel} v7...`} />
      </SectionCard>
    );
  }

  if (error || !strategyQuery.data) {
    return (
      <SectionCard title={experiment.title} eyebrow={experiment.eyebrow}>
        <ErrorState message={getErrorMessage(error, `Unable to load ${experiment.shortLabel} v7.`)} />
      </SectionCard>
    );
  }

  const strategy = strategyQuery.data;
  const backtests = backtestsQuery.data ?? [];
  const paperRuns = paperRunsQuery.data ?? [];
  const activePaperRun = paperRuns.find((row) => row.status === "running" || row.status === "created") ?? null;
  const latestBacktest = backtests[0] ?? null;
  const configRows = buildConfigRows(strategy.effective_config);

  async function handleRunBacktest(lookbackDays: number) {
    setActiveLookback(lookbackDays);
    const endAt = new Date();
    const startAt = new Date(endAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
    try {
      await runBacktestMutation.mutateAsync({
        strategy_code: experiment.code,
        symbol: experiment.symbol,
        timeframe: STRATEGY_TIMEFRAME,
        start_at: startAt.toISOString(),
        end_at: endAt.toISOString(),
        exchange_code: "binance_us",
        initial_capital: 10000,
        fee: 0.001,
        slippage: 0.0005,
        position_size_pct: 0.1,
        strategy_config_override: {
          symbols: [experiment.symbol],
          timeframes: [STRATEGY_TIMEFRAME],
        },
      });
    } finally {
      setActiveLookback(null);
    }
  }

  async function handleStartPaper() {
    setPaperBusy(true);
    try {
      await startPaperMutation.mutateAsync({
        strategyCode: experiment.code,
        payload: {
          symbols: [experiment.symbol],
          timeframes: [STRATEGY_TIMEFRAME],
          exchange_code: "binance_us",
          start_from_latest: true,
          initial_balance: 10000,
          currency: "USD",
          fee: 0.001,
          slippage: 0.0005,
          strategy_config_override: {
            symbols: [experiment.symbol],
            timeframes: [STRATEGY_TIMEFRAME],
          },
          metadata: {
            launched_from: "short_fade_lab_v7_page",
            strategy_code: experiment.code,
            selected_symbol: experiment.symbol,
            selected_timeframe: STRATEGY_TIMEFRAME,
          },
        },
      });
    } finally {
      setPaperBusy(false);
    }
  }

  async function handleStopPaper() {
    setPaperBusy(true);
    try {
      await stopPaperMutation.mutateAsync({
        strategyCode: experiment.code,
        payload: {
          reason: "manual_stop_from_short_fade_lab_v7_page",
        },
      });
    } finally {
      setPaperBusy(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <SectionCard title={experiment.title} eyebrow={experiment.eyebrow}>
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div className="grid gap-4">
            <p className="text-lg font-medium text-white">{experiment.stance}</p>
            <p className="text-sm leading-6 text-slate-300">{experiment.note}</p>
            <div className="grid gap-4 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Why it exists</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">{experiment.whyItExists}</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Interpretation</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">{experiment.interpretation}</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Current stream</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">
                  {experiment.symbol} · {STRATEGY_TIMEFRAME}
                </p>
                <p className="mt-3 text-xs leading-5 text-slate-400">{strategy.description}</p>
              </div>
            </div>
          </div>

          <div className="grid gap-4">
            <MetricCard label="Strategy status" value={<StatusBadge status={strategy.active_paper_status ?? "experimental"} />} hint={strategy.name} tone={experiment.tone} />
            <MetricCard
              label="Latest replay"
              value={latestBacktest ? formatPercent(latestBacktest.total_return_pct) : "No run yet"}
              hint={
                latestBacktest
                  ? `DD ${formatPercent(latestBacktest.max_drawdown_pct)} · ${formatInteger(latestBacktest.total_trades)} trades`
                  : `No ${experiment.shortLabel} v7 replay yet`
              }
              tone={latestBacktest && Number(latestBacktest.total_return_pct) > 0 ? "positive" : "default"}
            />
            <MetricCard
              label="Active paper"
              value={activePaperRun ? `#${activePaperRun.id}` : "None"}
              hint={activePaperRun ? formatDateTime(activePaperRun.started_at) : "Independent lane for this strategy only"}
              tone={activePaperRun ? "warning" : "default"}
            />
          </div>
        </div>
      </SectionCard>

      {runBacktestMutation.error ? <ErrorState message={getErrorMessage(runBacktestMutation.error, `Unable to start ${experiment.shortLabel} v7 backtest.`)} /> : null}
      {startPaperMutation.error ? <ErrorState message={getErrorMessage(startPaperMutation.error, `Unable to start ${experiment.shortLabel} v7 paper run.`)} /> : null}
      {stopPaperMutation.error ? <ErrorState message={getErrorMessage(stopPaperMutation.error, `Unable to stop ${experiment.shortLabel} v7 paper run.`)} /> : null}

      <SectionCard
        title="Config Snapshot"
        eyebrow={`${experiment.shortLabel} specific defaults`}
        actions={
          <div className="flex flex-wrap gap-2">
            {LOOKBACK_OPTIONS.map((lookback) => (
              <button
                key={lookback}
                type="button"
                onClick={() => handleRunBacktest(lookback)}
                disabled={runBacktestMutation.isPending}
                className="rounded-xl border border-emerald-400/25 bg-emerald-400/10 px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-emerald-100 transition hover:border-emerald-300/40 hover:bg-emerald-300/15 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {activeLookback === lookback && runBacktestMutation.isPending ? `Starting ${lookback}d...` : `Run ${lookback}d`}
              </button>
            ))}
          </div>
        }
      >
        <DataTable
          rows={configRows}
          rowKey={(row) => row.key}
          emptyTitle="No config found"
          emptyDescription="The backend strategy detail did not return an effective configuration."
          columns={[
            { key: "label", title: "Parameter", render: (row) => <span className="font-medium text-white">{row.label}</span> },
            { key: "value", title: "Value", render: (row) => <span className="text-slate-200">{row.value}</span> },
          ]}
        />
      </SectionCard>

      <SectionCard
        title="Paper Control"
        eyebrow={`${experiment.shortLabel} forward lane`}
        actions={
          activePaperRun ? (
            <button
              type="button"
              onClick={handleStopPaper}
              disabled={paperBusy || startPaperMutation.isPending || stopPaperMutation.isPending}
              className="rounded-xl border border-rose-400/25 bg-rose-400/10 px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-rose-100 transition hover:border-rose-300/40 hover:bg-rose-300/15 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {paperBusy ? "Stopping..." : "Stop paper run"}
            </button>
          ) : (
            <button
              type="button"
              onClick={handleStartPaper}
              disabled={paperBusy || startPaperMutation.isPending || stopPaperMutation.isPending}
              className="rounded-xl border border-sky-400/25 bg-sky-400/10 px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-sky-100 transition hover:border-sky-300/40 hover:bg-sky-300/15 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {paperBusy ? "Starting..." : "Start paper"}
            </button>
          )
        }
      >
        <div className="grid gap-4 md:grid-cols-3">
          <MetricCard
            label="Paper balance"
            value={activePaperRun ? formatCurrency(activePaperRun.account_balance ?? 0, activePaperRun.currency ?? "USD") : "$10,000.00"}
            hint={activePaperRun ? `${formatInteger(activePaperRun.open_positions_count)} open positions` : "Will initialize on start"}
            tone="default"
          />
          <MetricCard
            label="Last processed"
            value={activePaperRun?.last_processed_candle_at ? formatDateTime(activePaperRun.last_processed_candle_at) : "N/A"}
            hint="Paper starts latest-only on this single stream"
            tone="default"
          />
          <MetricCard
            label="Saved paper history"
            value={formatInteger(paperRuns.length)}
            hint="Only this symbol-specific branch"
            tone="default"
          />
        </div>
      </SectionCard>

      <SectionCard title="Recent Backtests" eyebrow={`Stored replay history for ${experiment.shortLabel} v7`}>
        <DataTable
          rows={backtests}
          rowKey={(row) => row.id}
          emptyTitle={`No ${experiment.shortLabel} v7 backtests yet`}
          emptyDescription="Run one of the windows above to start building the symbol-specific replay history."
          columns={[
            { key: "run", title: "Run", render: (row) => `#${row.id}` },
            { key: "status", title: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "market", title: "Window", render: (row) => `${row.symbol} · ${row.timeframe}` },
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
            { key: "trades", title: "Trades", render: (row) => formatInteger(row.total_trades) },
          ]}
        />
      </SectionCard>

      <SectionCard title="Recent Paper Runs" eyebrow={`Stored forward history for ${experiment.shortLabel} v7`}>
        <DataTable
          rows={paperRuns}
          rowKey={(row) => row.id}
          emptyTitle={`No ${experiment.shortLabel} v7 paper runs yet`}
          emptyDescription="Start a paper run once the replay history looks promising enough to watch live."
          columns={[
            { key: "run", title: "Run", render: (row) => `#${row.id}` },
            { key: "status", title: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "market", title: "Market", render: (row) => `${row.symbols.join(", ")} · ${row.timeframes.join(", ")}` },
            { key: "started", title: "Started", render: (row) => formatDateTime(row.started_at) },
            {
              key: "balance",
              title: "Balance / Processed",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatCurrency(row.account_balance ?? 0, row.currency ?? "USD")}</span>
                  <span className="text-xs text-slate-400">{row.last_processed_candle_at ? formatDateTime(row.last_processed_candle_at) : "No candles yet"}</span>
                </div>
              ),
            },
            { key: "positions", title: "Open", render: (row) => formatInteger(row.open_positions_count) },
          ]}
        />
      </SectionCard>
    </div>
  );
}

function buildConfigRows(config: Record<string, unknown>): ConfigRow[] {
  const rows: Array<[string, string]> = [
    ["symbols", "Default symbols"],
    ["timeframes", "Default timeframes"],
    ["impulse_bars", "Impulse bars"],
    ["impulse_min_return_pct", "Min 3-bar return"],
    ["breakout_lookback_bars", "Breakout lookback"],
    ["breakout_proximity_pct", "Breakout proximity"],
    ["ema_period", "EMA period"],
    ["stretch_above_ema_pct", "Stretch above EMA"],
    ["volume_sma_period", "Volume SMA"],
    ["volume_spike_mult", "Volume spike multiplier"],
    ["rejection_close_location_max", "Rejection close location max"],
    ["upper_wick_min_range_ratio", "Upper wick min ratio"],
    ["entry_breakdown_pct", "Entry breakdown"],
    ["entry_followthrough_close_location_max", "Entry follow-through max"],
    ["stop_buffer_pct", "Stop buffer"],
    ["max_stop_distance_pct", "Max stop distance"],
    ["take_profit_pct", "Take profit"],
    ["stop_loss_pct", "Stop loss"],
    ["time_exit_bars", "Time exit bars"],
    ["max_gap_up_pct", "Max gap up"],
    ["position_size_pct", "Position size"],
  ];

  return rows
    .filter(([key]) => key in config)
    .map(([key, label]) => {
      const raw = config[key];
      let value = "";
      if (Array.isArray(raw)) {
        value = raw.join(", ");
      } else if (typeof raw === "number") {
        value = String(raw);
      } else {
        value = String(raw);
      }
      return { key, label, value };
    });
}
