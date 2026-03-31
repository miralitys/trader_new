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
const ACTIVE_BRANCH = {
  code: "alpine_short_delta_fade_v8",
  symbol: "ALPINE-USDT",
  shortLabel: "ALPINE",
};
const WATCH_BRANCH = {
  code: "ondo_short_delta_fade_v7",
  symbol: "ONDO-USDT",
  shortLabel: "ONDO",
};

type ConfigRow = {
  key: string;
  label: string;
  value: string;
};

export default function ShortFadeLabPage() {
  const alpineStrategyQuery = useStrategy(ACTIVE_BRANCH.code, true);
  const alpineBacktestsQuery = useBacktests({ strategyCode: ACTIVE_BRANCH.code, limit: 100 }, true);
  const alpinePaperRunsQuery = useStrategyRuns({ strategyCode: ACTIVE_BRANCH.code, mode: "paper", limit: 100 }, true);

  const ondoStrategyQuery = useStrategy(WATCH_BRANCH.code, true);
  const ondoBacktestsQuery = useBacktests({ strategyCode: WATCH_BRANCH.code, limit: 100 }, true);
  const ondoPaperRunsQuery = useStrategyRuns({ strategyCode: WATCH_BRANCH.code, mode: "paper", limit: 100 }, true);

  const isLoading =
    (alpineStrategyQuery.isLoading && !alpineStrategyQuery.data) ||
    (alpineBacktestsQuery.isLoading && !alpineBacktestsQuery.data) ||
    (alpinePaperRunsQuery.isLoading && !alpinePaperRunsQuery.data) ||
    (ondoStrategyQuery.isLoading && !ondoStrategyQuery.data) ||
    (ondoBacktestsQuery.isLoading && !ondoBacktestsQuery.data) ||
    (ondoPaperRunsQuery.isLoading && !ondoPaperRunsQuery.data);

  const error =
    alpineStrategyQuery.error ??
    alpineBacktestsQuery.error ??
    alpinePaperRunsQuery.error ??
    ondoStrategyQuery.error ??
    ondoBacktestsQuery.error ??
    ondoPaperRunsQuery.error;

  if (isLoading) {
    return <LoadingState label="Loading short fade lab..." />;
  }

  if (error || !alpineStrategyQuery.data || !ondoStrategyQuery.data) {
    return <ErrorState message={getErrorMessage(error, "Unable to load the short fade lab.")} />;
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="ALPINE First"
        title="Short Fade Lab"
        description="We made the call: `ALPINE` is now the only active short-fade branch in `v8`. `ONDO` stays visible as a secondary watch, but it is frozen until `ALPINE` proves it deserves more tuning effort."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Primary branch" value="ALPINE v8" hint="Only active tuning branch" tone="positive" />
        <MetricCard label="Secondary watch" value="ONDO v7" hint="Read-only reference branch" tone="warning" />
        <MetricCard label="Timeframe" value="1h only" hint="No lower-timeframe detours in this round" tone="default" />
        <MetricCard label="Research stance" value="Narrowed again" hint="One active branch, one frozen benchmark" tone="default" />
      </section>

      <SectionCard title="Why We Narrowed Again" eyebrow="This is the healthy kind of pruning">
        <div className="grid gap-4 md:grid-cols-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Decision</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              `ALPINE` becomes the only active branch because it improved the most after the symbol-specific split, while `ONDO` stayed sparse and nearly unchanged.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">What frozen means</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              `ONDO` is still visible so we do not lose the benchmark, but we stop spending tuning cycles on it unless `ALPINE` fails and forces us back.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">What success looks like</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              `ALPINE v8` should either add a bit more density without degrading, or clearly prove this short-thesis is too small to keep pushing.
            </p>
          </div>
        </div>
      </SectionCard>

      <ActiveBranchPanel
        strategyCode={ACTIVE_BRANCH.code}
        symbol={ACTIVE_BRANCH.symbol}
        shortLabel={ACTIVE_BRANCH.shortLabel}
      />

      <FrozenWatchPanel
        strategyCode={WATCH_BRANCH.code}
        symbol={WATCH_BRANCH.symbol}
        shortLabel={WATCH_BRANCH.shortLabel}
      />
    </div>
  );
}

function ActiveBranchPanel({
  strategyCode,
  symbol,
  shortLabel,
}: {
  strategyCode: string;
  symbol: string;
  shortLabel: string;
}) {
  const strategyQuery = useStrategy(strategyCode, true);
  const backtestsQuery = useBacktests({ strategyCode, limit: 100 }, true);
  const paperRunsQuery = useStrategyRuns({ strategyCode, mode: "paper", limit: 100 }, true);
  const runBacktestMutation = useRunBacktest();
  const startPaperMutation = useStartStrategyPaperRun();
  const stopPaperMutation = useStopStrategyPaperRun();
  const [activeLookback, setActiveLookback] = useState<number | null>(null);
  const [paperBusy, setPaperBusy] = useState(false);

  if (!strategyQuery.data) {
    return null;
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
        strategy_code: strategyCode,
        symbol,
        timeframe: STRATEGY_TIMEFRAME,
        start_at: startAt.toISOString(),
        end_at: endAt.toISOString(),
        exchange_code: "binance_us",
        initial_capital: 10000,
        fee: 0.001,
        slippage: 0.0005,
        position_size_pct: 0.1,
        strategy_config_override: {
          symbols: [symbol],
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
        strategyCode,
        payload: {
          symbols: [symbol],
          timeframes: [STRATEGY_TIMEFRAME],
          exchange_code: "binance_us",
          start_from_latest: true,
          initial_balance: 10000,
          currency: "USD",
          fee: 0.001,
          slippage: 0.0005,
          strategy_config_override: {
            symbols: [symbol],
            timeframes: [STRATEGY_TIMEFRAME],
          },
          metadata: {
            launched_from: "short_fade_lab_v8_page",
            strategy_code: strategyCode,
            selected_symbol: symbol,
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
        strategyCode,
        payload: {
          reason: "manual_stop_from_short_fade_lab_v8_page",
        },
      });
    } finally {
      setPaperBusy(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <SectionCard title="ALPINE v8" eyebrow="Primary active branch">
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div className="grid gap-4">
            <p className="text-lg font-medium text-white">
              `ALPINE` is the only branch still earning active tuning. `v8` is our deliberate attempt to get a little more signal density without wrecking the cleaner long-window profile we saw in `v7`.
            </p>
            <p className="text-sm leading-6 text-slate-300">
              This is the branch we should either promote into a real watch candidate or use as the final proof that the short-fade thesis stays too small to matter.
            </p>
            <div className="grid gap-4 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Hypothesis</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">
                  A slightly looser `ALPINE` branch may produce one extra layer of short-fade density while staying cleaner than `ONDO`.
                </p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Symbol</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">
                  {symbol} · {STRATEGY_TIMEFRAME}
                </p>
                <p className="mt-2 text-xs leading-5 text-slate-400">{strategy.description}</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Latest replay</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">
                  {latestBacktest ? formatPercent(latestBacktest.total_return_pct) : "No run yet"}
                </p>
                <p className="mt-2 text-xs leading-5 text-slate-400">
                  {latestBacktest
                    ? `DD ${formatPercent(latestBacktest.max_drawdown_pct)} · ${formatInteger(latestBacktest.total_trades)} trades`
                    : "Run 180d, 365d, and 720d to build the first v8 baseline."}
                </p>
              </div>
            </div>
          </div>

          <div className="grid gap-4">
            <MetricCard label="Branch status" value={<StatusBadge status={strategy.active_paper_status ?? "experimental"} />} hint={strategy.name} tone="positive" />
            <MetricCard
              label="Active paper"
              value={activePaperRun ? `#${activePaperRun.id}` : "None"}
              hint={activePaperRun ? formatDateTime(activePaperRun.started_at) : "Independent ALPINE-only lane"}
              tone={activePaperRun ? "warning" : "default"}
            />
            <MetricCard
              label="Stored replays"
              value={formatInteger(backtests.length)}
              hint="This branch no longer shares history with ONDO"
              tone="default"
            />
          </div>
        </div>
      </SectionCard>

      {runBacktestMutation.error ? <ErrorState message={getErrorMessage(runBacktestMutation.error, "Unable to start ALPINE v8 backtest.")} /> : null}
      {startPaperMutation.error ? <ErrorState message={getErrorMessage(startPaperMutation.error, "Unable to start ALPINE v8 paper run.")} /> : null}
      {stopPaperMutation.error ? <ErrorState message={getErrorMessage(stopPaperMutation.error, "Unable to stop ALPINE v8 paper run.")} /> : null}

      <SectionCard
        title="Config Snapshot"
        eyebrow="ALPINE v8 defaults"
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
        eyebrow="ALPINE v8 forward lane"
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
            hint="Latest-only start on ALPINE 1h"
            tone="default"
          />
          <MetricCard label="Saved paper runs" value={formatInteger(paperRuns.length)} hint="Only ALPINE v8 history" tone="default" />
        </div>
      </SectionCard>

      <SectionCard title="Recent Backtests" eyebrow="Stored replay history for ALPINE v8">
        <DataTable
          rows={backtests}
          rowKey={(row) => row.id}
          emptyTitle="No ALPINE v8 backtests yet"
          emptyDescription="Run one of the windows above to start building the ALPINE v8 baseline."
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
    </div>
  );
}

function FrozenWatchPanel({
  strategyCode,
  symbol,
  shortLabel,
}: {
  strategyCode: string;
  symbol: string;
  shortLabel: string;
}) {
  const strategyQuery = useStrategy(strategyCode, true);
  const backtestsQuery = useBacktests({ strategyCode, limit: 100 }, true);
  const paperRunsQuery = useStrategyRuns({ strategyCode, mode: "paper", limit: 100 }, true);

  if (!strategyQuery.data) {
    return null;
  }

  const strategy = strategyQuery.data;
  const backtests = backtestsQuery.data ?? [];
  const paperRuns = paperRunsQuery.data ?? [];
  const latestBacktest = backtests[0] ?? null;
  const configRows = buildConfigRows(strategy.effective_config);

  return (
    <div className="flex flex-col gap-6">
      <SectionCard title="ONDO v7" eyebrow="Frozen secondary watch">
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div className="grid gap-4">
            <p className="text-lg font-medium text-white">
              `ONDO` stays here as the benchmark branch we are deliberately not tuning anymore unless `ALPINE` collapses and forces us back.
            </p>
            <p className="text-sm leading-6 text-slate-300">
              This is intentionally read-only now. We keep the history visible so we do not lose context, but we stop spending fresh tuning effort on a branch that stayed too sparse.
            </p>
            <div className="grid gap-4 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Reason frozen</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">
                  `ONDO` never improved enough after the split. It stayed alive, but too sparse and too small to justify more active tuning.
                </p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Current stream</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">
                  {symbol} · {STRATEGY_TIMEFRAME}
                </p>
                <p className="mt-2 text-xs leading-5 text-slate-400">{strategy.description}</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Latest replay</p>
                <p className="mt-3 text-sm leading-6 text-slate-200">
                  {latestBacktest ? formatPercent(latestBacktest.total_return_pct) : "No run yet"}
                </p>
                <p className="mt-2 text-xs leading-5 text-slate-400">
                  {latestBacktest
                    ? `DD ${formatPercent(latestBacktest.max_drawdown_pct)} · ${formatInteger(latestBacktest.total_trades)} trades`
                    : "No ONDO v7 history found."}
                </p>
              </div>
            </div>
          </div>

          <div className="grid gap-4">
            <MetricCard label="Branch status" value="Frozen" hint={shortLabel + " stays as secondary watch"} tone="warning" />
            <MetricCard label="Stored replays" value={formatInteger(backtests.length)} hint="Preserved benchmark history" tone="default" />
            <MetricCard label="Paper runs" value={formatInteger(paperRuns.length)} hint="No new paper runs should be started from here" tone="default" />
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Frozen Config Snapshot" eyebrow="Reference only">
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

      <SectionCard title="Recent Backtests" eyebrow="Stored replay history for ONDO v7">
        <DataTable
          rows={backtests}
          rowKey={(row) => row.id}
          emptyTitle="No ONDO v7 backtests yet"
          emptyDescription="This frozen branch keeps history only as reference."
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
      const value = Array.isArray(raw) ? raw.join(", ") : String(raw);
      return { key, label, value };
    });
}
