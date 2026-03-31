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

const STRATEGY_CODE = "ondo_short_delta_fade_v1";
const STRATEGY_SYMBOL = "ONDO-USDT";
const STRATEGY_TIMEFRAME = "1h";
const LOOKBACK_OPTIONS = [180, 365, 720] as const;

type ConfigRow = {
  key: string;
  label: string;
  value: string;
};

export default function OndoShortDeltaPage() {
  const strategyQuery = useStrategy(STRATEGY_CODE, true);
  const backtestsQuery = useBacktests({ strategyCode: STRATEGY_CODE, limit: 100 }, true);
  const paperRunsQuery = useStrategyRuns({ strategyCode: STRATEGY_CODE, mode: "paper", limit: 100 }, true);
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
    return <LoadingState label="Loading ONDO short delta fade..." />;
  }

  if (error || !strategyQuery.data) {
    return <ErrorState message={getErrorMessage(error, "Unable to load the ONDO short strategy page.")} />;
  }

  const strategy = strategyQuery.data;
  const backtests = backtestsQuery.data ?? [];
  const paperRuns = paperRunsQuery.data ?? [];
  const latestBacktest = backtests[0] ?? null;
  const activePaperRun = paperRuns.find((row) => row.status === "running" || row.status === "created") ?? null;
  const configRows = buildConfigRows(strategy.effective_config);

  async function handleRunBacktest(lookbackDays: number) {
    setActiveLookback(lookbackDays);
    const endAt = new Date();
    const startAt = new Date(endAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
    try {
      await runBacktestMutation.mutateAsync({
        strategy_code: STRATEGY_CODE,
        symbol: STRATEGY_SYMBOL,
        timeframe: STRATEGY_TIMEFRAME,
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
      setActiveLookback(null);
    }
  }

  async function handleStartPaper() {
    setPaperBusy(true);
    try {
      await startPaperMutation.mutateAsync({
        strategyCode: STRATEGY_CODE,
        payload: {
          symbols: [STRATEGY_SYMBOL],
          timeframes: [STRATEGY_TIMEFRAME],
          exchange_code: "binance_us",
          start_from_latest: true,
          initial_balance: 10000,
          currency: "USD",
          fee: 0.001,
          slippage: 0.0005,
          strategy_config_override: {},
          metadata: {
            launched_from: "ondo_short_delta_page",
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
        strategyCode: STRATEGY_CODE,
        payload: {
          reason: "manual_stop_from_ondo_short_delta_page",
        },
      });
    } finally {
      setPaperBusy(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Experimental Short Strategy"
        title="ONDO Delta Fade"
        description="A dedicated research section for the ONDO short-only fade proxy we reconstructed from the external screenshots: upside overextension, rejection, and a fast mean-reversion exit package."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Strategy status" value={<StatusBadge status={strategy.active_paper_status ?? "experimental"} />} hint={strategy.name} tone="warning" />
        <MetricCard label="Market" value={`${STRATEGY_SYMBOL} · ${STRATEGY_TIMEFRAME}`} hint="Single-symbol short proxy" tone="default" />
        <MetricCard label="Direction" value={<StatusBadge status={strategy.primary_side} />} hint={strategy.spot_only ? "Spot-only" : "Simulated short / non-spot research"} tone="danger" />
        <MetricCard
          label="Latest replay"
          value={latestBacktest ? formatPercent(latestBacktest.total_return_pct) : "No run yet"}
          hint={
            latestBacktest
              ? `DD ${formatPercent(latestBacktest.max_drawdown_pct)} · ${formatInteger(latestBacktest.total_trades)} trades`
              : "Run one of the three windows below"
          }
          tone={latestBacktest && Number(latestBacktest.total_return_pct) > 0 ? "positive" : "default"}
        />
      </section>

      {runBacktestMutation.error ? <ErrorState message={getErrorMessage(runBacktestMutation.error, "Unable to start ONDO backtest.")} /> : null}
      {startPaperMutation.error ? <ErrorState message={getErrorMessage(startPaperMutation.error, "Unable to start ONDO paper run.")} /> : null}
      {stopPaperMutation.error ? <ErrorState message={getErrorMessage(stopPaperMutation.error, "Unable to stop ONDO paper run.")} /> : null}

      <SectionCard title="Strategy Thesis" eyebrow="Reconstructed from observed behavior">
        <div className="grid gap-4 md:grid-cols-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Setup</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              Detect a fast upside overextension, then require a failed breakout and a rejection candle before fading the move with a short.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Risk Package</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              Tight stop above the rejection bar, small take-profit, and a forced time exit after a very short holding window.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Intent</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              This is a candle-based proxy for the external `Delta-1` profile, not a literal order-flow clone. We are testing whether the behavior survives without proprietary delta data.
            </p>
          </div>
        </div>
      </SectionCard>

      <SectionCard
        title="Config Snapshot"
        eyebrow="Default backend parameters"
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
        eyebrow="Latest-only forward observation"
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
              {paperBusy ? "Starting..." : "Start latest-only paper"}
            </button>
          )
        }
      >
        <div className="grid gap-4 md:grid-cols-3">
          <MetricCard
            label="Active paper run"
            value={activePaperRun ? `#${activePaperRun.id}` : "None"}
            hint={activePaperRun ? activePaperRun.status : "No live forward run"}
            tone={activePaperRun ? "warning" : "default"}
          />
          <MetricCard
            label="Paper balance"
            value={activePaperRun ? formatCurrency(activePaperRun.account_balance ?? 0, activePaperRun.currency ?? "USD") : "$10,000.00"}
            hint={activePaperRun ? `${formatInteger(activePaperRun.open_positions_count)} open positions` : "Will initialize on start"}
            tone="default"
          />
          <MetricCard
            label="Last processed"
            value={activePaperRun?.last_processed_candle_at ? formatDateTime(activePaperRun.last_processed_candle_at) : "N/A"}
            hint="Latest-only paper starts from the freshest nightly candle"
            tone="default"
          />
        </div>
      </SectionCard>

      <SectionCard title="Recent Backtests" eyebrow="Stored replay history for this strategy">
        <DataTable
          rows={backtests}
          rowKey={(row) => row.id}
          emptyTitle="No ONDO backtests yet"
          emptyDescription="Run one of the replay windows above to start building the baseline history for this short proxy."
          columns={[
            { key: "run", title: "Run", render: (row) => `#${row.id}` },
            { key: "status", title: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "window", title: "Window", render: (row) => `${row.symbol} · ${row.timeframe}` },
            { key: "started", title: "Started", render: (row) => formatDateTime(row.started_at) },
            {
              key: "result",
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

      <SectionCard title="Recent Paper Runs" eyebrow="Stored forward-paper history for this strategy">
        <DataTable
          rows={paperRuns}
          rowKey={(row) => row.id}
          emptyTitle="No ONDO paper runs yet"
          emptyDescription="Once we start paper-testing the short proxy, the run history will appear here."
          columns={[
            { key: "run", title: "Run", render: (row) => `#${row.id}` },
            { key: "status", title: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "started", title: "Started", render: (row) => formatDateTime(row.started_at ?? row.created_at) },
            {
              key: "account",
              title: "Account",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatCurrency(row.account_balance ?? 0, row.currency ?? "USD")}</span>
                  <span className="text-xs text-slate-400">{formatInteger(row.open_positions_count)} open positions</span>
                </div>
              ),
            },
            {
              key: "progress",
              title: "Progress",
              render: (row) => (
                <div className="grid gap-1 text-xs text-slate-300">
                  <span>{formatInteger(extractProcessedCandles(row.metadata))} candles in metadata</span>
                  <span className="text-slate-400">{row.last_processed_candle_at ? formatDateTime(row.last_processed_candle_at) : "No watermark yet"}</span>
                </div>
              ),
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}

function buildConfigRows(config: Record<string, unknown>): ConfigRow[] {
  const keys = [
    ["impulse_bars", "Impulse Bars"],
    ["impulse_min_return_pct", "Min 3-Bar Return"],
    ["breakout_lookback_bars", "Breakout Lookback"],
    ["ema_period", "EMA Period"],
    ["stretch_above_ema_pct", "Stretch Above EMA"],
    ["volume_sma_period", "Volume SMA"],
    ["volume_spike_mult", "Volume Spike Multiplier"],
    ["rejection_close_location_max", "Rejection Close Location Max"],
    ["upper_wick_min_range_ratio", "Upper Wick Min Ratio"],
    ["take_profit_pct", "Take Profit"],
    ["stop_loss_pct", "Stop Loss"],
    ["time_exit_bars", "Time Exit Bars"],
    ["max_gap_up_pct", "Max Gap Up"],
    ["position_size_pct", "Position Size"],
  ] as const;

  return keys.map(([key, label]) => ({
    key,
    label,
    value: formatConfigValue(config[key]),
  }));
}

function formatConfigValue(value: unknown) {
  if (typeof value === "number") {
    return String(value);
  }
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.join(", ");
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (value === null || value === undefined) {
    return "N/A";
  }
  return JSON.stringify(value);
}

function extractProcessedCandles(metadata: Record<string, unknown>) {
  const candidate = metadata.processed_candles;
  return typeof candidate === "number" || typeof candidate === "string" ? candidate : 0;
}
