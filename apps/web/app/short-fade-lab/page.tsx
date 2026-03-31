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

const STRATEGY_CODE = "short_delta_fade_lab_v5";
const LOOKBACK_OPTIONS = [180, 365, 720] as const;
const SYMBOL_OPTIONS = ["ONDO-USDT", "ALPINE-USDT", "GALA-USDT"] as const;
const STRATEGY_TIMEFRAME = "1h";

type ConfigRow = {
  key: string;
  label: string;
  value: string;
};

export default function ShortFadeLabPage() {
  const strategyQuery = useStrategy(STRATEGY_CODE, true);
  const backtestsQuery = useBacktests({ strategyCode: STRATEGY_CODE, limit: 100 }, true);
  const paperRunsQuery = useStrategyRuns({ strategyCode: STRATEGY_CODE, mode: "paper", limit: 100 }, true);
  const runBacktestMutation = useRunBacktest();
  const startPaperMutation = useStartStrategyPaperRun();
  const stopPaperMutation = useStopStrategyPaperRun();
  const [selectedSymbol, setSelectedSymbol] = useState<string>("ONDO-USDT");
  const [activeLookback, setActiveLookback] = useState<number | null>(null);
  const [paperBusy, setPaperBusy] = useState(false);

  const isLoading =
    (strategyQuery.isLoading && !strategyQuery.data) ||
    (backtestsQuery.isLoading && !backtestsQuery.data) ||
    (paperRunsQuery.isLoading && !paperRunsQuery.data);
  const error = strategyQuery.error ?? backtestsQuery.error ?? paperRunsQuery.error;

  if (isLoading) {
    return <LoadingState label="Loading short fade lab..." />;
  }

  if (error || !strategyQuery.data) {
    return <ErrorState message={getErrorMessage(error, "Unable to load the short fade lab.")} />;
  }

  const strategy = strategyQuery.data;
  const backtests = backtestsQuery.data ?? [];
  const paperRuns = paperRunsQuery.data ?? [];
  const activePaperRun = paperRuns.find((row) => row.status === "running" || row.status === "created") ?? null;
  const configRows = buildConfigRows(strategy.effective_config);

  const selectedBacktests = backtests.filter((row) => row.symbol === selectedSymbol && row.timeframe === STRATEGY_TIMEFRAME);
  const latestSelectedBacktest = selectedBacktests[0] ?? null;

  async function handleRunBacktest(lookbackDays: number) {
    setActiveLookback(lookbackDays);
    const endAt = new Date();
    const startAt = new Date(endAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
    try {
      await runBacktestMutation.mutateAsync({
        strategy_code: STRATEGY_CODE,
        symbol: selectedSymbol,
        timeframe: STRATEGY_TIMEFRAME,
        start_at: startAt.toISOString(),
        end_at: endAt.toISOString(),
        exchange_code: "binance_us",
        initial_capital: 10000,
        fee: 0.001,
        slippage: 0.0005,
        position_size_pct: 0.1,
        strategy_config_override: {
          symbols: [selectedSymbol],
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
        strategyCode: STRATEGY_CODE,
        payload: {
          symbols: [selectedSymbol],
          timeframes: [STRATEGY_TIMEFRAME],
          exchange_code: "binance_us",
          start_from_latest: true,
          initial_balance: 10000,
          currency: "USD",
          fee: 0.001,
          slippage: 0.0005,
          strategy_config_override: {
            symbols: [selectedSymbol],
            timeframes: [STRATEGY_TIMEFRAME],
          },
          metadata: {
            launched_from: "short_fade_lab_page",
            selected_symbol: selectedSymbol,
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
        strategyCode: STRATEGY_CODE,
        payload: {
          reason: "manual_stop_from_short_fade_lab_page",
        },
      });
    } finally {
      setPaperBusy(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Focused Short Round"
        title="Short Fade Lab"
        description="Clean v5 round for the only streams still worth our time: `ONDO`, `ALPINE`, and `GALA` on `1h`. The broader basket underperformed, and `ONDO 15m` stayed dead, so this page now tracks just the survivors."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Lab status" value={<StatusBadge status={strategy.active_paper_status ?? "experimental"} />} hint={strategy.name} tone="warning" />
        <MetricCard label="Selected market" value={`${selectedSymbol} · ${STRATEGY_TIMEFRAME}`} hint="Backtests and paper runs use this exact stream" tone="default" />
        <MetricCard
          label="Latest selected replay"
          value={latestSelectedBacktest ? formatPercent(latestSelectedBacktest.total_return_pct) : "No run yet"}
          hint={
            latestSelectedBacktest
              ? `DD ${formatPercent(latestSelectedBacktest.max_drawdown_pct)} · ${formatInteger(latestSelectedBacktest.total_trades)} trades`
              : "Pick a symbol and timeframe, then launch a replay"
          }
          tone={latestSelectedBacktest && Number(latestSelectedBacktest.total_return_pct) > 0 ? "positive" : "default"}
        />
        <MetricCard label="Coverage plan" value="3 symbols · 1h only" hint="Fresh v5 round with clean history" tone="positive" />
      </section>

      {runBacktestMutation.error ? <ErrorState message={getErrorMessage(runBacktestMutation.error, "Unable to start short-fade backtest.")} /> : null}
      {startPaperMutation.error ? <ErrorState message={getErrorMessage(startPaperMutation.error, "Unable to start short-fade paper run.")} /> : null}
      {stopPaperMutation.error ? <ErrorState message={getErrorMessage(stopPaperMutation.error, "Unable to stop short-fade paper run.")} /> : null}

      <SectionCard title="Why This Round Exists" eyebrow="Narrowed after the first basket test">
        <div className="grid gap-4 md:grid-cols-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Kept Alive</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              `ONDO`, `ALPINE`, and `GALA` were the only names that showed even a faint positive pulse or at least a plausible next step for this short-fade idea.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Removed</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              `IOTA`, `AXS`, and `FIL` looked structurally weak. `ONDO 15m` stayed at zero trades, so it does not belong in the active round anymore.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Interpretation</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              This round is about cleaner signal reading, not more breadth. If these three do not improve, we should probably stop tuning this thesis instead of widening it again.
            </p>
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Market Selector" eyebrow="Choose the exact stream for v5">
        <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(320px,360px)]">
          <div className="grid gap-5">
            <div className="grid gap-2">
              <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Symbol</p>
              <div className="flex flex-wrap gap-2">
                {SYMBOL_OPTIONS.map((symbol) => {
                  const active = symbol === selectedSymbol;
                  return (
                    <button
                      key={symbol}
                      type="button"
                      onClick={() => setSelectedSymbol(symbol)}
                      className={`rounded-xl border px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] transition ${
                        active
                          ? "border-emerald-300/50 bg-emerald-400/15 text-emerald-100"
                          : "border-white/10 bg-white/[0.03] text-slate-300 hover:border-white/20 hover:bg-white/[0.06]"
                      }`}
                    >
                      {symbol.replace("-USDT", "")}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>

          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Current selection</p>
            <div className="mt-3 grid gap-3">
              <div>
                <p className="text-lg font-medium text-white">{selectedSymbol}</p>
                <p className="text-sm text-slate-400">{STRATEGY_TIMEFRAME} short-fade stream</p>
              </div>
              <div className="text-sm leading-6 text-slate-300">
                {selectedSymbol === "ONDO-USDT"
                  ? "This is still the anchor stream. We are checking whether ONDO keeps its small but real edge when isolated from the failed broader round."
                  : selectedSymbol === "ALPINE-USDT"
                    ? "ALPINE is the cleanest non-ONDO survivor so far: tiny sample, but at least not degrading as history expands."
                    : "GALA is the most unstable survivor. It can still earn on the mid window, but it needs this focused round to prove it does not break on longer history."}
              </div>
            </div>
          </div>
        </div>
      </SectionCard>

      <SectionCard
        title="Config Snapshot"
        eyebrow="Shared backend parameters"
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
        eyebrow="One live stream at a time"
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
              {paperBusy ? "Starting..." : "Start paper for selection"}
            </button>
          )
        }
      >
        <div className="grid gap-4 md:grid-cols-3">
          <MetricCard
            label="Active paper run"
            value={activePaperRun ? `#${activePaperRun.id}` : "None"}
            hint={activePaperRun ? `${activePaperRun.symbols.join(", ")} · ${activePaperRun.timeframes.join(", ")}` : "No live forward run"}
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
            hint="Paper starts latest-only on the selected stream"
            tone="default"
          />
        </div>
      </SectionCard>

      <SectionCard title="Recent Backtests" eyebrow="Stored replay history for the lab">
        <DataTable
          rows={backtests}
          rowKey={(row) => row.id}
          emptyTitle="No short-fade lab backtests yet"
          emptyDescription="Run one of the windows above to start comparing the 1h basket and ONDO 15m."
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

      <SectionCard title="Recent Paper Runs" eyebrow="Stored forward-paper history for the lab">
        <DataTable
          rows={paperRuns}
          rowKey={(row) => row.id}
          emptyTitle="No short-fade paper runs yet"
          emptyDescription="Start a paper run for any selected symbol/timeframe once a replay looks interesting."
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
