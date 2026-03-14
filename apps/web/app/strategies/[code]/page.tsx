"use client";

import Link from "next/link";
import { useParams } from "next/navigation";

import { PaperTradingForm } from "@/components/forms/paper-trading-form";
import { StrategyConfigForm } from "@/components/forms/strategy-config-form";
import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useBacktests, usePositions, useSignals, useStrategy, useStrategyConfig, useStrategyRuns, useTrades } from "@/lib/query-hooks";
import { formatCurrency, formatDateTime, formatInteger, formatNumber, formatPercent, getErrorMessage, prettyJson } from "@/lib/utils";

export default function StrategyDetailPage() {
  const params = useParams<{ code: string }>();
  const code = Array.isArray(params?.code) ? params.code[0] : params?.code ?? "";

  const strategyQuery = useStrategy(code);
  const configQuery = useStrategyConfig(code);
  const runsQuery = useStrategyRuns({ strategyCode: code, limit: 50 });
  const backtestsQuery = useBacktests({ strategyCode: code, limit: 20 });
  const runs = runsQuery.data ?? [];
  const focusRun = runs.find((run) => run.status === "running" && run.mode === "paper") ?? runs[0] ?? null;

  const signalsQuery = useSignals({ strategyRunId: focusRun?.id, limit: 20 }, Boolean(focusRun?.id));
  const tradesQuery = useTrades({ strategyRunId: focusRun?.id, limit: 20 }, Boolean(focusRun?.id));
  const positionsQuery = usePositions({ strategyRunId: focusRun?.id, limit: 20 }, Boolean(focusRun?.id));

  const isLoading =
    strategyQuery.isLoading ||
    configQuery.isLoading ||
    runsQuery.isLoading ||
    backtestsQuery.isLoading ||
    (Boolean(focusRun?.id) && (signalsQuery.isLoading || tradesQuery.isLoading || positionsQuery.isLoading));
  const error =
    strategyQuery.error ??
    configQuery.error ??
    runsQuery.error ??
    backtestsQuery.error ??
    signalsQuery.error ??
    tradesQuery.error ??
    positionsQuery.error;

  if (isLoading && !strategyQuery.data) {
    return <LoadingState label="Loading strategy..." />;
  }

  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load strategy details.")} />;
  }

  const strategy = strategyQuery.data;
  const config = configQuery.data;

  if (!strategy || !config) {
    return <ErrorState message={`Strategy ${code} was not found.`} />;
  }

  const latestBacktest = backtestsQuery.data?.[0] ?? null;
  const signals = signalsQuery.data ?? [];
  const trades = tradesQuery.data ?? [];
  const positions = positionsQuery.data ?? [];

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Strategy Console"
        title={strategy.name}
        description={strategy.description}
        actions={
          <>
            <StatusBadge status={strategy.active_paper_status ?? "idle"} />
            <div className="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-right">
              <p className="text-[11px] uppercase tracking-[0.18em] text-slate-400">Config source</p>
              <p className="mt-2 text-sm text-white">{strategy.config_source}</p>
            </div>
          </>
        }
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="Paper status"
          value={strategy.active_paper_status ? strategy.active_paper_status.toUpperCase() : "IDLE"}
          hint={focusRun ? `Run #${focusRun.id}` : "No active run"}
          tone={strategy.active_paper_status === "running" ? "positive" : "default"}
        />
        <MetricCard
          label="Account balance"
          value={focusRun?.account_balance ? formatCurrency(focusRun.account_balance, focusRun.currency ?? "USD") : "N/A"}
          hint={focusRun?.currency ?? "No paper account"}
        />
        <MetricCard
          label="Open positions"
          value={formatInteger(focusRun?.open_positions_count ?? positions.filter((position) => position.status === "open").length)}
          hint={focusRun ? "Focused paper run" : "No paper run selected"}
          tone={positions.some((position) => position.status === "open") ? "positive" : "default"}
        />
        <MetricCard
          label="Latest backtest"
          value={latestBacktest ? formatPercent(latestBacktest.total_return_pct) : "N/A"}
          hint={latestBacktest ? `${latestBacktest.symbol} · ${latestBacktest.timeframe}` : "No backtests yet"}
          tone={latestBacktest && Number(latestBacktest.total_return_pct) >= 0 ? "positive" : "default"}
        />
      </section>

      <div className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
        <SectionCard title="Paper trading controls" eyebrow="Start and stop isolated runtime execution">
          <PaperTradingForm strategy={strategy} initialConfig={config.config} />
        </SectionCard>

        <SectionCard title="Status and metrics" eyebrow="Current run and latest research">
          <div className="grid gap-4">
            <div className="rounded-2xl border border-white/8 bg-white/[0.03] p-4">
              <div className="flex items-center justify-between gap-4">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Focused run</p>
                  <p className="mt-2 text-lg font-semibold text-white">{focusRun ? `#${focusRun.id}` : "No paper run"}</p>
                </div>
                {focusRun ? <StatusBadge status={focusRun.status} /> : null}
              </div>
              <div className="mt-4 grid gap-3 text-sm text-slate-300">
                <p>Symbols: {focusRun?.symbols.join(", ") || "N/A"}</p>
                <p>Timeframes: {focusRun?.timeframes.join(", ") || "N/A"}</p>
                <p>Last processed candle: {formatDateTime(focusRun?.last_processed_candle_at)}</p>
              </div>
            </div>

            <div className="rounded-2xl border border-white/8 bg-white/[0.03] p-4">
              <div className="flex items-center justify-between gap-4">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Latest backtest snapshot</p>
                  <p className="mt-2 text-lg font-semibold text-white">
                    {latestBacktest ? `${latestBacktest.symbol} · ${latestBacktest.timeframe}` : "No backtests"}
                  </p>
                </div>
                {latestBacktest ? (
                  <Link href={`/backtests/${latestBacktest.id}`} className="text-sm text-sky-300 transition hover:text-sky-200">
                    Open
                  </Link>
                ) : null}
              </div>
              {latestBacktest ? (
                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                  <MetricCard
                    label="Return"
                    value={formatPercent(latestBacktest.total_return_pct)}
                    tone={Number(latestBacktest.total_return_pct) >= 0 ? "positive" : "danger"}
                  />
                  <MetricCard label="Win rate" value={formatPercent(latestBacktest.win_rate_pct)} />
                  <MetricCard label="Trades" value={formatInteger(latestBacktest.total_trades)} />
                  <MetricCard label="Max DD" value={formatPercent(latestBacktest.max_drawdown_pct)} tone="warning" />
                </div>
              ) : (
                <p className="mt-4 text-sm text-slate-400">Run a backtest to populate this block.</p>
              )}
            </div>

            <div className="rounded-2xl border border-white/8 bg-slate-950/50 p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Effective config preview</p>
              <pre className="mt-3 overflow-x-auto text-xs text-slate-300">{prettyJson(strategy.effective_config)}</pre>
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="Config editor" eyebrow="Persist validated strategy configuration">
        <StrategyConfigForm strategyCode={strategy.code} initialConfig={config.config} />
      </SectionCard>

      <div className="grid gap-6 xl:grid-cols-[1.05fr_1fr]">
        <SectionCard title="Recent signals" eyebrow="Focused paper run">
          <DataTable
            rows={signals}
            rowKey={(signal) => signal.id}
            emptyTitle="No signals yet"
            emptyDescription="Signals will appear after the strategy processes new candles."
            columns={[
              {
                key: "type",
                title: "Type",
                render: (signal) => <StatusBadge status={signal.signal_type} />,
              },
              {
                key: "market",
                title: "Market",
                render: (signal) => (
                  <div className="grid gap-1">
                    <span className="font-medium text-white">{signal.symbol}</span>
                    <span className="text-xs text-slate-400">{signal.timeframe}</span>
                  </div>
                ),
              },
              {
                key: "strength",
                title: "Strength",
                render: (signal) => formatPercent(signal.signal_strength, 3),
              },
              {
                key: "time",
                title: "Candle time",
                render: (signal) => formatDateTime(signal.candle_time),
              },
            ]}
          />
        </SectionCard>

        <SectionCard title="Recent trades" eyebrow="Focused paper run">
          <DataTable
            rows={trades}
            rowKey={(trade) => trade.id}
            emptyTitle="No trades yet"
            emptyDescription="Open and close positions in paper mode to populate the trade blotter."
            columns={[
              {
                key: "symbol",
                title: "Market",
                render: (trade) => trade.symbol,
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
      </div>

      <SectionCard title="Current positions" eyebrow="Open and closed position state">
        <DataTable
          rows={positions}
          rowKey={(position) => position.id}
          emptyTitle="No positions"
          emptyDescription="The selected run has not opened any positions yet."
          columns={[
            {
              key: "status",
              title: "Status",
              render: (position) => <StatusBadge status={position.status} />,
            },
            {
              key: "symbol",
              title: "Market",
              render: (position) => position.symbol,
            },
            {
              key: "qty",
              title: "Qty",
              render: (position) => formatNumber(position.qty, 6),
            },
            {
              key: "entry",
              title: "Avg entry",
              render: (position) => formatCurrency(position.avg_entry_price),
            },
            {
              key: "stop",
              title: "Stop / TP",
              render: (position) =>
                `${position.stop_price ? formatCurrency(position.stop_price) : "N/A"} / ${position.take_profit_price ? formatCurrency(position.take_profit_price) : "N/A"}`,
            },
            {
              key: "opened",
              title: "Opened",
              render: (position) => formatDateTime(position.opened_at),
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}
