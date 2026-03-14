"use client";

import { useParams } from "next/navigation";

import { EquityChart } from "@/components/charts/equity-chart";
import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useBacktest } from "@/lib/query-hooks";
import { formatCurrency, formatDateTime, formatInteger, formatNumber, formatPercent, getErrorMessage, prettyJson } from "@/lib/utils";

export default function BacktestDetailPage() {
  const params = useParams<{ id: string }>();
  const id = Number(Array.isArray(params?.id) ? params.id[0] : params?.id);
  const backtestQuery = useBacktest(id);

  if (backtestQuery.isLoading && !backtestQuery.data) {
    return <LoadingState label="Loading backtest..." />;
  }

  if (backtestQuery.error) {
    return <ErrorState message={getErrorMessage(backtestQuery.error, "Unable to load backtest detail.")} />;
  }

  const backtest = backtestQuery.data;
  if (!backtest) {
    return <ErrorState message={`Backtest ${id} was not found.`} />;
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Backtest Detail"
        title={`Run #${backtest.run_id ?? id}`}
        description={`${backtest.strategy_code} on ${backtest.symbol} · ${backtest.timeframe}`}
        actions={<StatusBadge status={backtest.status} />}
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="Total return"
          value={formatPercent(backtest.metrics.total_return_pct)}
          tone={Number(backtest.metrics.total_return_pct) >= 0 ? "positive" : "danger"}
        />
        <MetricCard label="Final equity" value={formatCurrency(backtest.final_equity)} hint={`Started ${formatCurrency(backtest.initial_capital)}`} />
        <MetricCard label="Win rate" value={formatPercent(backtest.metrics.win_rate_pct)} hint={`${formatInteger(backtest.metrics.total_trades)} trades`} />
        <MetricCard label="Max drawdown" value={formatPercent(backtest.metrics.max_drawdown_pct)} tone="warning" />
        <MetricCard label="Profit factor" value={String(backtest.metrics.profit_factor)} />
        <MetricCard label="Expectancy" value={formatCurrency(backtest.metrics.expectancy)} />
        <MetricCard label="Avg winner" value={formatCurrency(backtest.metrics.avg_winner)} tone="positive" />
        <MetricCard label="Avg loser" value={formatCurrency(backtest.metrics.avg_loser)} tone="danger" />
      </section>

      <div className="grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
        <SectionCard title="Equity curve" eyebrow="Mark-to-market history">
          <EquityChart data={backtest.equity_curve} />
        </SectionCard>

        <SectionCard title="Run metadata" eyebrow="Execution context">
          <div className="grid gap-4">
            <MetricCard label="Started" value={formatDateTime(backtest.started_at)} />
            <MetricCard label="Completed" value={formatDateTime(backtest.completed_at)} />
            <MetricCard label="Exchange" value={backtest.exchange_code} />
          </div>
          <div className="mt-5 rounded-2xl border border-white/8 bg-slate-950/50 p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Params</p>
            <pre className="mt-3 overflow-x-auto text-xs text-slate-300">{prettyJson(backtest.params)}</pre>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="Trades" eyebrow="Simulated fills and exits">
        <DataTable
          rows={backtest.trades}
          rowKey={(trade, index) => `${trade.entry_time}-${trade.exit_time}-${index}`}
          emptyTitle="No trades executed"
          emptyDescription="This backtest completed without any entries or exits."
          columns={[
            {
              key: "side",
              title: "Side",
              render: (trade) => <StatusBadge status={trade.side} />,
            },
            {
              key: "entry",
              title: "Entry",
              render: (trade) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{formatCurrency(trade.entry_price)}</span>
                  <span className="text-xs text-slate-400">{formatDateTime(trade.entry_time)}</span>
                </div>
              ),
            },
            {
              key: "exit",
              title: "Exit",
              render: (trade) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{formatCurrency(trade.exit_price)}</span>
                  <span className="text-xs text-slate-400">{formatDateTime(trade.exit_time)}</span>
                </div>
              ),
            },
            {
              key: "qty",
              title: "Qty",
              render: (trade) => formatNumber(trade.qty, 6),
            },
            {
              key: "pnl",
              title: "PnL",
              render: (trade) => (
                <span className={Number(trade.pnl) >= 0 ? "text-emerald-300" : "text-rose-300"}>{formatCurrency(trade.pnl)}</span>
              ),
            },
            {
              key: "pnl_pct",
              title: "PnL %",
              render: (trade) => formatPercent(trade.pnl_pct),
            },
            {
              key: "fees",
              title: "Fees",
              render: (trade) => formatCurrency(trade.fees),
            },
            {
              key: "reason",
              title: "Exit reason",
              render: (trade) => trade.exit_reason,
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}
