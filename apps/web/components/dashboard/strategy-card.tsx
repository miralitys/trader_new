import Link from "next/link";

import { StatusBadge } from "@/components/ui/status-badge";
import type { NumericValue } from "@/lib/types";
import { formatInteger, formatPercent } from "@/lib/utils";

export type StrategyDashboardCard = {
  code: string;
  name: string;
  description: string;
  status: string;
  totalReturnPct: NumericValue;
  winRatePct: NumericValue;
  totalTrades: number;
  maxDrawdownPct: NumericValue;
  openPositions: number;
  symbols: string[];
  timeframes: string[];
};

export function StrategyCard({ strategy }: { strategy: StrategyDashboardCard }) {
  return (
    <Link
      href={`/strategies/${strategy.code}`}
      className="group rounded-[26px] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.03),transparent),rgba(9,14,24,0.8)] p-5 shadow-[0_16px_50px_rgba(0,0,0,0.25)] transition-colors hover:border-sky-400/25 hover:bg-[linear-gradient(180deg,rgba(255,255,255,0.04),transparent),rgba(11,16,28,0.88)]"
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="text-[11px] uppercase tracking-[0.24em] text-slate-400">{strategy.code}</p>
          <h3 className="mt-2 text-xl font-semibold tracking-tight text-white">{strategy.name}</h3>
          <p className="mt-2 line-clamp-2 text-sm text-slate-400">{strategy.description}</p>
        </div>
        <StatusBadge status={strategy.status} />
      </div>

      <div className="mt-5 grid grid-cols-2 gap-3">
        <Metric label="Return" value={formatPercent(strategy.totalReturnPct)} />
        <Metric label="Win rate" value={formatPercent(strategy.winRatePct)} />
        <Metric label="Trades" value={formatInteger(strategy.totalTrades)} />
        <Metric label="Max DD" value={formatPercent(strategy.maxDrawdownPct)} />
        <Metric label="Open positions" value={formatInteger(strategy.openPositions)} />
        <Metric label="Markets" value={formatInteger(strategy.symbols.length)} />
      </div>

      <div className="mt-5 flex flex-wrap gap-2">
        {strategy.symbols.length ? (
          strategy.symbols.map((symbol) => (
            <span key={symbol} className="rounded-full border border-white/10 bg-white/[0.05] px-2.5 py-1 text-xs text-slate-300">
              {symbol}
            </span>
          ))
        ) : (
          <span className="rounded-full border border-dashed border-white/10 px-2.5 py-1 text-xs text-slate-500">No symbols</span>
        )}
        {strategy.timeframes.map((timeframe) => (
          <span key={timeframe} className="rounded-full border border-sky-400/20 bg-sky-400/10 px-2.5 py-1 text-xs text-sky-200">
            {timeframe}
          </span>
        ))}
      </div>
    </Link>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-3 py-3">
      <p className="text-[10px] uppercase tracking-[0.18em] text-slate-500">{label}</p>
      <p className="mt-2 text-lg font-semibold text-white">{value}</p>
    </div>
  );
}
