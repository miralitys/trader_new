import Link from "next/link";

import { SectionCard } from "@/components/section-card";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { prettyJson } from "@/lib/utils";

type PaperMarket = {
  symbol: string;
  priority: "Tier 1" | "Tier 2" | "Watchlist";
  timeframe: string;
  overrideConfig: Record<string, unknown>;
  returnPct: number;
  profitFactor: number;
  maxDrawdownPct: number;
  trades: number;
  note: string;
};

const liveMarkets: PaperMarket[] = [
  {
    symbol: "BTC-USDT",
    priority: "Tier 1",
    timeframe: "1h",
    overrideConfig: { target_r_multiple: 2.2 },
    returnPct: 6.31,
    profitFactor: 6.45,
    maxDrawdownPct: 4.39,
    trades: 5,
    note: "Главный anchor-market. Лучший и самый устойчивый результат в research."
  },
  {
    symbol: "AVAX-USDT",
    priority: "Tier 1",
    timeframe: "1h",
    overrideConfig: { target_r_multiple: 2.2 },
    returnPct: 7.67,
    profitFactor: 3.67,
    maxDrawdownPct: 5.25,
    trades: 7,
    note: "Сильный второй рынок. На 180d выглядит очень уверенно и не ломается сразу на длинных окнах."
  },
  {
    symbol: "ETH-USDT",
    priority: "Tier 2",
    timeframe: "1h",
    overrideConfig: { target_r_multiple: 2.2 },
    returnPct: 2.06,
    profitFactor: 2.02,
    maxDrawdownPct: 1.69,
    trades: 5,
    note: "Хороший secondary-market на 180d, но слабее по устойчивости, чем BTC и AVAX."
  },
  {
    symbol: "SOL-USDT",
    priority: "Tier 2",
    timeframe: "1h",
    overrideConfig: { target_r_multiple: 2.2 },
    returnPct: 1.99,
    profitFactor: 1.64,
    maxDrawdownPct: 3.33,
    trades: 5,
    note: "Можно держать в ротации как дополнительный рынок, но не как core sleeve."
  }
];

const watchlistMarkets: PaperMarket[] = [
  {
    symbol: "ADA-USDT",
    priority: "Watchlist",
    timeframe: "1h",
    overrideConfig: { target_r_multiple: 2.2 },
    returnPct: 6.22,
    profitFactor: 8.28,
    maxDrawdownPct: 3.09,
    trades: 2,
    note: "Результат красивый, но выборка слишком маленькая. Пока не включать в core paper rotation."
  }
];

const excludedSymbols = [
  "ALPINE-USDT",
  "BNB-USDT",
  "XRP-USDT",
  "1INCH-USDT",
  "LTC-USDT",
  "BCH-USDT",
  "LINK-USDT",
  "DOGE-USDT",
  "ICP-USDT",
  "GALA-USDT",
  "AXS-USDT",
  "IOTA-USDT",
  "FIL-USDT"
];

const blockedSymbols = [
  {
    symbol: "ONDO-USDT",
    reason: "Пока не хватает длинной истории для честного long warmup этой стратегии."
  }
];

function marketPayload(market: PaperMarket) {
  return {
    strategy_code: "trend_reclaim_72h",
    symbol: market.symbol,
    timeframe: market.timeframe,
    exchange_code: "binance_us",
    initial_capital: "1000",
    fee: "0.001",
    slippage: "0.0005",
    position_size_pct: "1",
    strategy_config_override: market.overrideConfig
  };
}

function priorityTone(priority: PaperMarket["priority"]) {
  switch (priority) {
    case "Tier 1":
      return "border-emerald-400/20 bg-emerald-500/[0.08]";
    case "Tier 2":
      return "border-sky-400/20 bg-sky-500/[0.08]";
    default:
      return "border-amber-400/20 bg-amber-500/[0.08]";
  }
}

function MarketCard({ market }: { market: PaperMarket }) {
  return (
    <article className={`rounded-3xl border p-5 ${priorityTone(market.priority)}`}>
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-[11px] uppercase tracking-[0.22em] text-slate-300">{market.priority}</p>
          <h3 className="mt-2 text-2xl font-semibold text-white">{market.symbol}</h3>
          <p className="mt-2 max-w-xl text-sm leading-6 text-slate-300">{market.note}</p>
        </div>
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-right">
          <p className="text-[11px] uppercase tracking-[0.18em] text-slate-400">Timeframe</p>
          <p className="mt-2 text-lg font-semibold text-white">{market.timeframe}</p>
        </div>
      </div>

      <div className="mt-5 grid gap-3 md:grid-cols-4">
        <MetricCard label="Return" value={`${market.returnPct.toFixed(2)}%`} tone={market.returnPct >= 0 ? "positive" : "danger"} />
        <MetricCard label="Profit Factor" value={market.profitFactor.toFixed(2)} tone={market.profitFactor >= 1 ? "positive" : "warning"} />
        <MetricCard label="Max DD" value={`${market.maxDrawdownPct.toFixed(2)}%`} tone={market.maxDrawdownPct <= 5 ? "positive" : "warning"} />
        <MetricCard label="Trades" value={market.trades} hint="180d backtest" />
      </div>

      <div className="mt-5 rounded-3xl border border-white/8 bg-[#040814] p-4">
        <div className="flex items-center justify-between gap-4">
          <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Paper payload</p>
          <span className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300">Ready to copy</span>
        </div>
        <pre className="mt-3 overflow-x-auto text-xs leading-6 text-sky-100">{prettyJson(marketPayload(market))}</pre>
      </div>
    </article>
  );
}

export default function PaperPage() {
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Paper Deployment"
        title="TrendReclaim72h paper playbook"
        description="Отдельная operator-страница с финальным shortlist по рынкам, готовыми payload-ами и коротким operational plan для paper rotation."
        actions={
          <>
            <Link
              href="/strategies/trend_reclaim_72h"
              className="rounded-2xl border border-sky-400/20 bg-sky-500/[0.08] px-4 py-3 text-sm font-medium text-sky-100 transition hover:bg-sky-500/[0.14]"
            >
              Open strategy page
            </Link>
            <Link
              href="/backtests"
              className="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm font-medium text-slate-200 transition hover:bg-white/[0.08]"
            >
              Open backtests
            </Link>
          </>
        }
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Tier 1 markets" value="2" hint="BTC · AVAX" tone="positive" />
        <MetricCard label="Tier 2 markets" value="2" hint="ETH · SOL" tone="positive" />
        <MetricCard label="Watchlist" value="1" hint="ADA" tone="warning" />
        <MetricCard label="Dropped markets" value={excludedSymbols.length} hint="Not for paper now" tone="danger" />
      </section>

      <SectionCard
        title="Paper Rotation"
        eyebrow="Ready now"
        actions={<span className="rounded-full border border-emerald-400/20 bg-emerald-500/[0.08] px-3 py-1 text-xs text-emerald-200">Use same config family</span>}
      >
        <div className="grid gap-5">
          {liveMarkets.map((market) => (
            <MarketCard key={market.symbol} market={market} />
          ))}
        </div>
      </SectionCard>

      <SectionCard title="Watchlist" eyebrow="Observe first">
        <div className="grid gap-5">
          {watchlistMarkets.map((market) => (
            <MarketCard key={market.symbol} market={market} />
          ))}
        </div>
      </SectionCard>

      <div className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <SectionCard title="Operator Checklist" eyebrow="First 14 days">
          <div className="grid gap-4 text-sm leading-6 text-slate-300">
            <div className="rounded-3xl border border-white/8 bg-white/[0.03] p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Days 1-7</p>
              <ul className="mt-3 grid gap-2">
                <li>Verify that BTC and AVAX are actually generating signals and not stalling in idle mode.</li>
                <li>Track trade count, fast failures, and whether paper fills resemble backtest cadence.</li>
                <li>Keep ETH and SOL as secondary sleeves, not as the main verdict drivers.</li>
              </ul>
            </div>
            <div className="rounded-3xl border border-white/8 bg-white/[0.03] p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Days 8-14</p>
              <ul className="mt-3 grid gap-2">
                <li>If BTC and AVAX remain orderly, keep them as core paper rotation.</li>
                <li>If ETH or SOL underperform heavily, downgrade them to watch mode instead of tuning parameters immediately.</li>
                <li>Do not add dropped markets back into rotation without a fresh research pass.</li>
              </ul>
            </div>
          </div>
        </SectionCard>

        <SectionCard title="Excluded Right Now" eyebrow="Do not run in paper">
          <div className="grid gap-4">
            <div className="rounded-3xl border border-rose-400/20 bg-rose-500/[0.06] p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-rose-200">Dropped markets</p>
              <p className="mt-3 text-sm leading-6 text-slate-300">{excludedSymbols.join(", ")}</p>
            </div>
            {blockedSymbols.map((item) => (
              <div key={item.symbol} className="rounded-3xl border border-amber-400/20 bg-amber-500/[0.06] p-4">
                <p className="text-[11px] uppercase tracking-[0.2em] text-amber-200">Blocked by data history</p>
                <p className="mt-3 text-sm font-medium text-white">{item.symbol}</p>
                <p className="mt-2 text-sm leading-6 text-slate-300">{item.reason}</p>
              </div>
            ))}
          </div>
        </SectionCard>
      </div>
    </div>
  );
}
