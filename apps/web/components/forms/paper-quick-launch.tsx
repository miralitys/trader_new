"use client";

import { useState } from "react";

import { useStartStrategyPaper, useStopStrategyPaper, useStrategy, useStrategyRuns } from "@/lib/query-hooks";
import type { StrategyPaperStartRequest } from "@/lib/types";
import { formatCurrency, formatInteger, formatStatusLabel, getErrorMessage } from "@/lib/utils";

const strategyCode = "trend_reclaim_72h";

const launchConfig: StrategyPaperStartRequest = {
  symbols: ["BTC-USDT", "AVAX-USDT", "ETH-USDT", "SOL-USDT"],
  timeframes: ["1h"],
  exchange_code: "binance_us",
  initial_balance: "10000",
  currency: "USD",
  fee: "0.001",
  slippage: "0.0005",
  strategy_config_override: {
    target_r_multiple: 2.2,
  },
  metadata: {
    source: "paper_page_quick_launch",
    portfolio: "trend_reclaim_72h_core",
  },
};

export function PaperQuickLaunch() {
  const strategyQuery = useStrategy(strategyCode);
  const runsQuery = useStrategyRuns({ strategyCode, mode: "paper", limit: 10 });
  const startPaper = useStartStrategyPaper(strategyCode);
  const stopPaper = useStopStrategyPaper(strategyCode);
  const [message, setMessage] = useState<string | null>(null);

  const strategy = strategyQuery.data;
  const activeRun = runsQuery.data?.find((run) => run.status === "running" && run.mode === "paper") ?? runsQuery.data?.[0] ?? null;

  const running = strategy?.active_paper_status === "running";
  const busy = strategyQuery.isLoading || runsQuery.isLoading;

  async function handleStart() {
    setMessage(null);

    try {
      const result = await startPaper.mutateAsync(launchConfig);
      setMessage(`Paper run #${result.run_id} запущен.`);
    } catch (error) {
      setMessage(getErrorMessage(error, "Не удалось запустить paper trading."));
    }
  }

  async function handleStop() {
    setMessage(null);

    try {
      const result = await stopPaper.mutateAsync({ reason: "paper_page_manual_stop" });
      setMessage(`Paper run остановлен со статусом ${formatStatusLabel(result.status)}.`);
    } catch (error) {
      setMessage(getErrorMessage(error, "Не удалось остановить paper trading."));
    }
  }

  return (
    <div className="grid gap-5">
      <div className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
          <p className="text-[11px] uppercase tracking-[0.22em] text-slate-400">Статус</p>
          <p className="mt-3 text-2xl font-semibold text-white">{busy ? "Loading..." : formatStatusLabel(strategy?.active_paper_status ?? "idle")}</p>
          <p className="mt-2 text-sm text-slate-400">Состояние хранится на backend и не теряется при выходе со страницы.</p>
        </div>
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
          <p className="text-[11px] uppercase tracking-[0.22em] text-slate-400">Активный run</p>
          <p className="mt-3 text-2xl font-semibold text-white">{activeRun ? `#${activeRun.id}` : "N/A"}</p>
          <p className="mt-2 text-sm text-slate-400">{activeRun ? activeRun.symbols.join(", ") : "Еще не запущен"}</p>
        </div>
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
          <p className="text-[11px] uppercase tracking-[0.22em] text-slate-400">Баланс</p>
          <p className="mt-3 text-2xl font-semibold text-white">
            {activeRun?.account_balance ? formatCurrency(activeRun.account_balance, activeRun.currency ?? "USD") : "$10,000.00"}
          </p>
          <p className="mt-2 text-sm text-slate-400">Стартовый paper balance для quick launch.</p>
        </div>
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
          <p className="text-[11px] uppercase tracking-[0.22em] text-slate-400">Рынки</p>
          <p className="mt-3 text-2xl font-semibold text-white">{formatInteger(launchConfig.symbols.length)}</p>
          <p className="mt-2 text-sm text-slate-400">{launchConfig.symbols.join(", ")}</p>
        </div>
      </div>

      <div className="rounded-3xl border border-white/8 bg-white/[0.03] p-5">
        <p className="text-[11px] uppercase tracking-[0.2em] text-emerald-300">Quick launch</p>
        <h3 className="mt-2 text-xl font-semibold text-white">BTC + AVAX + ETH + SOL</h3>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-300">
          Один paper run для четырех рынков с уже выбранным рабочим конфигом. Кнопка запускает виртуальную торговлю, а кнопка
          остановки завершает текущий активный run. После перехода на другую страницу статус сохранится, потому что он живет в backend.
        </p>

        <div className="mt-5 flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={handleStart}
            disabled={startPaper.isPending || running || busy}
            className="rounded-2xl bg-emerald-400 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
          >
            {startPaper.isPending ? "Запуск..." : "Запустить"}
          </button>
          <button
            type="button"
            onClick={handleStop}
            disabled={stopPaper.isPending || !running || busy}
            className="rounded-2xl border border-rose-400/30 bg-rose-500/10 px-5 py-3 text-sm font-semibold text-rose-100 transition hover:bg-rose-500/20 disabled:cursor-not-allowed disabled:border-white/10 disabled:bg-slate-900 disabled:text-slate-500"
          >
            {stopPaper.isPending ? "Остановка..." : "Остановить"}
          </button>
          {message ? <span className="text-sm text-slate-300">{message}</span> : null}
        </div>

        <div className="mt-5 rounded-3xl border border-white/8 bg-[#040814] p-4">
          <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Launch config</p>
          <pre className="mt-3 overflow-x-auto text-xs leading-6 text-sky-100">{JSON.stringify(launchConfig, null, 2)}</pre>
        </div>
      </div>
    </div>
  );
}
