"use client";

import { FormEvent, useEffect, useState } from "react";

import { useStartStrategyPaper, useStopStrategyPaper } from "@/lib/query-hooks";
import type { StrategySummary } from "@/lib/types";
import { compactList, getErrorMessage, parseJsonInput, prettyJson } from "@/lib/utils";

type PaperTradingFormProps = {
  strategy: StrategySummary;
  initialConfig: Record<string, unknown>;
};

const allowedSymbols = [
  "BTC-USDT",
  "ETH-USDT",
  "SOL-USDT",
  "BNB-USDT",
  "ADA-USDT",
  "ALPINE-USDT",
  "XRP-USDT",
  "1INCH-USDT",
  "LTC-USDT",
  "BCH-USDT",
  "AVAX-USDT",
  "LINK-USDT",
  "DOGE-USDT",
  "ICP-USDT",
  "GALA-USDT",
  "AXS-USDT",
  "ONDO-USDT",
  "IOTA-USDT",
  "FIL-USDT",
] as const;

export function PaperTradingForm({ strategy, initialConfig }: PaperTradingFormProps) {
  const startPaper = useStartStrategyPaper(strategy.code);
  const stopPaper = useStopStrategyPaper(strategy.code);
  const [symbols, setSymbols] = useState<string[]>([]);
  const [timeframes, setTimeframes] = useState<string[]>([]);
  const [initialBalance, setInitialBalance] = useState("10000");
  const [currency, setCurrency] = useState("USD");
  const [fee, setFee] = useState("0.001");
  const [slippage, setSlippage] = useState("0.0005");
  const [startFromLatest, setStartFromLatest] = useState(true);
  const [overrideText, setOverrideText] = useState("{}");
  const [stopReason, setStopReason] = useState("manual_stop");
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    const nextSymbols = compactList(initialConfig.symbols).filter((symbol) =>
      allowedSymbols.includes(symbol as (typeof allowedSymbols)[number]),
    );
    const nextTimeframes = compactList(initialConfig.timeframes);
    const nextInitialConfig = {
      ...initialConfig,
      symbols: nextSymbols.length ? nextSymbols : [allowedSymbols[0]],
      timeframes: nextTimeframes.length ? nextTimeframes : ["5m"],
    };
    setSymbols(nextSymbols.length ? nextSymbols : [allowedSymbols[0]]);
    setTimeframes(nextTimeframes.length ? nextTimeframes : ["5m"]);
    setOverrideText(prettyJson(nextInitialConfig));
  }, [initialConfig]);

  const running = strategy.active_paper_status === "running";

  async function handleStart(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);

    try {
      const result = await startPaper.mutateAsync({
        symbols,
        timeframes,
        exchange_code: "binance_us",
        initial_balance: initialBalance,
        currency,
        fee,
        slippage,
        start_from_latest: startFromLatest,
        strategy_config_override: parseJsonInput(overrideText, {}),
        metadata: {},
      });

      setMessage(`Paper run #${result.run_id} is ${result.status}.`);
    } catch (error) {
      setMessage(getErrorMessage(error, "Unable to start paper trading."));
    }
  }

  async function handleStop() {
    setMessage(null);

    try {
      const result = await stopPaper.mutateAsync({
        reason: stopReason,
      });
      setMessage(`Paper run stopped with status ${result.status}.`);
    } catch (error) {
      setMessage(getErrorMessage(error, "Unable to stop paper trading."));
    }
  }

  return (
    <div className="space-y-5">
      <form onSubmit={handleStart} className="space-y-5">
        <div className="grid gap-4 md:grid-cols-2">
          <Field label="Symbols">
            <input
              value={symbols.join(", ")}
              onChange={(event) =>
                setSymbols(
                  compactList(event.target.value.split(",")).filter((symbol) =>
                    allowedSymbols.includes(symbol as (typeof allowedSymbols)[number]),
                  ),
                )
              }
              className={inputClassName}
              placeholder="BTC-USDT, ETH-USDT, ICP-USDT, GALA-USDT, AXS-USDT, ONDO-USDT, IOTA-USDT, FIL-USDT"
            />
          </Field>

          <Field label="Timeframes">
            <input
              value={timeframes.join(", ")}
              onChange={(event) => setTimeframes(compactList(event.target.value.split(",")))}
              className={inputClassName}
              placeholder="5m, 15m"
            />
          </Field>

          <Field label="Initial balance">
            <input value={initialBalance} onChange={(event) => setInitialBalance(event.target.value)} className={inputClassName} />
          </Field>

          <Field label="Currency">
            <input value={currency} onChange={(event) => setCurrency(event.target.value)} className={inputClassName} />
          </Field>

          <Field label="Fee">
            <input value={fee} onChange={(event) => setFee(event.target.value)} className={inputClassName} />
          </Field>

          <Field label="Slippage">
            <input value={slippage} onChange={(event) => setSlippage(event.target.value)} className={inputClassName} />
          </Field>
        </div>

        <label className="flex items-start gap-3 rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 text-sm text-slate-300">
          <input
            type="checkbox"
            checked={startFromLatest}
            onChange={(event) => setStartFromLatest(event.target.checked)}
            className="mt-0.5 h-4 w-4 rounded border-white/20 bg-slate-950 text-emerald-400 focus:ring-emerald-400"
          />
          <span>
            <span className="block font-medium text-white">Start from latest candle</span>
            <span className="mt-1 block text-slate-400">
              Если включено, paper не будет переигрывать старую историю из базы и начнет ждать только новые свечи.
            </span>
          </span>
        </label>

        <label className="grid gap-2">
          <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Paper runtime config override JSON</span>
          <textarea
            rows={10}
            value={overrideText}
            onChange={(event) => setOverrideText(event.target.value)}
            className="rounded-2xl border border-white/10 bg-slate-950/70 px-3 py-3 font-mono text-sm text-white outline-none transition focus:border-sky-400/40"
          />
        </label>

        <div className="flex flex-wrap items-center gap-3 border-t border-white/6 pt-4">
          <button
            type="submit"
            disabled={startPaper.isPending || running}
            className="rounded-xl bg-emerald-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
          >
            {startPaper.isPending ? "Starting..." : "Start paper trading"}
          </button>

          <input
            value={stopReason}
            onChange={(event) => setStopReason(event.target.value)}
            className={inputClassName}
            placeholder="manual_stop"
          />

          <button
            type="button"
            onClick={handleStop}
            disabled={stopPaper.isPending || !running}
            className="rounded-xl border border-rose-400/30 bg-rose-500/10 px-4 py-2 text-sm font-semibold text-rose-100 transition hover:bg-rose-500/20 disabled:cursor-not-allowed disabled:border-white/10 disabled:bg-slate-900 disabled:text-slate-500"
          >
            {stopPaper.isPending ? "Stopping..." : "Stop paper trading"}
          </button>

          {message ? <span className="text-sm text-slate-300">{message}</span> : null}
        </div>
      </form>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="grid gap-2">
      <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">{label}</span>
      {children}
    </label>
  );
}

const inputClassName =
  "h-11 rounded-xl border border-white/10 bg-slate-950/60 px-3 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-sky-400/40";
