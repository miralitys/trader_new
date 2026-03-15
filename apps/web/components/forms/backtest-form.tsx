"use client";

import { useRouter } from "next/navigation";
import { FormEvent, useMemo, useState, startTransition } from "react";

import { useRunBacktest } from "@/lib/query-hooks";
import type { StrategySummary } from "@/lib/types";
import { compactList, parseJsonInput, toDatetimeLocalInput } from "@/lib/utils";

type BacktestFormProps = {
  strategies: StrategySummary[];
};

const presetSymbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"] as const;
const rangePresets = [30, 60, 90] as const;

function buildRangeInputs(days: number) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * days);
  return {
    startAt: toDatetimeLocalInput(start),
    endAt: toDatetimeLocalInput(end),
  };
}

export function BacktestForm({ strategies }: BacktestFormProps) {
  const router = useRouter();
  const runBacktest = useRunBacktest();
  const [strategyCode, setStrategyCode] = useState(strategies[0]?.code ?? "");
  const [symbol, setSymbol] = useState(presetSymbols[0]);
  const [timeframe, setTimeframe] = useState("5m");
  const [selectedRangeDays, setSelectedRangeDays] = useState<number | null>(30);
  const [startAt, setStartAt] = useState(buildRangeInputs(30).startAt);
  const [endAt, setEndAt] = useState(buildRangeInputs(30).endAt);
  const [initialCapital, setInitialCapital] = useState("10000");
  const [fee, setFee] = useState("0.001");
  const [slippage, setSlippage] = useState("0.0005");
  const [positionSizePct, setPositionSizePct] = useState("1");
  const [overrides, setOverrides] = useState("{}");
  const [message, setMessage] = useState<string | null>(null);

  const disabled = runBacktest.isPending || !strategyCode;
  const sortedStrategies = useMemo(() => strategies.slice().sort((left, right) => left.name.localeCompare(right.name)), [strategies]);

  function applyRangePreset(days: number) {
    const nextRange = buildRangeInputs(days);
    setSelectedRangeDays(days);
    setStartAt(nextRange.startAt);
    setEndAt(nextRange.endAt);
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);

    try {
      const result = await runBacktest.mutateAsync({
        strategy_code: strategyCode,
        symbol,
        timeframe,
        start_at: new Date(startAt).toISOString(),
        end_at: new Date(endAt).toISOString(),
        exchange_code: "binance_us",
        initial_capital: initialCapital,
        fee,
        slippage,
        position_size_pct: positionSizePct,
        strategy_config_override: parseJsonInput(overrides, {}),
      });

      setMessage(`Backtest ${result.run_id ?? ""} completed and saved.`);
      if (result.run_id) {
        startTransition(() => {
          router.push(`/backtests/${result.run_id}`);
        });
      }
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to run backtest.");
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Field label="Strategy">
          <select value={strategyCode} onChange={(event) => setStrategyCode(event.target.value)} className={inputClassName}>
            {sortedStrategies.map((strategy) => (
              <option key={strategy.code} value={strategy.code}>
                {strategy.name}
              </option>
            ))}
          </select>
        </Field>

        <Field label="Symbol">
          <select value={symbol} onChange={(event) => setSymbol(compactList([event.target.value])[0] ?? presetSymbols[0])} className={inputClassName}>
            {presetSymbols.map((presetSymbol) => (
              <option key={presetSymbol} value={presetSymbol}>
                {presetSymbol}
              </option>
            ))}
          </select>
        </Field>

        <Field label="Timeframe">
          <select value={timeframe} onChange={(event) => setTimeframe(event.target.value)} className={inputClassName}>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
          </select>
        </Field>

        <Field label="Initial capital">
          <input value={initialCapital} onChange={(event) => setInitialCapital(event.target.value)} className={inputClassName} inputMode="decimal" />
        </Field>

        <Field label="Start">
          <input
            type="datetime-local"
            value={startAt}
            onChange={(event) => {
              setSelectedRangeDays(null);
              setStartAt(event.target.value);
            }}
            className={inputClassName}
          />
        </Field>

        <Field label="End">
          <input
            type="datetime-local"
            value={endAt}
            onChange={(event) => {
              setSelectedRangeDays(null);
              setEndAt(event.target.value);
            }}
            className={inputClassName}
          />
        </Field>

        <Field label="Quick range">
          <div className="flex h-11 items-center gap-2 rounded-xl border border-white/10 bg-slate-950/60 px-2">
            {rangePresets.map((days) => (
              <button
                key={days}
                type="button"
                onClick={() => applyRangePreset(days)}
                className={`rounded-lg px-3 py-1.5 text-sm font-medium transition ${
                  selectedRangeDays === days
                    ? "bg-emerald-400 text-slate-950"
                    : "text-slate-300 hover:bg-white/5 hover:text-white"
                }`}
              >
                {days}d
              </button>
            ))}
          </div>
        </Field>

        <Field label="Fee">
          <input value={fee} onChange={(event) => setFee(event.target.value)} className={inputClassName} inputMode="decimal" />
        </Field>

        <Field label="Slippage">
          <input value={slippage} onChange={(event) => setSlippage(event.target.value)} className={inputClassName} inputMode="decimal" />
        </Field>

        <Field label="Position size pct">
          <input value={positionSizePct} onChange={(event) => setPositionSizePct(event.target.value)} className={inputClassName} inputMode="decimal" />
        </Field>
      </div>

      <Field label="Strategy config override JSON">
        <textarea
          value={overrides}
          onChange={(event) => setOverrides(event.target.value)}
          rows={8}
          className={textareaClassName}
          placeholder='{"symbols":["BTC-USDT"],"timeframes":["5m"]}'
        />
      </Field>

      <div className="flex flex-col gap-3 border-t border-white/6 pt-4 md:flex-row md:items-center md:justify-between">
        <p className="text-sm text-slate-400">Backtests run against candles already stored in Postgres. The strategy config override is optional.</p>
        <div className="flex flex-col items-start gap-3 sm:flex-row sm:items-center">
          {message ? <span className="text-sm text-slate-300">{message}</span> : null}
          <button
            type="submit"
            disabled={disabled}
            className="rounded-xl bg-emerald-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
          >
            {runBacktest.isPending ? "Running..." : "Run backtest"}
          </button>
        </div>
      </div>
    </form>
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

const textareaClassName =
  "min-h-[180px] rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 font-mono text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-sky-400/40";
