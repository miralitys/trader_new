"use client";

import { FormEvent, useState } from "react";

import { useRunDataSync } from "@/lib/query-hooks";
import { toDatetimeLocalInput } from "@/lib/utils";

const presetSymbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"] as const;

export function DataSyncForm() {
  const syncMutation = useRunDataSync();
  const [mode, setMode] = useState<"initial" | "incremental" | "manual">("manual");
  const [symbol, setSymbol] = useState<string>(presetSymbols[0]);
  const [timeframe, setTimeframe] = useState("5m");
  const [startAt, setStartAt] = useState(toDatetimeLocalInput(new Date(Date.now() - 1000 * 60 * 60 * 24 * 7)));
  const [endAt, setEndAt] = useState(toDatetimeLocalInput(new Date()));
  const [message, setMessage] = useState<string | null>(null);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);

    try {
      const result = await syncMutation.mutateAsync({
        mode,
        exchange_code: "binance_us",
        symbol,
        timeframe,
        start_at: mode === "incremental" ? undefined : new Date(startAt).toISOString(),
        end_at: new Date(endAt).toISOString(),
      });
      setMessage(`Sync job #${result.job_id} finished with status ${result.status}. Inserted ${result.inserted_rows} candles.`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to run data sync.");
    }
  }

  const showRange = mode !== "incremental";

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <Field label="Mode">
          <select value={mode} onChange={(event) => setMode(event.target.value as typeof mode)} className={inputClassName}>
            <option value="manual">manual</option>
            <option value="initial">initial</option>
            <option value="incremental">incremental</option>
          </select>
        </Field>

        <Field label="Symbol">
          <select value={symbol} onChange={(event) => setSymbol(event.target.value)} className={inputClassName}>
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

        {showRange ? (
          <Field label="Start">
            <input type="datetime-local" value={startAt} onChange={(event) => setStartAt(event.target.value)} className={inputClassName} />
          </Field>
        ) : null}

        <Field label="End">
          <input type="datetime-local" value={endAt} onChange={(event) => setEndAt(event.target.value)} className={inputClassName} />
        </Field>
      </div>

      <div className="flex flex-col gap-3 border-t border-white/6 pt-4 md:flex-row md:items-center md:justify-between">
        <p className="text-sm text-slate-400">Use incremental mode to top up the latest candles with overlap and dedupe. Initial and manual modes require an explicit range.</p>
        <div className="flex flex-col items-start gap-3 sm:flex-row sm:items-center">
          {message ? <span className="text-sm text-slate-300">{message}</span> : null}
          <button
            type="submit"
            disabled={syncMutation.isPending}
            className="rounded-xl bg-emerald-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
          >
            {syncMutation.isPending ? "Running..." : "Run sync"}
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
