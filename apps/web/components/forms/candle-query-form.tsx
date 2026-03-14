"use client";

import { FormEvent, useState } from "react";

import type { CandleFilters } from "@/lib/types";
import { toDatetimeLocalInput } from "@/lib/utils";

type CandleQueryFormProps = {
  onSubmit: (filters: CandleFilters) => void;
};

export function CandleQueryForm({ onSubmit }: CandleQueryFormProps) {
  const [symbol, setSymbol] = useState("BTC-USD");
  const [timeframe, setTimeframe] = useState("5m");
  const [startAt, setStartAt] = useState(toDatetimeLocalInput(new Date(Date.now() - 1000 * 60 * 60 * 24 * 3)));
  const [endAt, setEndAt] = useState(toDatetimeLocalInput(new Date()));
  const [limit, setLimit] = useState("500");

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onSubmit({
      symbol,
      timeframe,
      startAt: new Date(startAt).toISOString(),
      endAt: new Date(endAt).toISOString(),
      exchangeCode: "coinbase",
      limit: Number(limit),
    });
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <Field label="Symbol">
          <input value={symbol} onChange={(event) => setSymbol(event.target.value)} className={inputClassName} />
        </Field>

        <Field label="Timeframe">
          <select value={timeframe} onChange={(event) => setTimeframe(event.target.value)} className={inputClassName}>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
          </select>
        </Field>

        <Field label="Start">
          <input type="datetime-local" value={startAt} onChange={(event) => setStartAt(event.target.value)} className={inputClassName} />
        </Field>

        <Field label="End">
          <input type="datetime-local" value={endAt} onChange={(event) => setEndAt(event.target.value)} className={inputClassName} />
        </Field>

        <Field label="Limit">
          <input value={limit} onChange={(event) => setLimit(event.target.value)} className={inputClassName} inputMode="numeric" />
        </Field>
      </div>

      <div className="flex justify-end">
        <button
          type="submit"
          className="rounded-xl border border-sky-400/30 bg-sky-500/10 px-4 py-2 text-sm font-semibold text-sky-100 transition hover:bg-sky-500/20"
        >
          Query candles
        </button>
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
