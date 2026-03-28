"use client";

import { FormEvent, useMemo, useState } from "react";

import { DateRangePresets } from "@/components/forms/date-range-presets";
import { useRunDataSync } from "@/lib/query-hooks";
import { formatInteger, formatPercent, getErrorMessage, toDatetimeLocalInput } from "@/lib/utils";

const presetSymbols = [
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

const batchTimeframes = ["4h", "1h", "15m", "5m", "1m"] as const;
const batchDayPresets = [30, 60, 90, 180, 365, 720] as const;
const candlesPerDayByTimeframe: Record<(typeof batchTimeframes)[number], number> = {
  "4h": 6,
  "1h": 24,
  "15m": 96,
  "5m": 288,
  "1m": 1440,
};

type BatchProgress = {
  startedAt: number;
  completedJobs: number;
  totalJobs: number;
  completedWeight: number;
  totalWeight: number;
};

export function DataSyncForm() {
  const syncMutation = useRunDataSync();
  const [mode, setMode] = useState<"initial" | "incremental" | "manual">("manual");
  const [symbol, setSymbol] = useState<string>(presetSymbols[0]);
  const [timeframe, setTimeframe] = useState("5m");
  const [startAt, setStartAt] = useState(toDatetimeLocalInput(new Date(Date.now() - 1000 * 60 * 60 * 24 * 7)));
  const [endAt, setEndAt] = useState(toDatetimeLocalInput(new Date()));
  const [message, setMessage] = useState<string | null>(null);
  const [batchMessage, setBatchMessage] = useState<string | null>(null);
  const [isBatchRunning, setIsBatchRunning] = useState(false);
  const [batchProgress, setBatchProgress] = useState<BatchProgress | null>(null);

  const isRunning = syncMutation.isPending || isBatchRunning;
  const selectedRangeDays = useMemo(() => {
    const startMs = new Date(startAt).getTime();
    const endMs = new Date(endAt).getTime();
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      return 0;
    }
    return (endMs - startMs) / (1000 * 60 * 60 * 24);
  }, [startAt, endAt]);

  const batchEtaText = useMemo(() => {
    if (!batchProgress || batchProgress.completedWeight <= 0) {
      return "ETA will appear after the first completed job.";
    }

    const elapsedMs = Date.now() - batchProgress.startedAt;
    const msPerWeight = elapsedMs / batchProgress.completedWeight;
    const remainingWeight = Math.max(batchProgress.totalWeight - batchProgress.completedWeight, 0);
    const remainingMs = remainingWeight * msPerWeight;

    if (!Number.isFinite(remainingMs) || remainingMs <= 0) {
      return "Finalizing queue...";
    }

    const remainingMinutes = Math.round(remainingMs / 60000);
    if (remainingMinutes < 60) {
      return `ETA ~${remainingMinutes} min remaining.`;
    }

    const hours = Math.floor(remainingMinutes / 60);
    const minutes = remainingMinutes % 60;
    return `ETA ~${hours}h ${minutes}m remaining.`;
  }, [batchProgress]);

  function applyDayPreset(days: number) {
    const nextEnd = new Date();
    const nextStart = new Date(nextEnd.getTime() - days * 24 * 60 * 60 * 1000);
    setStartAt(toDatetimeLocalInput(nextStart));
    setEndAt(toDatetimeLocalInput(nextEnd));
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);
    setBatchMessage(null);

    try {
      const result = await syncMutation.mutateAsync({
        mode,
        exchange_code: "binance_us",
        symbol,
        timeframe,
        start_at: mode === "incremental" ? undefined : new Date(startAt).toISOString(),
        end_at: new Date(endAt).toISOString(),
      });
      const coverageMessage = result.coverage
        ? ` Coverage ${formatInteger(result.coverage.candle_count)} / ${formatInteger(result.coverage.expected_candle_count)} (${formatPercent(result.coverage.completion_pct)}).`
        : "";
      setMessage(
        `Sync job #${result.job_id} finished with status ${result.status}. Inserted ${formatInteger(result.inserted_rows)} new candles.${coverageMessage}`,
      );
    } catch (error) {
      setMessage(getErrorMessage(error, "Unable to run data sync."));
    }
  }

  async function handleBatchSync() {
    if (mode === "incremental") {
      setBatchMessage("Add All Data works with manual or initial mode because it needs an explicit date range.");
      return;
    }

    setMessage(null);
    setBatchMessage(null);
    setIsBatchRunning(true);

    const totalJobs = presetSymbols.length * batchTimeframes.length;
    const totalWeight = presetSymbols.reduce((symbolAcc) => {
      return (
        symbolAcc +
        batchTimeframes.reduce((timeframeAcc, orderedTimeframe) => {
          return timeframeAcc + selectedRangeDays * candlesPerDayByTimeframe[orderedTimeframe];
        }, 0)
      );
    }, 0);
    let completedJobs = 0;
    let totalInsertedRows = 0;
    let completedWeight = 0;

    setBatchProgress({
      startedAt: Date.now(),
      completedJobs: 0,
      totalJobs,
      completedWeight: 0,
      totalWeight,
    });

    try {
      for (const orderedTimeframe of batchTimeframes) {
        for (const orderedSymbol of presetSymbols) {
          setBatchMessage(
            `Running ${completedJobs + 1}/${totalJobs}: ${orderedSymbol} ${orderedTimeframe} ` +
              `for ${startAt} -> ${endAt}`,
          );

          const result = await syncMutation.mutateAsync({
            mode,
            exchange_code: "binance_us",
            symbol: orderedSymbol,
            timeframe: orderedTimeframe,
            start_at: new Date(startAt).toISOString(),
            end_at: new Date(endAt).toISOString(),
          });

          completedJobs += 1;
          totalInsertedRows += result.inserted_rows;
          completedWeight += selectedRangeDays * candlesPerDayByTimeframe[orderedTimeframe];
          setBatchProgress((current) =>
            current
              ? {
                  ...current,
                  completedJobs,
                  completedWeight,
                }
              : current,
          );
          setBatchMessage(
            `Completed ${completedJobs}/${totalJobs}: ${orderedSymbol} ${orderedTimeframe}. ` +
              `Inserted ${formatInteger(result.inserted_rows)} candles this run.`,
          );
        }
      }

      setBatchMessage(
        `Add All Data finished. Completed ${totalJobs} sync jobs and inserted ` +
          `${formatInteger(totalInsertedRows)} candles in total.`,
      );
    } catch (error) {
      setBatchMessage(
        `Batch stopped on job ${completedJobs}/${totalJobs}. ${getErrorMessage(error, "Unable to continue Add All Data.")}`,
      );
    } finally {
      setIsBatchRunning(false);
      setBatchProgress((current) =>
        current
          ? {
              ...current,
              completedJobs,
              completedWeight,
            }
          : current,
      );
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
            <option value="1m">1m</option>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
            <option value="4h">4h</option>
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

        {showRange ? (
          <Field label="Quick range" className="md:col-span-2 xl:col-span-3">
            <div className="rounded-xl border border-white/10 bg-slate-950/40 px-3 py-3">
              <DateRangePresets onSelect={applyDayPreset} />
            </div>
          </Field>
        ) : null}
      </div>

      <div className="grid gap-4 border-t border-white/6 pt-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(380px,0.85fr)]">
        <div className="flex flex-col gap-3">
          <p className="text-sm leading-7 text-slate-400">
            Use incremental mode to top up the latest candles with overlap and dedupe. Initial and manual modes
            require an explicit range.
          </p>
          {message ? <div className="rounded-2xl border border-white/8 bg-white/5 px-4 py-3 text-sm text-slate-200">{message}</div> : null}
        </div>

        <div className="rounded-3xl border border-sky-400/15 bg-sky-400/5 p-4 shadow-[0_0_0_1px_rgba(125,211,252,0.04)]">
          <div className="flex flex-col gap-4">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <p className="text-[11px] uppercase tracking-[0.25em] text-sky-200/75">All Data Queue</p>
                <h3 className="text-base font-semibold text-white">Run every symbol overnight</h3>
                <p className="text-sm text-slate-400">Queue order: 4h → 1h → 15m → 5m → 1m, one symbol at a time.</p>
              </div>
              <button
                type="button"
                onClick={handleBatchSync}
                disabled={isRunning || mode === "incremental"}
                className="min-w-[160px] rounded-2xl border border-sky-300/30 bg-sky-300/15 px-4 py-3 text-sm font-semibold text-sky-50 transition hover:bg-sky-300/25 disabled:cursor-not-allowed disabled:border-slate-700 disabled:bg-slate-800 disabled:text-slate-500"
              >
                {isBatchRunning ? "Running all data..." : "Add All Data"}
              </button>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <span className="mr-1 text-[11px] uppercase tracking-[0.2em] text-slate-500">Days</span>
              {batchDayPresets.map((days) => (
                <button
                  key={days}
                  type="button"
                  onClick={() => applyDayPreset(days)}
                  disabled={isRunning}
                  className="rounded-full border border-white/10 bg-slate-950/50 px-3.5 py-1.5 text-sm font-medium text-slate-200 transition hover:border-sky-300/35 hover:bg-sky-300/10 hover:text-white disabled:cursor-not-allowed disabled:border-white/5 disabled:bg-slate-900 disabled:text-slate-600"
                >
                  {days}d
                </button>
              ))}
            </div>

            <div className="rounded-2xl border border-white/8 bg-slate-950/45 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Queue status</p>
              <p className="mt-2 text-sm leading-6 text-sky-100">
                {batchMessage ?? "Pick a day range and start Add All Data when you want the full queue to run."}
              </p>
              <p className="mt-2 text-sm text-slate-400">{isBatchRunning ? batchEtaText : `Range selected: ~${Math.round(selectedRangeDays)} days.`}</p>
            </div>

            <div className="flex justify-end">
              <button
                type="submit"
                disabled={isRunning}
                className="rounded-xl bg-emerald-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
              >
                {isRunning ? "Running..." : "Run sync"}
              </button>
            </div>
          </div>
        </div>
      </div>
    </form>
  );
}

function Field({
  label,
  children,
  className = "",
}: {
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <label className={`grid gap-2 ${className}`.trim()}>
      <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">{label}</span>
      {children}
    </label>
  );
}

const inputClassName =
  "h-11 rounded-xl border border-white/10 bg-slate-950/60 px-3 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-sky-400/40";
