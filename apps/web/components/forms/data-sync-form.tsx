"use client";

import { FormEvent, useMemo, useState } from "react";

import { DateRangePresets } from "@/components/forms/date-range-presets";
import { longDayPresets, presetSymbols } from "@/lib/preset-symbols";
import { useRunDataSync } from "@/lib/query-hooks";
import { formatInteger, formatPercent, getErrorMessage, toDatetimeLocalInput } from "@/lib/utils";

const batchTimeframes = ["4h", "1h", "15m", "5m", "1m"] as const;
const batchDayPresets = longDayPresets;
const candlesPerDayByTimeframe: Record<(typeof batchTimeframes)[number], number> = {
  "4h": 6,
  "1h": 24,
  "15m": 96,
  "5m": 288,
  "1m": 1440,
};
const batchRetryAttempts = 3;

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
  const [batchLookbackDays, setBatchLookbackDays] = useState<number>(720);
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

  const batchRange = useMemo(() => {
    const nextEnd = new Date();
    const nextStart = new Date(nextEnd.getTime() - batchLookbackDays * 24 * 60 * 60 * 1000);
    return {
      startAt: toDatetimeLocalInput(nextStart),
      endAt: toDatetimeLocalInput(nextEnd),
    };
  }, [batchLookbackDays]);

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
          return timeframeAcc + batchLookbackDays * candlesPerDayByTimeframe[orderedTimeframe];
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
              `for ${batchRange.startAt} -> ${batchRange.endAt}`,
          );

          const result = await runBatchSyncJobWithRetry({
            symbol: orderedSymbol,
            timeframe: orderedTimeframe,
            startAtIso: new Date(batchRange.startAt).toISOString(),
            endAtIso: new Date(batchRange.endAt).toISOString(),
            mode,
            onRetry: (attempt, totalAttempts, errorText) => {
              setBatchMessage(
                `Retry ${attempt}/${totalAttempts} for ${orderedSymbol} ${orderedTimeframe}. ${errorText}`,
              );
            },
          });

          completedJobs += 1;
          totalInsertedRows += result.inserted_rows;
          completedWeight += batchLookbackDays * candlesPerDayByTimeframe[orderedTimeframe];
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
        `Batch stopped on job ${completedJobs + 1}/${totalJobs}. ${getErrorMessage(
          error,
          "Unable to continue Add All Data. The backend job may still have completed, so check the latest sync status before restarting.",
        )}`,
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

  async function runBatchSyncJobWithRetry({
    symbol,
    timeframe,
    startAtIso,
    endAtIso,
    mode,
    onRetry,
  }: {
    symbol: string;
    timeframe: (typeof batchTimeframes)[number];
    startAtIso: string;
    endAtIso: string;
    mode: "initial" | "incremental" | "manual";
    onRetry: (attempt: number, totalAttempts: number, errorText: string) => void;
  }) {
    let lastError: unknown;

    for (let attempt = 1; attempt <= batchRetryAttempts; attempt += 1) {
      try {
        return await syncMutation.mutateAsync({
          mode,
          exchange_code: "binance_us",
          symbol,
          timeframe,
          start_at: startAtIso,
          end_at: endAtIso,
        });
      } catch (error) {
        lastError = error;
        const isLastAttempt = attempt === batchRetryAttempts;
        if (!isRetryableBatchError(error) || isLastAttempt) {
          throw error;
        }

        onRetry(attempt, batchRetryAttempts - 1, getErrorMessage(error, "Temporary network error."));
        await sleep(attempt * 1500);
      }
    }

    throw lastError ?? new Error("Unknown batch sync error.");
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

      <div className="border-t border-white/6 pt-4">
        <div className="rounded-3xl border border-sky-400/15 bg-sky-400/5 p-4 shadow-[0_0_0_1px_rgba(125,211,252,0.04)]">
          <div className="flex flex-col gap-5">
            <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto] xl:items-start">
              <div className="space-y-2">
                <p className="text-sm leading-7 text-slate-400">
                  Use incremental mode to top up the latest candles with overlap and dedupe. Initial and manual modes
                  require an explicit range.
                </p>
                <div className="space-y-1">
                  <p className="text-[11px] uppercase tracking-[0.25em] text-sky-200/75">All Data Queue</p>
                  <h3 className="text-base font-semibold text-white">Run every symbol overnight</h3>
                  <p className="text-sm text-slate-400">
                    Queue order: 4h → 1h → 15m → 5m → 1m, one symbol at a time.
                  </p>
                </div>
              </div>

              <div className="flex flex-wrap gap-3 xl:justify-end">
                <button
                  type="button"
                  onClick={handleBatchSync}
                  disabled={isRunning || mode === "incremental"}
                  className="min-w-[180px] rounded-2xl border border-sky-300/30 bg-sky-300/15 px-4 py-3 text-sm font-semibold text-sky-50 transition hover:bg-sky-300/25 disabled:cursor-not-allowed disabled:border-slate-700 disabled:bg-slate-800 disabled:text-slate-500"
                >
                  {isBatchRunning ? "Running all data..." : "Add All Data"}
                </button>

                <button
                  type="submit"
                  disabled={isRunning}
                  className="rounded-xl bg-emerald-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
                >
                  {isRunning ? "Running..." : "Run sync"}
                </button>
              </div>
            </div>

            <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(320px,0.9fr)]">
              <div className="flex flex-wrap items-center gap-2">
                <span className="mr-1 text-[11px] uppercase tracking-[0.2em] text-slate-500">Days</span>
                {batchDayPresets.map((days) => (
                  <button
                    key={days}
                    type="button"
                    onClick={() => setBatchLookbackDays(days)}
                    disabled={isRunning}
                    className={`rounded-full border px-3.5 py-1.5 text-sm font-medium transition disabled:cursor-not-allowed disabled:border-white/5 disabled:bg-slate-900 disabled:text-slate-600 ${
                      batchLookbackDays === days
                        ? "border-emerald-300/45 bg-emerald-400/15 text-emerald-100"
                        : "border-white/10 bg-slate-950/50 text-slate-200 hover:border-sky-300/35 hover:bg-sky-300/10 hover:text-white"
                    }`}
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
                <p className="mt-2 text-sm text-slate-300">
                  Batch range: {batchRange.startAt} → {batchRange.endAt} ({batchLookbackDays}d)
                </p>
                <p className="mt-2 text-sm text-slate-400">
                  {isBatchRunning ? batchEtaText : `Queue prepared for ~${batchLookbackDays} days.`}
                </p>
              </div>
            </div>

            {message ? (
              <div className="rounded-2xl border border-white/8 bg-white/5 px-4 py-3 text-sm text-slate-200">
                {message}
              </div>
            ) : null}
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

function isRetryableBatchError(error: unknown) {
  const message = getErrorMessage(error, "").toLowerCase();
  return (
    message.includes("failed to fetch") ||
    message.includes("networkerror") ||
    message.includes("network request failed") ||
    message.includes("load failed") ||
    message.includes("timeout") ||
    message.includes("tempor")
  );
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
