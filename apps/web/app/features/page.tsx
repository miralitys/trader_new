"use client";

import { useMemo, useState } from "react";

import { SectionCard } from "@/components/section-card";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { longDayPresets, presetSymbols, presetTimeframes } from "@/lib/preset-symbols";
import { useFeatureCoverage, useFeatureRuns, useRunFeatureLayer } from "@/lib/query-hooks";
import type { FeatureCoverage, FeatureRun } from "@/lib/types";
import { formatDateTime, formatInteger, getErrorMessage } from "@/lib/utils";

const EMPTY_FEATURE_RUNS: FeatureRun[] = [];
const EMPTY_FEATURE_COVERAGE: FeatureCoverage[] = [];
const FEATURE_BATCH_TIMEFRAMES = ["4h", "1h", "15m", "5m", "1m"] as const;

type FeatureBatchState = {
  startedAt: number;
  totalJobs: number;
  completedJobs: number;
  currentIndex: number;
  currentSymbol: string | null;
  currentTimeframe: string | null;
  completedBySymbol: Record<string, number>;
  failedJobs: number;
};

export default function FeatureLayerPage() {
  const [lookbackDays, setLookbackDays] = useState(180);
  const [message, setMessage] = useState<string | null>(null);
  const [batchState, setBatchState] = useState<FeatureBatchState | null>(null);

  const featureRunsQuery = useFeatureRuns({ limit: 1000 });
  const featureCoverageQuery = useFeatureCoverage();
  const runFeatureMutation = useRunFeatureLayer();

  const runs = featureRunsQuery.data ?? EMPTY_FEATURE_RUNS;
  const coverageRows = featureCoverageQuery.data ?? EMPTY_FEATURE_COVERAGE;
  const averageRunDurationByKey = useMemo(() => {
    const grouped = new Map<string, number[]>();

    for (const run of runs) {
      if (run.status !== "completed") {
        continue;
      }
      const createdAtMs = new Date(run.created_at).getTime();
      const updatedAtMs = new Date(run.updated_at).getTime();
      if (!Number.isFinite(createdAtMs) || !Number.isFinite(updatedAtMs) || updatedAtMs <= createdAtMs) {
        continue;
      }
      const key = `${run.timeframe}:${run.lookback_days}`;
      const current = grouped.get(key) ?? [];
      current.push(updatedAtMs - createdAtMs);
      grouped.set(key, current);
    }

    const averages = new Map<string, number>();
    for (const [key, values] of grouped.entries()) {
      averages.set(key, values.reduce((sum, value) => sum + value, 0) / values.length);
    }
    return averages;
  }, [runs]);

  const runsBySymbol = useMemo(() => {
    return runs.reduce<Record<string, FeatureRun[]>>((accumulator, run) => {
      accumulator[run.symbol] = accumulator[run.symbol] ? [...accumulator[run.symbol], run] : [run];
      return accumulator;
    }, {});
  }, [runs]);

  const coverageByKey = useMemo(() => {
    return coverageRows.reduce<Record<string, FeatureCoverage>>((accumulator, row) => {
      accumulator[`${row.symbol}:${row.timeframe}`] = row;
      return accumulator;
    }, {});
  }, [coverageRows]);

  const stats = useMemo(() => {
    const completedRuns = runs.filter((run) => run.status === "completed");
    const failedRuns = runs.filter((run) => run.status === "failed");
    const totalRows = completedRuns.reduce((sum, run) => sum + run.feature_rows_upserted, 0);
    return {
      totalRuns: runs.length,
      completedRuns: completedRuns.length,
      failedRuns: failedRuns.length,
      totalRows,
    };
  }, [runs]);

  async function handleRun(symbol: string, timeframe: string) {
    setMessage(null);
    try {
      const result = await runFeatureMutation.mutateAsync({
        exchange_code: "binance_us",
        symbol,
        timeframe,
        lookback_days: lookbackDays,
      });
      setMessage(
        `${result.symbol} ${result.timeframe} completed. Saved ${formatInteger(result.feature_rows_upserted)} feature rows from ${formatInteger(result.source_candle_count)} source candles.`,
      );
    } catch (error) {
      setMessage(getErrorMessage(error, `Unable to build features for ${symbol} ${timeframe}.`));
    }
  }

  async function handleRunAll() {
    if (runFeatureMutation.isPending || batchState) {
      return;
    }

    setMessage(null);
    const queue = FEATURE_BATCH_TIMEFRAMES.flatMap((timeframe) =>
      presetSymbols.map((symbol) => ({
        symbol,
        timeframe,
      })),
    );
    const batchStartedAt = Date.now();

    const initialCompletedBySymbol = Object.fromEntries(presetSymbols.map((symbol) => [symbol, 0])) as Record<string, number>;

    setBatchState({
      startedAt: batchStartedAt,
      totalJobs: queue.length,
      completedJobs: 0,
      currentIndex: 0,
      currentSymbol: queue[0]?.symbol ?? null,
      currentTimeframe: queue[0]?.timeframe ?? null,
      completedBySymbol: initialCompletedBySymbol,
      failedJobs: 0,
    });

    let completedJobs = 0;
    let failedJobs = 0;
    const completedBySymbol = { ...initialCompletedBySymbol };

    for (let index = 0; index < queue.length; index += 1) {
      const item = queue[index];

      setBatchState({
        startedAt: batchStartedAt,
        totalJobs: queue.length,
        completedJobs,
        currentIndex: index + 1,
        currentSymbol: item.symbol,
        currentTimeframe: item.timeframe,
        completedBySymbol: { ...completedBySymbol },
        failedJobs,
      });

      try {
        await runFeatureMutation.mutateAsync({
          exchange_code: "binance_us",
          symbol: item.symbol,
          timeframe: item.timeframe,
          lookback_days: lookbackDays,
        });
      } catch {
        failedJobs += 1;
      } finally {
        completedJobs += 1;
        completedBySymbol[item.symbol] = (completedBySymbol[item.symbol] ?? 0) + 1;

        setBatchState({
          startedAt: batchStartedAt,
          totalJobs: queue.length,
          completedJobs,
          currentIndex: index + 1,
          currentSymbol: item.symbol,
          currentTimeframe: item.timeframe,
          completedBySymbol: { ...completedBySymbol },
          failedJobs,
        });
      }
    }

    setMessage(
      `Batch feature build finished. Completed ${completedJobs}/${queue.length} jobs with ${failedJobs} failed runs across ${presetSymbols.length} symbols.`,
    );
    setBatchState(null);
  }

  const batchPercent = batchState ? Math.round((batchState.completedJobs / Math.max(1, batchState.totalJobs)) * 100) : 0;
  const batchEtaText = useMemo(() => buildBatchEta(batchState), [batchState]);

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Feature Layer"
        title="Per-symbol feature generation"
        description="This page computes the first MVP feature layer on top of the two-year candle dataset. Every coin has its own block, every timeframe runs separately, and every run leaves a history trail so we can see exactly what has been generated."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Tracked symbols" value={formatInteger(presetSymbols.length)} hint="19 symbols in the active research basket" />
        <MetricCard label="Timeframes" value={formatInteger(presetTimeframes.length)} hint="1m, 5m, 15m, 1h, 4h" />
        <MetricCard label="Completed runs" value={formatInteger(stats.completedRuns)} hint={`${formatInteger(stats.totalRows)} feature rows saved`} tone={stats.completedRuns ? "positive" : "warning"} />
        <MetricCard label="Failed runs" value={formatInteger(stats.failedRuns)} hint="History stays visible per symbol" tone={stats.failedRuns ? "danger" : "default"} />
      </section>

      <SectionCard title="Run feature generation" eyebrow="Global run window">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
          <div className="max-w-3xl space-y-4">
            <p className="text-sm leading-7 text-slate-400">
              Feature generation calculates per-bar signals for returns, volatility, structure, trend, volume, and compression. Pick one global lookback window, then launch any symbol/timeframe independently.
            </p>
            <div className="flex flex-wrap gap-3">
              <button
                type="button"
                onClick={handleRunAll}
                disabled={runFeatureMutation.isPending || Boolean(batchState)}
                className="rounded-xl bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
              >
                {batchState ? "Запускаем все..." : "Запустить все"}
              </button>
              <div className="rounded-xl border border-white/8 bg-slate-950/45 px-4 py-2 text-sm text-slate-300">
                Queue order: 4h → 1h → 15m → 5m → 1m for every symbol
              </div>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <span className="mr-1 text-[11px] uppercase tracking-[0.2em] text-slate-500">Days</span>
            {longDayPresets.map((days) => (
              <button
                key={days}
                type="button"
                onClick={() => setLookbackDays(days)}
                className={`rounded-full border px-3.5 py-1.5 text-sm font-medium transition ${
                  lookbackDays === days
                    ? "border-emerald-300/45 bg-emerald-400/15 text-emerald-100"
                    : "border-white/10 bg-slate-950/50 text-slate-200 hover:border-emerald-300/30 hover:bg-emerald-400/10 hover:text-white"
                }`}
              >
                {days}d
              </button>
            ))}
          </div>
        </div>
        {batchState ? (
          <div className="mt-5 rounded-2xl border border-emerald-300/10 bg-emerald-300/5 px-4 py-4">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <p className="text-sm font-semibold text-white">
                  Running {batchState.currentIndex}/{batchState.totalJobs}: {batchState.currentSymbol} {batchState.currentTimeframe}
                </p>
                <p className="mt-1 text-sm text-slate-400">
                  Completed {batchState.completedJobs} jobs · Failed {batchState.failedJobs}
                </p>
                <p className="mt-1 text-sm text-slate-500">{batchEtaText}</p>
              </div>
              <p className="text-sm font-semibold text-emerald-100">{batchPercent}% complete</p>
            </div>
            <div className="mt-3 h-2 overflow-hidden rounded-full bg-slate-950/45">
              <div className="h-full rounded-full bg-emerald-300 transition-all duration-300" style={{ width: `${batchPercent}%` }} />
            </div>
          </div>
        ) : null}
        {message ? <div className="mt-5 rounded-2xl border border-white/8 bg-white/5 px-4 py-3 text-sm text-slate-200">{message}</div> : null}
      </SectionCard>

      <div className="grid gap-5">
        {presetSymbols.map((symbol) => {
          const symbolRuns = (runsBySymbol[symbol] ?? []).slice(0, 8);
          const symbolBatchCompleted = batchState?.completedBySymbol[symbol] ?? 0;
          const symbolBatchPercent = Math.round((symbolBatchCompleted / FEATURE_BATCH_TIMEFRAMES.length) * 100);
          const symbolIsCurrent = batchState?.currentSymbol === symbol;

          return (
            <SectionCard key={symbol} title={symbol} eyebrow="Independent feature build" className="h-full">
              <div className="flex flex-col gap-5">
                {batchState ? (
                  <div className="rounded-2xl border border-white/8 bg-slate-950/35 px-4 py-4">
                    <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                      <div>
                        <p className="text-sm font-semibold text-white">
                          {symbolIsCurrent
                            ? `Running now · ${batchState.currentTimeframe}`
                            : `${symbolBatchCompleted}/${FEATURE_BATCH_TIMEFRAMES.length} timeframes complete`}
                        </p>
                        <p className="mt-1 text-sm text-slate-400">
                          Queue order for this symbol: {FEATURE_BATCH_TIMEFRAMES.join(" → ")}
                        </p>
                      </div>
                      <p className="text-sm font-semibold text-slate-200">{symbolBatchPercent}%</p>
                    </div>
                    <div className="mt-3 h-2 overflow-hidden rounded-full bg-slate-950/45">
                      <div
                        className={`h-full rounded-full transition-all duration-300 ${symbolIsCurrent ? "bg-sky-300" : "bg-emerald-300"}`}
                        style={{ width: `${symbolBatchPercent}%` }}
                      />
                    </div>
                  </div>
                ) : null}

                <div className="grid gap-3">
                  {FEATURE_BATCH_TIMEFRAMES.map((timeframe) => {
                    const coverage = coverageByKey[`${symbol}:${timeframe}`];
                    const isRunningThisButton =
                      (runFeatureMutation.isPending &&
                        runFeatureMutation.variables?.symbol === symbol &&
                        runFeatureMutation.variables?.timeframe === timeframe) ||
                      (batchState?.currentSymbol === symbol && batchState?.currentTimeframe === timeframe);

                    return (
                      <button
                        key={timeframe}
                        type="button"
                        onClick={() => handleRun(symbol, timeframe)}
                        disabled={runFeatureMutation.isPending || Boolean(batchState)}
                        className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-4 text-left transition hover:border-sky-300/30 hover:bg-sky-400/5 disabled:cursor-not-allowed disabled:border-white/5 disabled:bg-slate-950/30"
                      >
                        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                          <div className="grid gap-3 lg:min-w-0 lg:flex-1 lg:grid-cols-[120px_minmax(0,1fr)_auto] lg:items-center">
                            <div className="flex items-center gap-3">
                              <span className="text-lg font-semibold text-white">{timeframe}</span>
                              {coverage ? (
                                <StatusBadge status={coverage.feature_count > 0 ? "completed" : "idle"} />
                              ) : (
                                <StatusBadge status="idle" />
                              )}
                            </div>

                            <div className="grid gap-1 text-sm text-slate-300 sm:grid-cols-2 xl:grid-cols-3">
                              <span>{coverage ? `${formatInteger(coverage.feature_count)} rows` : "No feature rows yet"}</span>
                              <span className="text-slate-400">
                                {coverage?.loaded_end_at ? `Latest: ${formatDateTime(coverage.loaded_end_at)}` : "No completed feature window"}
                              </span>
                              <span className="text-slate-500">
                                {coverage?.loaded_start_at ? `Start: ${formatDateTime(coverage.loaded_start_at)}` : "Runs this timeframe only for this coin"}
                              </span>
                            </div>

                            <div className="lg:justify-self-end">
                              <span className="inline-flex rounded-xl bg-sky-400/10 px-3 py-2 text-sm font-medium text-sky-100">
                                {isRunningThisButton ? "Running..." : `Build ${lookbackDays}d`}
                              </span>
                            </div>
                          </div>
                        </div>
                      </button>
                    );
                  })}
                </div>

                <div className="rounded-2xl border border-white/8 bg-slate-950/35 px-4 py-4">
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Run history</p>
                    <p className="text-xs text-slate-500">Newest first</p>
                  </div>

                  {symbolRuns.length ? (
                    <div className="grid gap-3">
                      {symbolRuns.map((run) => (
                        <div key={run.id} className="grid gap-2 rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                          <div className="flex flex-wrap items-center justify-between gap-3">
                            <div className="flex items-center gap-3">
                              <span className="text-sm font-semibold text-white">{run.timeframe}</span>
                              <StatusBadge status={run.status} />
                            </div>
                            <span className="text-xs text-slate-400">{formatDateTime(run.updated_at)}</span>
                          </div>
                          <div className="grid gap-1 text-sm text-slate-300 md:grid-cols-2">
                            <span>{formatInteger(run.feature_rows_upserted)} feature rows</span>
                            <span>{formatInteger(run.source_candle_count)} source candles</span>
                            <span>{run.lookback_days}d window</span>
                            <span>
                              {run.computed_end_at ? `Computed through ${formatDateTime(run.computed_end_at)}` : "No completed feature window"}
                            </span>
                          </div>
                          {run.status === "running" ? (
                            <p className="text-sm text-sky-200">
                              {buildFeatureRunEta(run, averageRunDurationByKey)}
                            </p>
                          ) : null}
                          {run.error_text ? <p className="text-sm text-rose-200">{run.error_text}</p> : null}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-slate-400">No feature runs for {symbol} yet. Use one of the timeframe buttons above.</p>
                  )}
                </div>
              </div>
            </SectionCard>
          );
        })}
      </div>
    </div>
  );
}

function buildBatchEta(batchState: FeatureBatchState | null) {
  if (!batchState) {
    return null;
  }

  if (batchState.completedJobs <= 0) {
    return "ETA will appear after the first completed job.";
  }

  const elapsedMs = Date.now() - batchState.startedAt;
  if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) {
    return "ETA unavailable.";
  }

  const msPerJob = elapsedMs / batchState.completedJobs;
  const remainingJobs = Math.max(batchState.totalJobs - batchState.completedJobs, 0);
  const remainingMs = msPerJob * remainingJobs;
  return formatEta(remainingMs);
}

function buildFeatureRunEta(run: FeatureRun, averageRunDurationByKey: Map<string, number>) {
  const createdAtMs = new Date(run.created_at).getTime();
  if (!Number.isFinite(createdAtMs)) {
    return "ETA unavailable.";
  }

  const elapsedMs = Date.now() - createdAtMs;
  const key = `${run.timeframe}:${run.lookback_days}`;
  const expectedMs = averageRunDurationByKey.get(key) ?? estimateFeatureRunDurationMs(run.timeframe, run.lookback_days);

  if (!Number.isFinite(expectedMs) || expectedMs <= 0) {
    return "ETA unavailable.";
  }

  const remainingMs = Math.max(expectedMs - elapsedMs, 0);
  if (remainingMs <= 0) {
    return "Finalizing run...";
  }

  return `ETA ${formatEta(remainingMs)}`;
}

function estimateFeatureRunDurationMs(timeframe: string, lookbackDays: number) {
  const msPerDay: Record<string, number> = {
    "4h": 350,
    "1h": 1200,
    "15m": 1800,
    "5m": 3500,
    "1m": 12000,
  };
  return (msPerDay[timeframe] ?? 1500) * lookbackDays;
}

function formatEta(durationMs: number) {
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return "Finalizing...";
  }

  const minutes = Math.round(durationMs / 60000);
  if (minutes < 1) {
    return "~< 1 min remaining.";
  }
  if (minutes < 60) {
    return `~${minutes} min remaining.`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `~${hours}h ${remainingMinutes}m remaining.`;
}
