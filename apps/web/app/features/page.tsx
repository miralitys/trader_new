"use client";

import { useMemo, useState } from "react";

import { SectionCard } from "@/components/section-card";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { longDayPresets, presetSymbols } from "@/lib/preset-symbols";
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
  const [selectedSymbol, setSelectedSymbol] = useState<string>(presetSymbols[0]);
  const [message, setMessage] = useState<string | null>(null);
  const [batchState, setBatchState] = useState<FeatureBatchState | null>(null);

  const featureRunsQuery = useFeatureRuns({ limit: 1000 });
  const featureCoverageQuery = useFeatureCoverage({ symbol: selectedSymbol });
  const runFeatureMutation = useRunFeatureLayer();

  const runs = featureRunsQuery.data ?? EMPTY_FEATURE_RUNS;
  const coverageRows = featureCoverageQuery.data ?? EMPTY_FEATURE_COVERAGE;

  const runsBySymbol = useMemo(() => {
    return runs.reduce<Record<string, FeatureRun[]>>((accumulator, run) => {
      accumulator[run.symbol] = accumulator[run.symbol] ? [...accumulator[run.symbol], run] : [run];
      return accumulator;
    }, {});
  }, [runs]);

  const selectedCoverageByKey = useMemo(() => {
    return coverageRows.reduce<Record<string, FeatureCoverage>>((accumulator, row) => {
      accumulator[`${row.symbol}:${row.timeframe}`] = row;
      return accumulator;
    }, {});
  }, [coverageRows]);

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

  const stats = useMemo(() => {
    const completedRuns = runs.filter((run) => run.status === "completed");
    const failedRuns = runs.filter((run) => run.status === "failed");
    const runningRuns = runs.filter((run) => run.status === "running");
    const queuedRuns = runs.filter((run) => run.status === "queued");
    const totalRows = completedRuns.reduce((sum, run) => sum + run.feature_rows_upserted, 0);
    return {
      totalRuns: runs.length,
      completedRuns: completedRuns.length,
      failedRuns: failedRuns.length,
      runningRuns: runningRuns.length,
      queuedRuns: queuedRuns.length,
      totalRows,
    };
  }, [runs]);

  const runningRun = useMemo(() => {
    return runs.find((run) => run.status === "running") ?? null;
  }, [runs]);

  const queuedRuns = useMemo(() => {
    return runs
      .filter((run) => run.status === "queued")
      .sort((left, right) => new Date(left.created_at).getTime() - new Date(right.created_at).getTime());
  }, [runs]);

  const nextQueuedRun = queuedRuns[0] ?? null;
  const workerQueueEtaText = useMemo(
    () => buildWorkerQueueEta(runningRun, queuedRuns, averageRunDurationByKey),
    [runningRun, queuedRuns, averageRunDurationByKey],
  );

  const symbolMeta = useMemo(() => {
    return presetSymbols.map((symbol) => {
      const symbolRuns = runsBySymbol[symbol] ?? [];
      const completed = symbolRuns.filter((run) => run.status === "completed").length;
      const failed = symbolRuns.filter((run) => run.status === "failed").length;
      const running = symbolRuns.find((run) => run.status === "running") ?? null;
      const lastRun = symbolRuns[0] ?? null;
      const latestCompletedByTimeframe = new Map<string, FeatureRun>();
      for (const run of symbolRuns) {
        if (run.status !== "completed" || run.feature_rows_upserted <= 0) {
          continue;
        }
        if (!latestCompletedByTimeframe.has(run.timeframe)) {
          latestCompletedByTimeframe.set(run.timeframe, run);
        }
      }
      const populatedTimeframes = latestCompletedByTimeframe.size;

      return {
        symbol,
        completed,
        failed,
        running,
        lastRun,
        populatedTimeframes,
      };
    });
  }, [runsBySymbol]);

  const selectedRuns = (runsBySymbol[selectedSymbol] ?? []).slice(0, 10);
  const selectedSymbolMeta = symbolMeta.find((item) => item.symbol === selectedSymbol) ?? null;
  const selectedCoverageRows = FEATURE_BATCH_TIMEFRAMES.map((timeframe) => ({
    timeframe,
    coverage: selectedCoverageByKey[`${selectedSymbol}:${timeframe}`] ?? null,
  }));

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
        `${result.symbol} ${result.timeframe} queued as feature run #${result.id}. Worker will build it in the background.`,
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

    let queuedJobs = 0;
    let failedJobs = 0;
    const completedBySymbol = { ...initialCompletedBySymbol };

    for (let index = 0; index < queue.length; index += 1) {
      const item = queue[index];

      setBatchState({
        startedAt: batchStartedAt,
        totalJobs: queue.length,
        completedJobs: queuedJobs,
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
        queuedJobs += 1;
      } catch {
        failedJobs += 1;
      } finally {
        completedBySymbol[item.symbol] = (completedBySymbol[item.symbol] ?? 0) + 1;

        setBatchState({
          startedAt: batchStartedAt,
          totalJobs: queue.length,
          completedJobs: queuedJobs,
          currentIndex: index + 1,
          currentSymbol: item.symbol,
          currentTimeframe: item.timeframe,
          completedBySymbol: { ...completedBySymbol },
          failedJobs,
        });
      }
    }

    setMessage(
      `Batch feature queue finished. Queued ${queuedJobs}/${queue.length} jobs with ${failedJobs} submission failures across ${presetSymbols.length} symbols.`,
    );
    setBatchState(null);
  }

  const batchPercent = batchState ? Math.round((batchState.completedJobs / Math.max(1, batchState.totalJobs)) * 100) : 0;
  const batchEtaText = useMemo(() => buildBatchEta(batchState), [batchState]);

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Feature Layer"
        title="Feature Workspace"
        description="Use one compact workspace to run batch feature generation, inspect symbol coverage, and review recent builds without scrolling through a long operations wall."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricCard label="Tracked symbols" value={formatInteger(presetSymbols.length)} hint="19 symbols in the active research basket" />
        <MetricCard label="Timeframes" value={formatInteger(FEATURE_BATCH_TIMEFRAMES.length)} hint="4h, 1h, 15m, 5m, 1m" />
        <MetricCard
          label="Completed runs"
          value={formatInteger(stats.completedRuns)}
          hint={`${formatInteger(stats.totalRows)} feature rows saved`}
          tone={stats.completedRuns ? "positive" : "warning"}
        />
        <MetricCard
          label="Running / failed"
          value={`${formatInteger(stats.runningRuns)} / ${formatInteger(stats.failedRuns)}`}
          hint="Live workspace status"
          tone={stats.failedRuns ? "danger" : stats.runningRuns ? "warning" : "default"}
        />
        <MetricCard
          label="Queued jobs"
          value={formatInteger(stats.queuedRuns)}
          hint={
            nextQueuedRun
              ? `${nextQueuedRun.symbol} · ${nextQueuedRun.timeframe} next · ${workerQueueEtaText}`
              : "No worker backlog"
          }
          tone={stats.queuedRuns ? "warning" : "default"}
        />
      </section>

      <SectionCard title="Control center" eyebrow="Global feature build">
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_minmax(360px,0.8fr)]">
          <div className="space-y-5">
            <p className="max-w-3xl text-sm leading-7 text-slate-400">
              Generate feature rows for one market or queue the full basket in research order. The queue always runs larger bars first so you get usable higher-timeframe features earlier and the heavier 1m work last.
            </p>

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

            <div className="grid gap-3 sm:grid-cols-2">
              <button
                type="button"
                onClick={handleRunAll}
                disabled={runFeatureMutation.isPending || Boolean(batchState)}
                className="rounded-2xl bg-emerald-300 px-4 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
              >
                {batchState ? "Ставим в очередь..." : "Запустить все"}
              </button>
              <div className="rounded-2xl border border-white/8 bg-slate-950/45 px-4 py-3 text-sm text-slate-300">
                Queue order: {FEATURE_BATCH_TIMEFRAMES.join(" → ")} across all symbols
              </div>
            </div>

            {batchState ? (
              <div className="rounded-2xl border border-emerald-300/10 bg-emerald-300/5 px-4 py-4">
                <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                  <div>
                    <p className="text-sm font-semibold text-white">
                      Queueing {batchState.currentIndex}/{batchState.totalJobs}: {batchState.currentSymbol} {batchState.currentTimeframe}
                    </p>
                    <p className="mt-1 text-sm text-slate-400">
                      Queued {batchState.completedJobs} jobs · Failed {batchState.failedJobs}
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

            {message ? (
              <div className="rounded-2xl border border-white/8 bg-white/5 px-4 py-3 text-sm text-slate-200">
                {message}
              </div>
            ) : null}
          </div>

          <div className="rounded-3xl border border-white/8 bg-slate-950/35 p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Workspace snapshot</p>
            <div className="mt-4 grid gap-3">
              <WorkspaceStat label="Current symbol" value={batchState?.currentSymbol ?? runningRun?.symbol ?? selectedSymbol} />
              <WorkspaceStat label="Current timeframe" value={batchState?.currentTimeframe ?? runningRun?.timeframe ?? "—"} />
              <WorkspaceStat label="Selected window" value={`${lookbackDays}d`} />
              <WorkspaceStat
                label="Next in queue"
                value={
                  nextQueuedRun
                    ? `${nextQueuedRun.symbol} · ${nextQueuedRun.timeframe} · ${nextQueuedRun.lookback_days}d`
                    : "Nothing waiting in the worker queue"
                }
              />
              <WorkspaceStat label="Queue ETA" value={workerQueueEtaText} subdued />
              <WorkspaceStat label="Most recent message" value={message ?? "No recent queue messages"} subdued />
            </div>
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Symbol navigator" eyebrow="One active market at a time">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {symbolMeta.map((item) => {
            const isActive = item.symbol === selectedSymbol;
            const batchProgress = batchState ? Math.round(((batchState.completedBySymbol[item.symbol] ?? 0) / FEATURE_BATCH_TIMEFRAMES.length) * 100) : null;
            return (
              <button
                key={item.symbol}
                type="button"
                onClick={() => setSelectedSymbol(item.symbol)}
                className={`rounded-2xl border px-4 py-4 text-left transition ${
                  isActive
                    ? "border-sky-300/35 bg-sky-400/10"
                    : "border-white/8 bg-slate-950/35 hover:border-white/15 hover:bg-white/[0.04]"
                }`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-base font-semibold text-white">{item.symbol}</p>
                    <p className="mt-1 text-sm text-slate-400">
                      {item.populatedTimeframes}/{FEATURE_BATCH_TIMEFRAMES.length} timeframes populated
                    </p>
                  </div>
                  <StatusBadge
                    status={item.running ? "running" : item.failed ? "failed" : item.completed ? "completed" : "idle"}
                  />
                </div>

                <div className="mt-4 grid gap-1 text-sm text-slate-300 sm:grid-cols-3">
                  <span>{formatInteger(item.completed)} completed</span>
                  <span>{formatInteger(item.failed)} failed</span>
                  <span>{item.lastRun ? formatDateTime(item.lastRun.updated_at) : "No runs yet"}</span>
                </div>

                {batchProgress !== null ? (
                  <div className="mt-4">
                    <div className="mb-2 flex items-center justify-between text-xs text-slate-400">
                      <span>Batch progress</span>
                      <span>{batchProgress}%</span>
                    </div>
                    <div className="h-2 overflow-hidden rounded-full bg-slate-950/45">
                      <div
                        className={`h-full rounded-full transition-all duration-300 ${batchState?.currentSymbol === item.symbol ? "bg-sky-300" : "bg-emerald-300"}`}
                        style={{ width: `${batchProgress}%` }}
                      />
                    </div>
                  </div>
                ) : null}
              </button>
            );
          })}
        </div>
      </SectionCard>

      <SectionCard title={selectedSymbol} eyebrow="Active symbol workspace">
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
          <div className="space-y-4">
            {selectedCoverageRows.map(({ timeframe, coverage }) => {
              const isQueuedThisRow = selectedRuns.find((run) => run.timeframe === timeframe && run.status === "queued") ?? null;
              const isRunningThisRow =
                (runFeatureMutation.isPending &&
                  runFeatureMutation.variables?.symbol === selectedSymbol &&
                  runFeatureMutation.variables?.timeframe === timeframe) ||
                (batchState?.currentSymbol === selectedSymbol && batchState?.currentTimeframe === timeframe);

              const runningRunForRow = selectedRuns.find((run) => run.timeframe === timeframe && run.status === "running") ?? null;

              return (
                <button
                  key={timeframe}
                  type="button"
                  onClick={() => handleRun(selectedSymbol, timeframe)}
                  disabled={runFeatureMutation.isPending || Boolean(batchState)}
                  className="w-full rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-4 text-left transition hover:border-sky-300/30 hover:bg-sky-400/5 disabled:cursor-not-allowed disabled:border-white/5 disabled:bg-slate-950/30"
                >
                  <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div className="grid gap-3 lg:min-w-0 lg:flex-1 lg:grid-cols-[110px_minmax(0,1fr)]">
                      <div className="flex items-center gap-3">
                        <span className="text-lg font-semibold text-white">{timeframe}</span>
                        <StatusBadge
                          status={
                            isRunningThisRow
                              ? "running"
                              : isQueuedThisRow
                                ? "queued"
                                : coverage && coverage.feature_count > 0
                                  ? "completed"
                                  : "idle"
                          }
                        />
                      </div>

                      <div className="grid gap-2 text-sm text-slate-300 md:grid-cols-2 xl:grid-cols-4">
                        <span>{coverage ? `${formatInteger(coverage.feature_count)} rows` : "No feature rows yet"}</span>
                        <span className="text-slate-400">
                          {coverage?.loaded_end_at ? `Latest: ${formatDateTime(coverage.loaded_end_at)}` : "No completed feature window"}
                        </span>
                        <span className="text-slate-500">
                          {coverage?.loaded_start_at ? `Start: ${formatDateTime(coverage.loaded_start_at)}` : "Runs this timeframe only for this coin"}
                        </span>
                        <span className="text-sky-100">
                          {isRunningThisRow ? "Running now" : isQueuedThisRow ? "Queued in worker" : `Build ${lookbackDays}d`}
                        </span>
                      </div>
                    </div>
                  </div>

                  {isRunningThisRow ? (
                    <div className="mt-4 rounded-xl border border-sky-400/15 bg-sky-400/10 px-3 py-3 text-sm text-sky-100">
                      {buildFeatureRunEta(
                        runningRunForRow ?? {
                          id: -1,
                          exchange: "binance_us",
                          symbol: selectedSymbol,
                          timeframe,
                          lookback_days: lookbackDays,
                          start_at: null,
                          end_at: null,
                          status: "running",
                          source_candle_count: 0,
                          feature_rows_upserted: 0,
                          computed_start_at: null,
                          computed_end_at: null,
                          error_text: null,
                          created_at: new Date().toISOString(),
                          updated_at: new Date().toISOString(),
                        },
                        averageRunDurationByKey,
                      )}
                    </div>
                  ) : isQueuedThisRow ? (
                    <div className="mt-4 rounded-xl border border-white/10 bg-white/[0.04] px-3 py-3 text-sm text-slate-200">
                      Waiting in the worker queue. It will start automatically after earlier jobs finish.
                    </div>
                  ) : null}
                </button>
              );
            })}
          </div>

          <div className="rounded-3xl border border-white/8 bg-slate-950/35 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Run history</p>
                <h4 className="mt-1 text-base font-semibold text-white">Newest first</h4>
              </div>
              {selectedSymbolMeta ? (
                <StatusBadge
                  status={
                    selectedSymbolMeta.running
                      ? "running"
                      : selectedSymbolMeta.failed
                        ? "failed"
                        : selectedSymbolMeta.completed
                          ? "completed"
                          : "idle"
                  }
                />
              ) : null}
            </div>

            <div className="mt-4 space-y-3">
              {selectedRuns.length ? (
                selectedRuns.map((run) => (
                  <div key={run.id} className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div className="flex items-center gap-3">
                        <span className="text-sm font-semibold text-white">{run.timeframe}</span>
                        <StatusBadge status={run.status} />
                      </div>
                      <span className="text-xs text-slate-400">{formatDateTime(run.updated_at)}</span>
                    </div>

                    <div className="mt-3 grid gap-2 text-sm text-slate-300">
                      <span>{formatInteger(run.feature_rows_upserted)} feature rows</span>
                      <span>{formatInteger(run.source_candle_count)} source candles</span>
                      <span>{run.lookback_days}d window</span>
                      <span>
                        {run.computed_end_at ? `Computed through ${formatDateTime(run.computed_end_at)}` : "No completed feature window"}
                      </span>
                      {run.status === "running" ? (
                        <span className="text-sky-200">{buildFeatureRunEta(run, averageRunDurationByKey)}</span>
                      ) : null}
                    </div>

                    {run.error_text ? <p className="mt-3 text-sm text-rose-200">{run.error_text}</p> : null}
                  </div>
                ))
              ) : (
                <p className="text-sm text-slate-400">No feature runs for {selectedSymbol} yet. Use the rows on the left to start building.</p>
              )}
            </div>
          </div>
        </div>
      </SectionCard>
    </div>
  );
}

function WorkspaceStat({
  label,
  value,
  subdued = false,
}: {
  label: string;
  value: string;
  subdued?: boolean;
}) {
  return (
    <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
      <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">{label}</p>
      <p className={`mt-2 text-sm leading-6 ${subdued ? "text-slate-400" : "text-white"}`}>{value}</p>
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

function buildWorkerQueueEta(
  runningRun: FeatureRun | null,
  queuedRuns: FeatureRun[],
  averageRunDurationByKey: Map<string, number>,
) {
  const remainingDurations: number[] = [];

  if (runningRun) {
    remainingDurations.push(resolveRemainingRunDurationMs(runningRun, averageRunDurationByKey));
  }

  for (const run of queuedRuns) {
    remainingDurations.push(resolveExpectedRunDurationMs(run, averageRunDurationByKey));
  }

  const totalRemainingMs = remainingDurations.reduce((sum, value) => sum + value, 0);
  if (totalRemainingMs <= 0) {
    return "No queued work remaining.";
  }
  return formatEta(totalRemainingMs);
}

function resolveRemainingRunDurationMs(run: FeatureRun, averageRunDurationByKey: Map<string, number>) {
  const createdAtMs = new Date(run.created_at).getTime();
  const expectedMs = resolveExpectedRunDurationMs(run, averageRunDurationByKey);
  if (!Number.isFinite(createdAtMs) || !Number.isFinite(expectedMs) || expectedMs <= 0) {
    return 0;
  }
  const elapsedMs = Date.now() - createdAtMs;
  return Math.max(expectedMs - elapsedMs, 0);
}

function resolveExpectedRunDurationMs(run: FeatureRun, averageRunDurationByKey: Map<string, number>) {
  const key = `${run.timeframe}:${run.lookback_days}`;
  return averageRunDurationByKey.get(key) ?? estimateFeatureRunDurationMs(run.timeframe, run.lookback_days);
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
