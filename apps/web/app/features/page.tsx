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

export default function FeatureLayerPage() {
  const [lookbackDays, setLookbackDays] = useState(180);
  const [message, setMessage] = useState<string | null>(null);

  const featureRunsQuery = useFeatureRuns({ limit: 1000 });
  const featureCoverageQuery = useFeatureCoverage();
  const runFeatureMutation = useRunFeatureLayer();

  const runs = featureRunsQuery.data ?? EMPTY_FEATURE_RUNS;
  const coverageRows = featureCoverageQuery.data ?? EMPTY_FEATURE_COVERAGE;

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
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="max-w-3xl">
            <p className="text-sm leading-7 text-slate-400">
              Feature generation calculates per-bar signals for returns, volatility, structure, trend, volume, and compression. Pick one global lookback window, then launch any symbol/timeframe independently.
            </p>
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
        {message ? <div className="mt-5 rounded-2xl border border-white/8 bg-white/5 px-4 py-3 text-sm text-slate-200">{message}</div> : null}
      </SectionCard>

      <div className="grid gap-5">
        {presetSymbols.map((symbol) => {
          const symbolRuns = (runsBySymbol[symbol] ?? []).slice(0, 8);

          return (
            <SectionCard key={symbol} title={symbol} eyebrow="Independent feature build" className="h-full">
              <div className="flex flex-col gap-5">
                <div className="grid gap-3">
                  {presetTimeframes.map((timeframe) => {
                    const coverage = coverageByKey[`${symbol}:${timeframe}`];
                    const isRunningThisButton =
                      runFeatureMutation.isPending &&
                      runFeatureMutation.variables?.symbol === symbol &&
                      runFeatureMutation.variables?.timeframe === timeframe;

                    return (
                      <button
                        key={timeframe}
                        type="button"
                        onClick={() => handleRun(symbol, timeframe)}
                        disabled={runFeatureMutation.isPending}
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
