"use client";

import { useState } from "react";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { longDayPresets, presetSymbols, presetTimeframes } from "@/lib/preset-symbols";
import { usePatternScans, useResearchSummary, useStartPatternScan } from "@/lib/query-hooks";
import type { PatternScanRun, ResearchSummary } from "@/lib/types";
import { formatDateTime, formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

const forwardPresets = [6, 12, 24] as const;

export default function PatternsPage() {
  const [lookbackDays, setLookbackDays] = useState(720);
  const [forwardBars, setForwardBars] = useState(12);
  const [maxBarsPerSeries, setMaxBarsPerSeries] = useState(5000);
  const [message, setMessage] = useState<string | null>(null);

  const runsQuery = usePatternScans(20, true);
  const summaryQuery = useResearchSummary();
  const startMutation = useStartPatternScan();

  if ((runsQuery.isLoading || summaryQuery.isLoading) && !runsQuery.data && !summaryQuery.data) {
    return <LoadingState label="Loading pattern workspace..." />;
  }

  const error = runsQuery.error ?? summaryQuery.error;
  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load pattern workspace.")} />;
  }

  const runs = runsQuery.data ?? [];
  const latestRun = runs[0] ?? null;
  const runningRun = runs.find((run) => run.status === "queued" || run.status === "running") ?? null;
  const latestCompletedRun = runs.find((run) => run.status === "completed" && run.report) ?? null;
  const report: ResearchSummary | null = latestCompletedRun?.report ?? summaryQuery.data ?? null;
  const runningEtaText = buildPatternEta(runningRun);

  const candidateCount = report?.patterns.filter((item) => item.verdict === "candidate").length ?? 0;
  const readySeries = report?.coverage.filter((item) => item.ready_for_pattern_scan).length ?? 0;

  async function handleStart() {
    setMessage(null);
    try {
      await startMutation.mutateAsync({
        exchange_code: "binance_us",
        symbols: [...presetSymbols],
        timeframes: [...presetTimeframes],
        lookback_days: lookbackDays,
        forward_bars: forwardBars,
        fee_pct: 0.001,
        slippage_pct: 0.0005,
        max_bars_per_series: maxBarsPerSeries,
      });
      setMessage("Pattern scan queued. The worker will pick it up and keep the full report in history.");
    } catch (error) {
      setMessage(getErrorMessage(error, "Unable to queue pattern scan."));
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Pattern Mining"
        title="Pattern Workspace"
        description="Run rule-based pattern scans over the feature-backed dataset, keep the scans in history, and review the best friction-adjusted candidates without relying on fragile live requests."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Pattern scans" value={formatInteger(runs.length)} hint="Background run history" />
        <MetricCard label="Ready series" value={formatInteger(readySeries)} hint={`${formatInteger(report?.coverage.length ?? 0)} series in the report`} tone={readySeries ? "positive" : "warning"} />
        <MetricCard label="Candidate patterns" value={formatInteger(candidateCount)} hint={`${formatInteger(report?.patterns.length ?? 0)} ranked rows`} tone={candidateCount ? "positive" : "warning"} />
        <MetricCard
          label="Latest run"
          value={latestRun ? `#${latestRun.id}` : "No runs"}
          hint={latestRun ? `${latestRun.status} · ${formatDateTime(latestRun.updated_at)}` : "Queue the first pattern scan"}
          tone={latestRun?.status === "failed" ? "danger" : latestRun?.status === "completed" ? "positive" : latestRun ? "warning" : "default"}
        />
      </section>

      <SectionCard title="Pattern scan control" eyebrow="Queued background research">
        <div className="grid gap-5 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
          <div className="space-y-5">
            <p className="max-w-3xl text-sm leading-7 text-slate-400">
              The first scan uses rule-based patterns only: range breakout, flush reclaim, and compression release. We
              run the full basket in the background and keep the finished leaderboard in history so we can compare runs
              cleanly over time.
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

            <div className="flex flex-wrap items-center gap-2">
              <span className="mr-1 text-[11px] uppercase tracking-[0.2em] text-slate-500">Forward bars</span>
              {forwardPresets.map((bars) => (
                <button
                  key={bars}
                  type="button"
                  onClick={() => setForwardBars(bars)}
                  className={`rounded-full border px-3.5 py-1.5 text-sm font-medium transition ${
                    forwardBars === bars
                      ? "border-sky-300/45 bg-sky-400/15 text-sky-100"
                      : "border-white/10 bg-slate-950/50 text-slate-200 hover:border-sky-300/30 hover:bg-sky-400/10 hover:text-white"
                  }`}
                >
                  {bars}
                </button>
              ))}
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <label className="grid gap-2">
                <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Max bars per series</span>
                <input
                  type="number"
                  min={500}
                  max={50000}
                  value={maxBarsPerSeries}
                  onChange={(event) => setMaxBarsPerSeries(Number(event.target.value))}
                  className={inputClassName}
                />
              </label>
              <div className="rounded-2xl border border-white/8 bg-slate-950/45 px-4 py-3 text-sm text-slate-300">
                Universe: {presetSymbols.length} symbols · {presetTimeframes.length} timeframes
                <br />
                Total series per run: {presetSymbols.length * presetTimeframes.length}
              </div>
            </div>

            <div className="flex flex-wrap gap-3">
              <button
                type="button"
                onClick={handleStart}
                disabled={startMutation.isPending || Boolean(runningRun)}
                className="rounded-xl bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
              >
                {startMutation.isPending ? "Queueing scan..." : runningRun ? "Pattern scan already running" : "Start pattern scan"}
              </button>
            </div>

            {message ? (
              <div className="rounded-2xl border border-white/8 bg-white/5 px-4 py-3 text-sm text-slate-200">
                {message}
              </div>
            ) : null}
          </div>

          <div className="space-y-4 rounded-3xl border border-white/8 bg-slate-950/35 p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Live run status</p>

            {runningRun ? (
              <div className="rounded-2xl border border-sky-400/15 bg-sky-400/10 px-4 py-4 text-sm text-sky-100">
                <p className="font-medium">
                  Pattern scan {runningRun.status} · run #{runningRun.id}
                </p>
                <p className="mt-1 text-sky-100/80">
                  Started {formatNullableDateTime(runningRun.started_at)} · updated {formatNullableDateTime(runningRun.updated_at)}
                </p>
                {runningRun.progress ? (
                  <div className="mt-3 space-y-2">
                    <div className="flex items-center justify-between gap-3 text-xs text-sky-100/85">
                      <span>
                        {runningRun.progress.processed_series}/{runningRun.progress.total_series} series
                      </span>
                      <span>{formatPercent(runningRun.progress.percent_complete)} complete</span>
                    </div>
                    <div className="h-2 overflow-hidden rounded-full bg-slate-950/40">
                      <div
                        className="h-full rounded-full bg-sky-300 transition-all duration-300"
                        style={{ width: `${Number(runningRun.progress.percent_complete) || 0}%` }}
                      />
                    </div>
                    <p className="text-xs text-sky-100/75">
                      Current: {runningRun.progress.current_symbol ?? "—"} · {runningRun.progress.current_timeframe ?? "—"}
                    </p>
                    <p className="text-xs text-sky-100/75">{runningEtaText}</p>
                  </div>
                ) : null}
              </div>
            ) : (
              <div className="rounded-2xl border border-white/8 bg-white/[0.04] px-4 py-4 text-sm text-slate-300">
                No active pattern scan. Queue one to build the first leaderboard from the full feature-backed dataset.
              </div>
            )}

            <div className="grid gap-3 md:grid-cols-2">
              <MetricCard label="Fee assumption" value="0.10%" hint="Per side" />
              <MetricCard label="Slippage assumption" value="0.05%" hint="Per side" />
              <MetricCard label="Patterns" value="3" hint="Rule-based MVP" />
              <MetricCard label="Execution model" value="Next-bar" hint="Roundtrip friction-adjusted" />
            </div>
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Pattern scan history" eyebrow="Background run ledger">
        <DataTable
          rows={runs}
          rowKey={(row) => row.id}
          emptyTitle="No pattern scans yet"
          emptyDescription="Start the first background scan to build history here."
          columns={[
            { key: "id", title: "Run", render: (row) => `#${row.id}` },
            {
              key: "status",
              title: "Status",
              render: (row) => (
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    <StatusBadge status={row.status} />
                    <span className="text-xs text-slate-400">{row.status}</span>
                  </div>
                  {row.progress ? (
                    <span className="text-[11px] text-slate-500">
                      {row.progress.processed_series}/{row.progress.total_series} · {formatPercent(row.progress.percent_complete)}
                    </span>
                  ) : null}
                </div>
              ),
            },
            { key: "window", title: "Window", render: (row) => `${row.lookback_days}d · +${row.forward_bars} bars` },
            {
              key: "timing",
              title: "Started",
              render: (row) => formatNullableDateTime(row.started_at ?? row.created_at),
            },
            {
              key: "finished",
              title: "Finished",
              render: (row) => formatNullableDateTime(row.completed_at),
            },
            {
              key: "candidates",
              title: "Candidates",
              render: (row) =>
                row.report_summary
                  ? formatInteger(row.report_summary.patterns.filter((item) => item.verdict === "candidate").length)
                  : row.error_text
                    ? "0"
                    : "—",
            },
          ]}
        />
      </SectionCard>

      {latestRun?.status === "failed" && latestRun.error_text ? (
        <section className="rounded-3xl border border-rose-400/20 bg-rose-400/10 p-4 text-sm text-rose-100">
          <p className="font-medium">Latest pattern scan failed</p>
          <p className="mt-2 whitespace-pre-wrap text-rose-100/85">{latestRun.error_text}</p>
        </section>
      ) : null}

      {report ? (
        <>
          <SectionCard title="Top pattern candidates" eyebrow="Latest completed report">
            <DataTable
              rows={report.patterns.slice(0, 20)}
              rowKey={(row) => `${row.symbol}-${row.timeframe}-${row.pattern_code}`}
              emptyTitle="No patterns scored yet"
              emptyDescription="Run a scan after feature generation finishes to populate the leaderboard."
              columns={[
                {
                  key: "pattern",
                  title: "Pattern",
                  render: (row) => (
                    <div className="grid gap-1">
                      <span className="font-medium text-white">{row.pattern_name}</span>
                      <span className="text-xs text-slate-400">{row.pattern_code}</span>
                    </div>
                  ),
                },
                { key: "market", title: "Market", render: (row) => `${row.symbol} · ${row.timeframe}` },
                { key: "samples", title: "Samples", render: (row) => formatInteger(row.sample_size) },
                { key: "winrate", title: "Win rate", render: (row) => formatPercent(row.win_rate_pct) },
                { key: "avgForward", title: "Avg forward", render: (row) => formatPercent(row.avg_forward_return_pct) },
                { key: "avgNet", title: "Avg net", render: (row) => formatPercent(row.avg_net_return_pct) },
                { key: "verdict", title: "Verdict", render: (row) => <StatusBadge status={row.verdict} /> },
              ]}
            />
          </SectionCard>

          <SectionCard title="Coverage matrix" eyebrow="Latest completed pattern report">
            <DataTable
              rows={report.coverage}
              rowKey={(row) => `${row.symbol}-${row.timeframe}`}
              emptyTitle="No pattern coverage rows"
              emptyDescription="Coverage appears after the first completed pattern scan."
              columns={[
                { key: "market", title: "Market", render: (row) => `${row.symbol} · ${row.timeframe}` },
                { key: "candles", title: "Candles", render: (row) => formatInteger(row.candle_count) },
                {
                  key: "range",
                  title: "Loaded range",
                  render: (row) =>
                    row.loaded_start_at && row.loaded_end_at
                      ? `${formatDateTime(row.loaded_start_at)} -> ${formatDateTime(row.loaded_end_at)}`
                      : "No data",
                },
                { key: "ready", title: "Ready", render: (row) => <StatusBadge status={row.ready_for_pattern_scan ? "ready" : "insufficient_history"} /> },
              ]}
            />
          </SectionCard>
        </>
      ) : null}
    </div>
  );
}

const inputClassName =
  "h-11 rounded-xl border border-white/10 bg-slate-950/60 px-3 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-sky-400/40";

function formatNullableDateTime(value: string | null) {
  if (!value) {
    return "—";
  }
  return formatDateTime(value);
}

function buildPatternEta(run: PatternScanRun | null) {
  if (!run?.started_at || !run.progress || run.progress.processed_series <= 0 || run.progress.total_series <= 0) {
    return "ETA will appear once the scan has processed a few series.";
  }

  const startedAtMs = new Date(run.started_at).getTime();
  const nowMs = Date.now();
  if (!Number.isFinite(startedAtMs) || nowMs <= startedAtMs) {
    return "ETA unavailable.";
  }

  const elapsedMs = nowMs - startedAtMs;
  const avgMsPerSeries = elapsedMs / Number(run.progress.processed_series);
  const remainingSeries = Math.max(Number(run.progress.total_series) - Number(run.progress.processed_series), 0);
  const etaMs = Math.max(remainingSeries * avgMsPerSeries, 0);

  if (!Number.isFinite(etaMs) || etaMs <= 0) {
    return "Almost done.";
  }

  return `ETA ~${formatEta(etaMs)} remaining.`;
}

function formatEta(durationMs: number) {
  const totalMinutes = Math.max(Math.round(durationMs / 60000), 0);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;

  if (hours <= 0) {
    return `${Math.max(minutes, 1)}m`;
  }
  return `${hours}h ${minutes}m`;
}
