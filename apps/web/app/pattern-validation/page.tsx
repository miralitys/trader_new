"use client";

import { useEffect, useMemo, useState } from "react";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { usePatternScans } from "@/lib/query-hooks";
import type { PatternScanRun, PatternSummary } from "@/lib/types";
import { formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

type RegistryVerdict = "candidate" | "monitor";
type ValidationBucket = "promote" | "keep_watching" | "archive";

type AggregatedPatternRecord = {
  key: string;
  patternName: string;
  patternCode: string;
  symbol: string;
  timeframe: string;
  verdict: RegistryVerdict;
  candidateHits: number;
  monitorHits: number;
  runHits: number;
  totalSampleSize: number;
  avgSampleSize: number;
  bestNetReturnPct: number;
  avgNetReturnPct: number;
  bestWinRatePct: number;
  avgWinRatePct: number;
  windows: string[];
  horizons: number[];
  bucket: ValidationBucket;
};

type PatternHit = {
  runId: number;
  lookbackDays: number;
  forwardBars: number;
  maxBarsPerSeries: number;
  verdict: string;
  sampleSize: number;
  avgNetReturnPct: number;
  avgForwardReturnPct: number;
  winRatePct: number;
  timeframe: string;
  symbol: string;
  patternName: string;
  patternCode: string;
};

export default function PatternValidationPage() {
  const runsQuery = usePatternScans(200, true);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  const runs = runsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const registry = aggregatePatternRegistry(completedRuns);
  const hitsByKey = buildPatternHitHistory(completedRuns);

  const promoteRows = registry.filter((row) => row.bucket === "promote");
  const keepWatchingRows = registry.filter((row) => row.bucket === "keep_watching");
  const archiveRows = registry.filter((row) => row.bucket === "archive");

  useEffect(() => {
    if (!registry.length) {
      setSelectedKey(null);
      return;
    }
    setSelectedKey((current) => (current && registry.some((row) => row.key === current) ? current : registry[0].key));
  }, [registry]);

  const selectedRecord = registry.find((row) => row.key === selectedKey) ?? null;
  const selectedHits = selectedKey ? hitsByKey.get(selectedKey) ?? [] : [];

  if (runsQuery.isLoading && !runsQuery.data) {
    return <LoadingState label="Loading pattern validation..." />;
  }

  if (runsQuery.error) {
    return <ErrorState message={getErrorMessage(runsQuery.error, "Unable to load pattern validation.")} />;
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Next Research Step"
        title="Pattern Validation"
        description="Review every candidate and monitor as a cross-run object, not a single report row. Pick one setup, inspect its hit history, and decide whether we promote it, keep watching it, or archive it."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Completed scans" value={formatInteger(completedRuns.length)} hint="Runs included in validation" />
        <MetricCard label="Promote" value={formatInteger(promoteRows.length)} hint="First-priority setups" tone={promoteRows.length ? "positive" : "default"} />
        <MetricCard label="Keep watching" value={formatInteger(keepWatchingRows.length)} hint="Live but not promoted yet" tone={keepWatchingRows.length ? "warning" : "default"} />
        <MetricCard label="Archive" value={formatInteger(archiveRows.length)} hint="Low-priority review backlog" />
      </section>

      <SectionCard title="Validation Queue" eyebrow="Cross-run buckets">
        <div className="grid gap-4 xl:grid-cols-3">
          <div className="rounded-2xl border border-emerald-400/15 bg-emerald-400/10 p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-emerald-100/70">Promote</p>
            <p className="mt-2 text-sm leading-6 text-emerald-50/85">
              Repeated candidate behavior, positive economics, and enough cross-run evidence to deserve the next validation pass.
            </p>
          </div>
          <div className="rounded-2xl border border-amber-300/15 bg-amber-300/10 p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-amber-100/70">Keep Watching</p>
            <p className="mt-2 text-sm leading-6 text-amber-50/85">
              Live setups with either weaker repetition, weaker economics, or not enough stability yet.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Archive</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Interesting enough to keep, but not where we should spend the next round of attention.
            </p>
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Promote Queue" eyebrow="First-priority setups">
        <DataTable
          rows={promoteRows}
          rowKey={(row) => row.key}
          emptyTitle="No promote setups yet"
          emptyDescription="Promote rows appear once repeated candidates start clustering."
          columns={buildRegistryColumns(selectedKey, setSelectedKey)}
        />
      </SectionCard>

      <SectionCard title="Keep Watching" eyebrow="Worth tracking, not promoted yet">
        <DataTable
          rows={keepWatchingRows}
          rowKey={(row) => row.key}
          emptyTitle="No watchlist setups yet"
          emptyDescription="Monitor-heavy setups will appear here automatically."
          columns={buildRegistryColumns(selectedKey, setSelectedKey)}
        />
      </SectionCard>

      <SectionCard title="Archive" eyebrow="Low-priority backlog">
        <DataTable
          rows={archiveRows}
          rowKey={(row) => row.key}
          emptyTitle="Nothing archived yet"
          emptyDescription="Archive rows appear when a setup has too little support to justify immediate follow-up."
          columns={buildRegistryColumns(selectedKey, setSelectedKey)}
        />
      </SectionCard>

      {selectedRecord ? (
        <>
          <SectionCard title="Selected Setup" eyebrow={`${selectedRecord.patternName} · ${selectedRecord.symbol} · ${selectedRecord.timeframe}`}>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <MetricCard label="Bucket" value={formatBucketLabel(selectedRecord.bucket)} hint={`${formatInteger(selectedRecord.candidateHits)} candidate · ${formatInteger(selectedRecord.monitorHits)} monitor`} tone={selectedRecord.bucket === "promote" ? "positive" : selectedRecord.bucket === "keep_watching" ? "warning" : "default"} />
              <MetricCard label="Run hits" value={formatInteger(selectedRecord.runHits)} hint={`${selectedRecord.windows.join(", ")} windows`} />
              <MetricCard label="Avg / Best net" value={formatPercent(selectedRecord.avgNetReturnPct)} hint={`best ${formatPercent(selectedRecord.bestNetReturnPct)}`} tone={selectedRecord.avgNetReturnPct > 0 ? "positive" : "warning"} />
              <MetricCard label="Avg sample" value={formatInteger(Math.round(selectedRecord.avgSampleSize))} hint={`${formatInteger(selectedRecord.totalSampleSize)} total sample`} />
            </div>
          </SectionCard>

          <SectionCard title="Hit History" eyebrow="Every completed scan where this setup appeared">
            <DataTable
              rows={selectedHits}
              rowKey={(row) => `${row.runId}-${row.lookbackDays}-${row.forwardBars}`}
              emptyTitle="No hit history"
              emptyDescription="Completed scan hits for the selected setup will appear here."
              columns={[
                { key: "run", title: "Run", render: (row) => `#${row.runId}` },
                { key: "window", title: "Window", render: (row) => `${row.lookbackDays}d · +${row.forwardBars} bars` },
                { key: "bars", title: "Max bars", render: (row) => formatInteger(row.maxBarsPerSeries) },
                { key: "sample", title: "Sample", render: (row) => formatInteger(row.sampleSize) },
                { key: "net", title: "Avg net", render: (row) => formatPercent(row.avgNetReturnPct) },
                { key: "forward", title: "Avg forward", render: (row) => formatPercent(row.avgForwardReturnPct) },
                { key: "win", title: "Win rate", render: (row) => formatPercent(row.winRatePct) },
                { key: "verdict", title: "Verdict", render: (row) => <StatusBadge status={row.verdict} /> },
              ]}
            />
          </SectionCard>
        </>
      ) : null}
    </div>
  );
}

function buildRegistryColumns(
  selectedKey: string | null,
  onSelect: (key: string) => void,
) {
  return [
    {
      key: "setup",
      title: "Setup",
      render: (row: AggregatedPatternRecord) => (
        <button
          type="button"
          onClick={() => onSelect(row.key)}
          className={`grid gap-1 rounded-xl border px-3 py-2 text-left transition ${
            row.key === selectedKey
              ? "border-sky-300/35 bg-sky-400/10"
              : "border-white/10 bg-white/[0.03] hover:border-sky-300/20 hover:bg-sky-400/[0.06]"
          }`}
        >
          <span className="font-medium text-white">{row.patternName}</span>
          <span className="text-xs text-slate-400">
            {row.symbol} · {row.timeframe}
          </span>
        </button>
      ),
    },
    {
      key: "hits",
      title: "Hits",
      render: (row: AggregatedPatternRecord) => (
        <div className="grid gap-1">
          <span>{formatInteger(row.candidateHits)} candidate</span>
          <span className="text-xs text-slate-400">{formatInteger(row.monitorHits)} monitor</span>
        </div>
      ),
    },
    {
      key: "net",
      title: "Avg / Best net",
      render: (row: AggregatedPatternRecord) => (
        <div className="grid gap-1">
          <span>{formatPercent(row.avgNetReturnPct)}</span>
          <span className="text-xs text-slate-400">best {formatPercent(row.bestNetReturnPct)}</span>
        </div>
      ),
    },
    {
      key: "coverage",
      title: "Windows / Horizons",
      render: (row: AggregatedPatternRecord) => (
        <div className="grid gap-1 text-xs text-slate-300">
          <span>{row.windows.join(", ")}</span>
          <span className="text-slate-400">{row.horizons.map((value) => `+${value}`).join(", ")} bars</span>
        </div>
      ),
    },
    {
      key: "bucket",
      title: "Bucket",
      render: (row: AggregatedPatternRecord) => <StatusBadge status={row.bucket} />,
    },
  ];
}

function aggregatePatternRegistry(runs: PatternScanRun[]) {
  const registry = new Map<string, AggregatedPatternRecord>();

  for (const run of runs) {
    const report = run.report_summary;
    if (!report) {
      continue;
    }

    for (const pattern of report.patterns) {
      if (pattern.verdict !== "candidate" && pattern.verdict !== "monitor") {
        continue;
      }

      const key = `${pattern.pattern_code}:${pattern.symbol}:${pattern.timeframe}`;
      const existing = registry.get(key);
      const next = accumulatePatternRecord(existing, pattern, run.lookback_days, run.forward_bars, key);
      registry.set(key, next);
    }
  }

  return Array.from(registry.values()).sort(compareRegistryRows);
}

function buildPatternHitHistory(runs: PatternScanRun[]) {
  const history = new Map<string, PatternHit[]>();

  for (const run of runs) {
    const report = run.report_summary;
    if (!report) {
      continue;
    }

    for (const pattern of report.patterns) {
      if (pattern.verdict !== "candidate" && pattern.verdict !== "monitor") {
        continue;
      }

      const key = `${pattern.pattern_code}:${pattern.symbol}:${pattern.timeframe}`;
      const hit: PatternHit = {
        runId: run.id,
        lookbackDays: run.lookback_days,
        forwardBars: run.forward_bars,
        maxBarsPerSeries: run.max_bars_per_series,
        verdict: pattern.verdict,
        sampleSize: Number(pattern.sample_size ?? 0),
        avgNetReturnPct: Number(pattern.avg_net_return_pct ?? 0),
        avgForwardReturnPct: Number(pattern.avg_forward_return_pct ?? 0),
        winRatePct: Number(pattern.win_rate_pct ?? 0),
        timeframe: pattern.timeframe,
        symbol: pattern.symbol,
        patternName: pattern.pattern_name,
        patternCode: pattern.pattern_code,
      };
      history.set(key, [...(history.get(key) ?? []), hit]);
    }
  }

  for (const [key, hits] of history.entries()) {
    history.set(
      key,
      hits.sort((left, right) => {
        if (left.forwardBars !== right.forwardBars) {
          return left.forwardBars - right.forwardBars;
        }
        if (left.lookbackDays !== right.lookbackDays) {
          return left.lookbackDays - right.lookbackDays;
        }
        return left.runId - right.runId;
      }),
    );
  }

  return history;
}

function accumulatePatternRecord(
  existing: AggregatedPatternRecord | undefined,
  pattern: PatternSummary,
  lookbackDays: number,
  forwardBars: number,
  key: string,
): AggregatedPatternRecord {
  const currentNet = Number(pattern.avg_net_return_pct ?? 0);
  const currentWinRate = Number(pattern.win_rate_pct ?? 0);
  const sampleSize = Number(pattern.sample_size ?? 0);
  const windowLabel = `${lookbackDays}d`;

  if (!existing) {
    const base: AggregatedPatternRecord = {
      key,
      patternName: pattern.pattern_name,
      patternCode: pattern.pattern_code,
      symbol: pattern.symbol,
      timeframe: pattern.timeframe,
      verdict: pattern.verdict as RegistryVerdict,
      candidateHits: pattern.verdict === "candidate" ? 1 : 0,
      monitorHits: pattern.verdict === "monitor" ? 1 : 0,
      runHits: 1,
      totalSampleSize: sampleSize,
      avgSampleSize: sampleSize,
      bestNetReturnPct: currentNet,
      avgNetReturnPct: currentNet,
      bestWinRatePct: currentWinRate,
      avgWinRatePct: currentWinRate,
      windows: [windowLabel],
      horizons: [forwardBars],
      bucket: "archive",
    };
    return { ...base, bucket: classifyBucket(base) };
  }

  const runHits = existing.runHits + 1;
  const totalSampleSize = existing.totalSampleSize + sampleSize;
  const next: AggregatedPatternRecord = {
    ...existing,
    verdict: existing.candidateHits + (pattern.verdict === "candidate" ? 1 : 0) > 0 ? "candidate" : "monitor",
    candidateHits: existing.candidateHits + (pattern.verdict === "candidate" ? 1 : 0),
    monitorHits: existing.monitorHits + (pattern.verdict === "monitor" ? 1 : 0),
    runHits,
    totalSampleSize,
    avgSampleSize: totalSampleSize / runHits,
    bestNetReturnPct: Math.max(existing.bestNetReturnPct, currentNet),
    avgNetReturnPct: (existing.avgNetReturnPct * existing.runHits + currentNet) / runHits,
    bestWinRatePct: Math.max(existing.bestWinRatePct, currentWinRate),
    avgWinRatePct: (existing.avgWinRatePct * existing.runHits + currentWinRate) / runHits,
    windows: uniqueSortedStrings([...existing.windows, windowLabel]),
    horizons: uniqueSortedNumbers([...existing.horizons, forwardBars]),
    bucket: "archive",
  };
  return { ...next, bucket: classifyBucket(next) };
}

function classifyBucket(row: AggregatedPatternRecord): ValidationBucket {
  if (
    row.candidateHits >= 2 &&
    row.avgNetReturnPct > 0 &&
    row.avgSampleSize >= 15 &&
    row.windows.length >= 1 &&
    row.horizons.length >= 1
  ) {
    return "promote";
  }

  if (
    row.candidateHits >= 1 ||
    (row.monitorHits >= 2 && row.bestNetReturnPct > 0) ||
    (row.monitorHits >= 1 && row.avgNetReturnPct > 0 && row.avgSampleSize >= 10)
  ) {
    return "keep_watching";
  }

  return "archive";
}

function uniqueSortedStrings(values: string[]) {
  return Array.from(new Set(values)).sort((left, right) => Number.parseInt(left, 10) - Number.parseInt(right, 10));
}

function uniqueSortedNumbers(values: number[]) {
  return Array.from(new Set(values)).sort((left, right) => left - right);
}

function compareRegistryRows(left: AggregatedPatternRecord, right: AggregatedPatternRecord) {
  const bucketPriority: Record<ValidationBucket, number> = {
    promote: 0,
    keep_watching: 1,
    archive: 2,
  };

  if (bucketPriority[left.bucket] !== bucketPriority[right.bucket]) {
    return bucketPriority[left.bucket] - bucketPriority[right.bucket];
  }
  if (right.candidateHits !== left.candidateHits) {
    return right.candidateHits - left.candidateHits;
  }
  if (right.monitorHits !== left.monitorHits) {
    return right.monitorHits - left.monitorHits;
  }
  if (right.avgNetReturnPct !== left.avgNetReturnPct) {
    return right.avgNetReturnPct - left.avgNetReturnPct;
  }
  return left.key.localeCompare(right.key);
}

function formatBucketLabel(value: ValidationBucket) {
  if (value === "keep_watching") {
    return "Keep Watching";
  }
  return value[0].toUpperCase() + value.slice(1);
}
