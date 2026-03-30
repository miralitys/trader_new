"use client";

import { useMemo } from "react";

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
};

export default function PatternRegistryPage() {
  const runsQuery = usePatternScans(200, true);

  if (runsQuery.isLoading && !runsQuery.data) {
    return <LoadingState label="Loading pattern registry..." />;
  }

  if (runsQuery.error) {
    return <ErrorState message={getErrorMessage(runsQuery.error, "Unable to load pattern registry.")} />;
  }

  const runs = runsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const registry = aggregatePatternRegistry(completedRuns);

  const candidateRows = registry.filter((row) => row.verdict === "candidate");
  const monitorRows = registry.filter((row) => row.verdict === "monitor");

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Cross-Run Registry"
        title="Pattern Registry"
        description="Unified registry across all completed pattern scans. Use it to review every unique candidate and monitor across windows, horizons, and timeframes without bouncing between individual reports."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Completed scans" value={formatInteger(completedRuns.length)} hint="Runs included in the registry" />
        <MetricCard label="Unique candidates" value={formatInteger(candidateRows.length)} hint="Cross-run candidate setups" tone={candidateRows.length ? "positive" : "warning"} />
        <MetricCard label="Unique monitors" value={formatInteger(monitorRows.length)} hint="Cross-run monitor setups" tone={monitorRows.length ? "warning" : "default"} />
        <MetricCard
          label="Registry rows"
          value={formatInteger(registry.length)}
          hint="Unique pattern + symbol + timeframe combinations"
        />
      </section>

      <SectionCard title="Candidate Registry" eyebrow="All unique candidate setups">
        <DataTable
          rows={candidateRows}
          rowKey={(row) => row.key}
          emptyTitle="No candidate setups yet"
          emptyDescription="Completed scans with candidate verdicts will appear here automatically."
          columns={[
            {
              key: "pattern",
              title: "Pattern",
              render: (row) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{row.patternName}</span>
                  <span className="text-xs text-slate-400">{row.patternCode}</span>
                </div>
              ),
            },
            { key: "market", title: "Market", render: (row) => `${row.symbol} · ${row.timeframe}` },
            {
              key: "hits",
              title: "Hits",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatInteger(row.candidateHits)} candidate</span>
                  <span className="text-xs text-slate-400">{formatInteger(row.monitorHits)} monitor</span>
                </div>
              ),
            },
            {
              key: "sample",
              title: "Avg sample",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatInteger(Math.round(row.avgSampleSize))}</span>
                  <span className="text-xs text-slate-400">{formatInteger(row.totalSampleSize)} total</span>
                </div>
              ),
            },
            {
              key: "net",
              title: "Avg / Best net",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatPercent(row.avgNetReturnPct)}</span>
                  <span className="text-xs text-slate-400">best {formatPercent(row.bestNetReturnPct)}</span>
                </div>
              ),
            },
            {
              key: "winrate",
              title: "Avg / Best win",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatPercent(row.avgWinRatePct)}</span>
                  <span className="text-xs text-slate-400">best {formatPercent(row.bestWinRatePct)}</span>
                </div>
              ),
            },
            {
              key: "coverage",
              title: "Windows / Horizons",
              render: (row) => (
                <div className="grid gap-1 text-xs text-slate-300">
                  <span>{row.windows.join(", ")}</span>
                  <span className="text-slate-400">{row.horizons.map((value) => `+${value}`).join(", ")} bars</span>
                </div>
              ),
            },
            { key: "verdict", title: "Registry verdict", render: (row) => <StatusBadge status={row.verdict} /> },
          ]}
        />
      </SectionCard>

      <SectionCard title="Monitor Registry" eyebrow="All unique monitor setups">
        <DataTable
          rows={monitorRows}
          rowKey={(row) => row.key}
          emptyTitle="No monitor setups yet"
          emptyDescription="Completed scans with monitor verdicts will appear here automatically."
          columns={[
            {
              key: "pattern",
              title: "Pattern",
              render: (row) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{row.patternName}</span>
                  <span className="text-xs text-slate-400">{row.patternCode}</span>
                </div>
              ),
            },
            { key: "market", title: "Market", render: (row) => `${row.symbol} · ${row.timeframe}` },
            {
              key: "hits",
              title: "Hits",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatInteger(row.monitorHits)} monitor</span>
                  <span className="text-xs text-slate-400">{formatInteger(row.candidateHits)} candidate</span>
                </div>
              ),
            },
            {
              key: "sample",
              title: "Avg sample",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatInteger(Math.round(row.avgSampleSize))}</span>
                  <span className="text-xs text-slate-400">{formatInteger(row.totalSampleSize)} total</span>
                </div>
              ),
            },
            {
              key: "net",
              title: "Avg / Best net",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatPercent(row.avgNetReturnPct)}</span>
                  <span className="text-xs text-slate-400">best {formatPercent(row.bestNetReturnPct)}</span>
                </div>
              ),
            },
            {
              key: "coverage",
              title: "Windows / Horizons",
              render: (row) => (
                <div className="grid gap-1 text-xs text-slate-300">
                  <span>{row.windows.join(", ")}</span>
                  <span className="text-slate-400">{row.horizons.map((value) => `+${value}`).join(", ")} bars</span>
                </div>
              ),
            },
            { key: "verdict", title: "Registry verdict", render: (row) => <StatusBadge status={row.verdict} /> },
          ]}
        />
      </SectionCard>
    </div>
  );
}

function aggregatePatternRegistry(runs: PatternScanRun[]) {
  const registry = new Map<string, AggregatedPatternRecord>();

  for (const run of runs) {
    const timeframe = run.timeframes[0];
    const report = run.report_summary;
    if (!timeframe || !report) {
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
    return {
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
    };
  }

  const runHits = existing.runHits + 1;
  const totalSampleSize = existing.totalSampleSize + sampleSize;
  return {
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
  };
}

function uniqueSortedStrings(values: string[]) {
  return Array.from(new Set(values)).sort((left, right) => Number.parseInt(left, 10) - Number.parseInt(right, 10));
}

function uniqueSortedNumbers(values: number[]) {
  return Array.from(new Set(values)).sort((left, right) => left - right);
}

function compareRegistryRows(left: AggregatedPatternRecord, right: AggregatedPatternRecord) {
  if (left.verdict !== right.verdict) {
    return left.verdict === "candidate" ? -1 : 1;
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
