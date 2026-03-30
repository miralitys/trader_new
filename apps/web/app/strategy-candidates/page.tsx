"use client";

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

type StrategyCandidateRow = {
  key: string;
  priority: number;
  patternName: string;
  patternCode: string;
  symbol: string;
  timeframe: string;
  candidateHits: number;
  monitorHits: number;
  avgSampleSize: number;
  avgNetReturnPct: number;
  bestNetReturnPct: number;
  windows: string[];
  horizons: number[];
  role: string;
  whyItMatters: string;
  nextStep: string;
};

const candidateBriefs: Record<
  string,
  {
    priority: number;
    role: string;
    whyItMatters: string;
    nextStep: string;
  }
> = {
  "compression_release:AVAX-USDT:1h": {
    priority: 1,
    role: "Baseline 1h leader",
    whyItMatters: "Most repeated 1h candidate in the full matrix and one of the cleanest cross-horizon signals.",
    nextStep: "Use as the first reference model for the strategy layer.",
  },
  "flush_reclaim:1INCH-USDT:1h": {
    priority: 2,
    role: "Best 1h reclaim",
    whyItMatters: "Repeats across all major windows and stays economically strong instead of fading after one good run.",
    nextStep: "Promote as the main reclaim-continuation candidate.",
  },
  "range_breakout:GALA-USDT:1h": {
    priority: 3,
    role: "Fast 1h breakout",
    whyItMatters: "Clean repeated breakout behavior with strong net returns on the shorter and medium horizons.",
    nextStep: "Frame as the first fast impulse breakout prototype.",
  },
  "compression_release:ADA-USDT:1h": {
    priority: 4,
    role: "Stable 1h compression",
    whyItMatters: "Less flashy than the top two, but repeatable enough to deserve a full strategy candidate pass.",
    nextStep: "Keep in the primary 1h pool and validate exits.",
  },
  "compression_release:GALA-USDT:1h": {
    priority: 5,
    role: "Secondary GALA 1h setup",
    whyItMatters: "Blends repeated candidate and monitor behavior, which suggests a real edge with some regime sensitivity.",
    nextStep: "Compare directly against GALA 1h breakout and decide whether to merge or split the model.",
  },
  "range_breakout:BNB-USDT:4h": {
    priority: 6,
    role: "Slow structural breakout",
    whyItMatters: "Gives you a higher-timeframe anchor that is calmer and easier to reason about than the fast intraday setups.",
    nextStep: "Turn into a 4h structural breakout candidate with conservative exits.",
  },
  "flush_reclaim:1INCH-USDT:4h": {
    priority: 7,
    role: "Higher-timeframe reclaim",
    whyItMatters: "Useful as a slower counterpart to the 1h reclaim model on the same symbol.",
    nextStep: "Validate as the higher-timeframe confirmation variant.",
  },
  "flush_reclaim:GALA-USDT:5m": {
    priority: 8,
    role: "Best intraday candidate",
    whyItMatters: "The strongest lower-timeframe result in the matrix, especially on the longer windows and horizons.",
    nextStep: "Promote as the first serious intraday strategy candidate.",
  },
  "flush_reclaim:IOTA-USDT:5m": {
    priority: 9,
    role: "Second intraday reclaim",
    whyItMatters: "Not as explosive as GALA, but it carries a stronger sample base and more measured economics.",
    nextStep: "Use as the comparison intraday reclaim model.",
  },
  "flush_reclaim:IOTA-USDT:15m": {
    priority: 10,
    role: "Bridge setup",
    whyItMatters: "Sits between the 5m and 1h layers and helps us compare how reclaim behavior changes with speed.",
    nextStep: "Keep as the bridge candidate between intraday and mid-timeframe strategy logic.",
  },
};

export default function StrategyCandidatesPage() {
  const runsQuery = usePatternScans(200, true);

  if (runsQuery.isLoading && !runsQuery.data) {
    return <LoadingState label="Loading strategy candidates..." />;
  }

  if (runsQuery.error) {
    return <ErrorState message={getErrorMessage(runsQuery.error, "Unable to load strategy candidates.")} />;
  }

  const runs = runsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const candidates = aggregateStrategyCandidates(completedRuns);

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Promotion Layer"
        title="Strategy Candidates"
        description="The first approved pool promoted out of Pattern Validation. These are the setups we should turn into explicit strategy prototypes next."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Completed scans" value={formatInteger(completedRuns.length)} hint="Source runs behind the candidate pool" />
        <MetricCard label="Approved setups" value={formatInteger(candidates.length)} hint="Promoted into strategy-candidate status" tone={candidates.length ? "positive" : "warning"} />
        <MetricCard
          label="1h focus"
          value={formatInteger(candidates.filter((row) => row.timeframe === "1h").length)}
          hint="Current strongest timeframe"
          tone="positive"
        />
        <MetricCard
          label="Intraday focus"
          value={formatInteger(candidates.filter((row) => row.timeframe === "5m" || row.timeframe === "15m").length)}
          hint="Fast setups ready for deeper review"
          tone="warning"
        />
      </section>

      <SectionCard title="Approved Pool" eyebrow="First strategy-candidate list">
        <DataTable
          rows={candidates}
          rowKey={(row) => row.key}
          emptyTitle="No approved candidates yet"
          emptyDescription="Approved setups will appear here once the validation layer promotes them."
          columns={[
            { key: "priority", title: "Priority", render: (row) => formatInteger(row.priority) },
            {
              key: "setup",
              title: "Setup",
              render: (row) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{row.patternName}</span>
                  <span className="text-xs text-slate-400">
                    {row.symbol} · {row.timeframe}
                  </span>
                </div>
              ),
            },
            {
              key: "signal",
              title: "Why this stays",
              render: (row) => (
                <div className="grid gap-1 text-sm text-slate-200">
                  <span>{row.role}</span>
                  <span className="text-xs leading-5 text-slate-400">{row.whyItMatters}</span>
                </div>
              ),
            },
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
              key: "stats",
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
            {
              key: "next",
              title: "Next step",
              render: (row) => <span className="text-sm text-slate-300">{row.nextStep}</span>,
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}

function aggregateStrategyCandidates(runs: PatternScanRun[]) {
  const registry = new Map<string, StrategyCandidateRow>();

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
      const currentNet = Number(pattern.avg_net_return_pct ?? 0);
      const sampleSize = Number(pattern.sample_size ?? 0);
      const windowLabel = `${run.lookback_days}d`;
      const existing = registry.get(key);

      if (!existing) {
        registry.set(key, {
          key,
          priority: candidateBriefs[key]?.priority ?? 999,
          patternName: pattern.pattern_name,
          patternCode: pattern.pattern_code,
          symbol: pattern.symbol,
          timeframe: pattern.timeframe,
          candidateHits: pattern.verdict === "candidate" ? 1 : 0,
          monitorHits: pattern.verdict === "monitor" ? 1 : 0,
          avgSampleSize: sampleSize,
          avgNetReturnPct: currentNet,
          bestNetReturnPct: currentNet,
          windows: [windowLabel],
          horizons: [run.forward_bars],
          role: candidateBriefs[key]?.role ?? "Exploratory strategy candidate",
          whyItMatters: candidateBriefs[key]?.whyItMatters ?? "This setup earned promotion from repeated completed scans.",
          nextStep: candidateBriefs[key]?.nextStep ?? "Translate it into a concrete strategy prototype.",
        });
        continue;
      }

      const totalHits = existing.candidateHits + existing.monitorHits + 1;
      const previousHits = totalHits - 1;
      registry.set(key, {
        ...existing,
        candidateHits: existing.candidateHits + (pattern.verdict === "candidate" ? 1 : 0),
        monitorHits: existing.monitorHits + (pattern.verdict === "monitor" ? 1 : 0),
        avgSampleSize: (existing.avgSampleSize * previousHits + sampleSize) / totalHits,
        avgNetReturnPct: (existing.avgNetReturnPct * previousHits + currentNet) / totalHits,
        bestNetReturnPct: Math.max(existing.bestNetReturnPct, currentNet),
        windows: uniqueSortedStrings([...existing.windows, windowLabel]),
        horizons: uniqueSortedNumbers([...existing.horizons, run.forward_bars]),
      });
    }
  }

  return Array.from(registry.values())
    .filter((row) => isApproved(row))
    .sort((left, right) => {
      if (left.priority !== right.priority) {
        return left.priority - right.priority;
      }
      if (right.candidateHits !== left.candidateHits) {
        return right.candidateHits - left.candidateHits;
      }
      return right.avgNetReturnPct - left.avgNetReturnPct;
    });
}

function isApproved(row: StrategyCandidateRow) {
  return row.candidateHits >= 2 && row.avgNetReturnPct > 0 && row.avgSampleSize >= 15;
}

function uniqueSortedStrings(values: string[]) {
  return Array.from(new Set(values)).sort((left, right) => Number.parseInt(left, 10) - Number.parseInt(right, 10));
}

function uniqueSortedNumbers(values: number[]) {
  return Array.from(new Set(values)).sort((left, right) => left - right);
}
