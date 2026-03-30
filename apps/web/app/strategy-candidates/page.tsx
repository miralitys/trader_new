"use client";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { usePatternScans } from "@/lib/query-hooks";
import { aggregateApprovedStrategyCandidates } from "@/lib/strategy-layer";
import { formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

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
  const candidates = aggregateApprovedStrategyCandidates(completedRuns);

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Promotion Layer"
        title="Strategy Candidates"
        description="The first approved pool promoted out of Pattern Validation. These are the setups we should translate into explicit replay specs, paper-test specs, and then real strategy logic."
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
          label="Fast layer"
          value={formatInteger(candidates.filter((row) => row.timeframe === "15m" || row.timeframe === "5m").length)}
          hint="Intraday candidates ready for deeper testing"
          tone="warning"
        />
      </section>

      <SectionCard title="Approved Pool" eyebrow="Current strategy-candidate set">
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
