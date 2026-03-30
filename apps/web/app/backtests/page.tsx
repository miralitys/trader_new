"use client";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { usePatternScans } from "@/lib/query-hooks";
import { aggregateApprovedStrategyCandidates } from "@/lib/strategy-layer";
import { formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

export default function BacktestsPage() {
  const runsQuery = usePatternScans(200, true);

  if (runsQuery.isLoading && !runsQuery.data) {
    return <LoadingState label="Loading backtest layer..." />;
  }

  if (runsQuery.error) {
    return <ErrorState message={getErrorMessage(runsQuery.error, "Unable to load backtest layer.")} />;
  }

  const runs = runsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const candidates = aggregateApprovedStrategyCandidates(completedRuns);
  const longestWindowCount = candidates.filter((row) => row.bestLookbackDays >= 720).length;
  const longerExitCount = candidates.filter((row) => row.bestForwardBars >= 12).length;

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Replay Layer"
        title="Backtests"
        description="This layer turns the approved strategy pool into explicit replay specs. It gives us one operating surface for what we should replay first, with which window, and with which validated horizon before we wire these setups into executable strategy code."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Approved setups" value={formatInteger(candidates.length)} hint="Current replay queue" tone={candidates.length ? "positive" : "warning"} />
        <MetricCard label="720d-ready" value={formatInteger(longestWindowCount)} hint="Best validated hit came from the full long window" />
        <MetricCard label="12/24-bar lean" value={formatInteger(longerExitCount)} hint="Setups whose strongest hit already prefers a longer hold" />
        <MetricCard label="Engine status" value="Spec ready" hint="Replay specs are ready even though engine wiring is still the next step" tone="positive" />
      </section>

      <SectionCard title="Replay Queue" eyebrow="First backtest pass under the approved pool">
        <DataTable
          rows={candidates}
          rowKey={(row) => row.key}
          emptyTitle="No replay candidates yet"
          emptyDescription="Approved strategy candidates will appear here automatically."
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
              key: "replay",
              title: "First replay spec",
              render: (row) => (
                <div className="grid gap-1 text-sm text-slate-200">
                  <span>
                    {row.bestLookbackDays}d · +{row.bestForwardBars} bars
                  </span>
                  <span className="text-xs text-slate-400">{formatInteger(row.bestMaxBarsPerSeries)} max bars per series</span>
                </div>
              ),
            },
            {
              key: "evidence",
              title: "Evidence",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatInteger(row.candidateHits)} candidate hits</span>
                  <span className="text-xs text-slate-400">{formatInteger(Math.round(row.avgSampleSize))} avg sample</span>
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
              key: "status",
              title: "Status",
              render: () => <StatusBadge status="ready" />,
            },
            {
              key: "focus",
              title: "Replay goal",
              render: (row) => <span className="text-sm text-slate-300">{row.backtestFocus}</span>,
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="How To Use This Layer" eyebrow="What we do here next">
        <div className="grid gap-4 xl:grid-cols-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">1. Replay first</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Start with the recommended replay spec for each setup. That gives us one baseline per candidate instead of immediately exploding into parameter churn.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">2. Compare exits</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Once the baseline replay is healthy, compare nearby exit horizons and check whether the edge survives with conservative assumptions.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">3. Promote to engine</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Only after a candidate survives replay do we translate it into executable strategy logic for the real backtest and paper engine layer.
            </p>
          </div>
        </div>
      </SectionCard>
    </div>
  );
}
