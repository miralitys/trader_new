"use client";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { usePatternScans } from "@/lib/query-hooks";
import { aggregateApprovedStrategyCandidates, timeframePaperWindow } from "@/lib/strategy-layer";
import { formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

export default function PaperTestsPage() {
  const runsQuery = usePatternScans(200, true);

  if (runsQuery.isLoading && !runsQuery.data) {
    return <LoadingState label="Loading paper-test layer..." />;
  }

  if (runsQuery.error) {
    return <ErrorState message={getErrorMessage(runsQuery.error, "Unable to load paper-test layer.")} />;
  }

  const runs = runsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const candidates = aggregateApprovedStrategyCandidates(completedRuns);
  const swingCount = candidates.filter((row) => row.timeframe === "4h" || row.timeframe === "1h").length;
  const intradayCount = candidates.filter((row) => row.timeframe === "15m" || row.timeframe === "5m" || row.timeframe === "1m").length;

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Forward Layer"
        title="Paper Tests"
        description="This layer turns the approved pool into forward-paper specs. It tells us which setups we want to watch live after the nightly pipeline refreshes, how long we should observe them, and what kind of behavior should keep them in the pool."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Approved setups" value={formatInteger(candidates.length)} hint="Current forward-paper queue" tone={candidates.length ? "positive" : "warning"} />
        <MetricCard label="Swing tempo" value={formatInteger(swingCount)} hint="1h and 4h setups to watch over weeks" />
        <MetricCard label="Intraday tempo" value={formatInteger(intradayCount)} hint="15m and 5m setups to watch over days" tone="warning" />
        <MetricCard label="Paper mode" value="Latest only" hint="Forward observation starts from the newest post-nightly slice" tone="positive" />
      </section>

      <SectionCard title="Forward Queue" eyebrow="First paper-test pass under the approved pool">
        <DataTable
          rows={candidates}
          rowKey={(row) => row.key}
          emptyTitle="No forward-paper candidates yet"
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
              key: "plan",
              title: "Paper plan",
              render: (row) => (
                <div className="grid gap-1 text-sm text-slate-200">
                  <span>Latest-only forward paper</span>
                  <span className="text-xs text-slate-400">{timeframePaperWindow(row.timeframe)} observation window</span>
                </div>
              ),
            },
            {
              key: "evidence",
              title: "Validated edge",
              render: (row) => (
                <div className="grid gap-1">
                  <span>{formatPercent(row.avgNetReturnPct)} avg net</span>
                  <span className="text-xs text-slate-400">best hit {row.bestLookbackDays}d / +{row.bestForwardBars}</span>
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
              title: "What to watch",
              render: (row) => <span className="text-sm text-slate-300">{row.paperFocus}</span>,
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="How To Use This Layer" eyebrow="What paper testing means here">
        <div className="grid gap-4 xl:grid-cols-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">1. Start from nightly refresh</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              We only care about forward behavior after the nightly data, validation, and feature pipeline has finished. That keeps paper tests aligned with the freshest trusted slice.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">2. Judge behavior, not just PnL</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              For paper tests we care about signal frequency, regime quality, and whether the live tape still matches the pattern shape that got approved.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">3. Graduate carefully</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              A setup should leave this layer only after forward behavior still looks healthy, not just because one historical backtest looked attractive.
            </p>
          </div>
        </div>
      </SectionCard>
    </div>
  );
}
