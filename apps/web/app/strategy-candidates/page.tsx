"use client";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useBacktests, usePatternScans } from "@/lib/query-hooks";
import {
  aggregateApprovedStrategyCandidates,
  applyBaselineBacktestVerdicts,
  type StrategyCandidateRow,
} from "@/lib/strategy-layer";
import { formatDateTime, formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

export default function StrategyCandidatesPage() {
  const runsQuery = usePatternScans(200, true);
  const backtestsQuery = useBacktests({ limit: 100 }, true);

  if ((runsQuery.isLoading && !runsQuery.data) || (backtestsQuery.isLoading && !backtestsQuery.data)) {
    return <LoadingState label="Loading strategy candidates..." />;
  }

  const error = runsQuery.error ?? backtestsQuery.error;
  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load strategy candidates.")} />;
  }

  const runs = runsQuery.data ?? [];
  const backtests = backtestsQuery.data ?? [];
  const completedRuns = runs.filter((run) => run.status === "completed" && run.report_summary);
  const candidates = applyBaselineBacktestVerdicts(
    aggregateApprovedStrategyCandidates(completedRuns),
    backtests,
  );
  const promoted = candidates.filter((row) => row.baselineStatus === "promoted");
  const watching = candidates.filter((row) => row.baselineStatus === "watch");
  const archived = candidates.filter((row) => row.baselineStatus === "archived_after_baseline");
  const pending = candidates.filter((row) => row.baselineStatus === "pending");

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Promotion Layer"
        title="Strategy Candidates"
        description="The first approved pool promoted out of Pattern Validation. Baseline replay results now automatically push each setup into a working status: promoted, watch, or archived after baseline."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Completed scans" value={formatInteger(completedRuns.length)} hint="Source runs behind the candidate pool" />
        <MetricCard label="Approved setups" value={formatInteger(candidates.length)} hint="Promoted into strategy-candidate status" tone={candidates.length ? "positive" : "warning"} />
        <MetricCard label="Promoted" value={formatInteger(promoted.length)} hint="Held up on baseline replay" tone={promoted.length ? "positive" : "default"} />
        <MetricCard label="Watch / Archived" value={`${formatInteger(watching.length)} / ${formatInteger(archived.length)}`} hint={`${formatInteger(pending.length)} pending baseline review`} tone={archived.length ? "warning" : "default"} />
      </section>

      <SectionCard title="Promoted After Baseline" eyebrow="Keep moving forward with these">
        <DataTable
          rows={promoted}
          rowKey={(row) => row.key}
          emptyTitle="Nothing promoted yet"
          emptyDescription="Once a baseline replay finishes with healthy return and contained drawdown, the setup will appear here."
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
              key: "baseline_status",
              title: "Baseline verdict",
              render: (row) => (
                <div className="grid gap-1">
                  <StatusBadge status={row.baselineStatus} />
                  <span className="text-xs leading-5 text-slate-400">{row.baselineReason}</span>
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
              key: "backtest",
              title: "Latest replay",
              render: (row) =>
                row.latestBacktest ? (
                  <div className="grid gap-1">
                    <span>{formatPercent(row.latestBacktest.total_return_pct)}</span>
                    <span className="text-xs text-slate-400">
                      DD {formatPercent(row.latestBacktest.max_drawdown_pct)} · {formatInteger(row.latestBacktest.total_trades)} trades
                    </span>
                  </div>
                ) : (
                  <span className="text-sm text-slate-400">No replay yet</span>
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

      <SectionCard title="Watchlist After Baseline" eyebrow="Not dead, but not ready yet">
        <DataTable
          rows={watching}
          rowKey={(row) => row.key}
          emptyTitle="No watchlist setups"
          emptyDescription="Near-flat baseline results will land here for the next review round."
          columns={buildBaselineColumns()}
        />
      </SectionCard>

      <SectionCard title="Archived After Baseline" eyebrow="Set aside for now">
        <DataTable
          rows={archived}
          rowKey={(row) => row.key}
          emptyTitle="Nothing archived after baseline"
          emptyDescription="Setups with weak baseline replay results will be parked here."
          columns={buildBaselineColumns()}
        />
      </SectionCard>
    </div>
  );
}

function buildBaselineColumns() {
  return [
    { key: "priority", title: "Priority", render: (row: StrategyCandidateRow) => formatInteger(row.priority) },
    {
      key: "setup",
      title: "Setup",
      render: (row: StrategyCandidateRow) => (
        <div className="grid gap-1">
          <span className="font-medium text-white">{row.patternName}</span>
          <span className="text-xs text-slate-400">
            {row.symbol} · {row.timeframe}
          </span>
        </div>
      ),
    },
    {
      key: "verdict",
      title: "Baseline verdict",
      render: (row: StrategyCandidateRow) => (
        <div className="grid gap-1">
          <StatusBadge status={row.baselineStatus} />
          <span className="text-xs leading-5 text-slate-400">{row.baselineReason}</span>
        </div>
      ),
    },
    {
      key: "latest",
      title: "Latest replay",
      render: (row: StrategyCandidateRow) =>
        row.latestBacktest ? (
          <div className="grid gap-1">
            <span>{formatPercent(row.latestBacktest.total_return_pct)}</span>
            <span className="text-xs text-slate-400">
              DD {formatPercent(row.latestBacktest.max_drawdown_pct)} · {formatInteger(row.latestBacktest.total_trades)} trades
            </span>
            <span className="text-xs text-slate-500">{formatDateTime(row.latestBacktest.completed_at)}</span>
          </div>
        ) : (
          <span className="text-sm text-slate-400">No replay yet</span>
        ),
    },
    {
      key: "next",
      title: "Next step",
      render: (row: StrategyCandidateRow) => <span className="text-sm text-slate-300">{row.nextStep}</span>,
    },
  ];
}
