"use client";

import Link from "next/link";
import { useDeferredValue, useEffect, useState } from "react";

import { BacktestForm } from "@/components/forms/backtest-form";
import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { MetricCard } from "@/components/ui/metric-card";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useBacktests, useDeleteBacktests, useStopBacktest, useStrategies } from "@/lib/query-hooks";
import { formatCurrency, formatDateTime, formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

export default function BacktestsPage() {
  const strategiesQuery = useStrategies();
  const backtestsQuery = useBacktests({ limit: 200 });
  const [strategyFilter, setStrategyFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [search, setSearch] = useState("");
  const [selectedRunIds, setSelectedRunIds] = useState<number[]>([]);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const deferredSearch = useDeferredValue(search);
  const stopBacktest = useStopBacktest();
  const deleteBacktests = useDeleteBacktests();

  const strategies = strategiesQuery.data ?? [];
  const backtestsData = backtestsQuery.data;
  const backtests = backtestsData ?? [];
  const normalizedSearch = deferredSearch.trim().toLowerCase();

  const filteredBacktests = backtests.filter((backtest) => {
    const matchesStrategy = strategyFilter === "all" || backtest.strategy_code === strategyFilter;
    const matchesStatus = statusFilter === "all" || backtest.status === statusFilter;
    const matchesSearch =
      !normalizedSearch ||
      String(backtest.id).includes(normalizedSearch) ||
      backtest.symbol.toLowerCase().includes(normalizedSearch) ||
      backtest.timeframe.toLowerCase().includes(normalizedSearch) ||
      backtest.strategy_name.toLowerCase().includes(normalizedSearch);

    return matchesStrategy && matchesStatus && matchesSearch;
  });

  const completedRuns = filteredBacktests.filter((backtest) => backtest.status === "completed");
  const failedRuns = filteredBacktests.filter((backtest) => backtest.status === "failed");
  const averageReturn =
    completedRuns.length > 0
      ? completedRuns.reduce((sum, run) => sum + Number(run.total_return_pct), 0) / completedRuns.length
      : 0;
  const selectedFilteredRunIds = filteredBacktests
    .map((backtest) => backtest.id)
    .filter((runId) => selectedRunIds.includes(runId));

  useEffect(() => {
    const currentBacktests = backtestsData ?? [];
    setSelectedRunIds((current) => current.filter((runId) => currentBacktests.some((backtest) => backtest.id === runId)));
  }, [backtestsData]);

  if ((strategiesQuery.isLoading || backtestsQuery.isLoading) && !strategiesQuery.data && !backtestsQuery.data) {
    return <LoadingState label="Loading backtests..." />;
  }

  const error = strategiesQuery.error ?? backtestsQuery.error;
  if (error) {
    return <ErrorState message={getErrorMessage(error, "Unable to load backtest data.")} />;
  }

  async function handleStop(runId: number) {
    setActionMessage(null);
    if (!window.confirm(`Stop backtest #${runId}?`)) {
      return;
    }

    try {
      const result = await stopBacktest.mutateAsync({
        id: runId,
        payload: { reason: "manual_stop" },
      });
      setActionMessage(`Backtest #${runId} marked ${result.status}.`);
    } catch (mutationError) {
      setActionMessage(getErrorMessage(mutationError, `Unable to stop backtest #${runId}.`));
    }
  }

  function toggleRunSelection(runId: number) {
    setSelectedRunIds((current) =>
      current.includes(runId) ? current.filter((value) => value !== runId) : [...current, runId],
    );
  }

  async function handleDeleteSelected() {
    setActionMessage(null);
    if (!selectedFilteredRunIds.length) {
      return;
    }

    const label =
      selectedFilteredRunIds.length === 1
        ? `Delete backtest #${selectedFilteredRunIds[0]}?`
        : `Delete ${selectedFilteredRunIds.length} selected backtests?`;
    if (!window.confirm(`${label} This removes them from the database.`)) {
      return;
    }

    try {
      const result = await deleteBacktests.mutateAsync({ run_ids: selectedFilteredRunIds });
      setSelectedRunIds((current) => current.filter((runId) => !result.deleted_run_ids.includes(runId)));

      const parts: string[] = [];
      if (result.deleted_run_ids.length) {
        parts.push(`Deleted ${result.deleted_run_ids.length} run${result.deleted_run_ids.length === 1 ? "" : "s"}.`);
      }
      if (result.blocked_runs.length) {
        parts.push(
          `Skipped active run${result.blocked_runs.length === 1 ? "" : "s"}: ${result.blocked_runs
            .map((item) => `#${item.run_id}`)
            .join(", ")}.`,
        );
      }
      if (result.missing_run_ids.length) {
        parts.push(`Already missing: ${result.missing_run_ids.map((runId) => `#${runId}`).join(", ")}.`);
      }
      setActionMessage(parts.join(" ") || "No backtests were deleted.");
    } catch (mutationError) {
      setActionMessage(getErrorMessage(mutationError, "Unable to delete selected backtests."));
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Research Lab"
        title="Backtests"
        description="Launch candle-driven historical runs, review execution metrics, and inspect equity curves before promoting ideas into paper trading."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Runs loaded" value={formatInteger(filteredBacktests.length)} hint={`${formatInteger(backtests.length)} total`} />
        <MetricCard label="Completed" value={formatInteger(completedRuns.length)} hint="Finished simulations" tone="positive" />
        <MetricCard label="Failed" value={formatInteger(failedRuns.length)} hint="Need investigation" tone={failedRuns.length ? "danger" : "default"} />
        <MetricCard label="Avg return" value={formatPercent(averageReturn)} hint="Completed runs only" tone={averageReturn >= 0 ? "positive" : "danger"} />
      </section>

      <SectionCard title="Run backtest" eyebrow="Simulation parameters">
        <BacktestForm strategies={strategies} />
      </SectionCard>

      <SectionCard title="Backtest history" eyebrow="Runs and filters">
        <div className="mb-5 grid gap-4 border-b border-white/6 pb-5 md:grid-cols-2 xl:grid-cols-4">
          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Strategy</span>
            <select value={strategyFilter} onChange={(event) => setStrategyFilter(event.target.value)} className={inputClassName}>
              <option value="all">All visible strategies</option>
              {strategies.map((strategy) => (
                <option key={strategy.code} value={strategy.code}>
                  {strategy.name}
                </option>
              ))}
            </select>
          </label>

          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Status</span>
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)} className={inputClassName}>
              <option value="all">All statuses</option>
              <option value="completed">completed</option>
              <option value="running">running</option>
              <option value="queued">queued</option>
              <option value="failed">failed</option>
            </select>
          </label>

          <label className="grid gap-2 xl:col-span-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Search</span>
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              className={inputClassName}
              placeholder="Run id, symbol, timeframe, strategy"
            />
          </label>
        </div>
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          {actionMessage ? <p className="text-sm text-slate-300">{actionMessage}</p> : <div />}
          {selectedFilteredRunIds.length ? (
            <button
              type="button"
              onClick={handleDeleteSelected}
              disabled={deleteBacktests.isPending}
              className="rounded-xl border border-rose-400/30 bg-rose-500/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-rose-100 transition hover:bg-rose-500/20 disabled:cursor-not-allowed disabled:border-white/10 disabled:bg-slate-900 disabled:text-slate-500"
            >
              {deleteBacktests.isPending ? "Deleting..." : `Delete (${selectedFilteredRunIds.length})`}
            </button>
          ) : null}
        </div>

        <DataTable
          rows={filteredBacktests}
          rowKey={(row) => row.id}
          emptyTitle="No backtests match the current filters"
          emptyDescription="Adjust the filters or run a new backtest."
          columns={[
            {
              key: "run",
              title: "Run",
              render: (row) => (
                <div className="grid gap-1">
                  <Link href={`/backtests/${row.id}`} className="font-medium text-white transition hover:text-sky-300">
                    #{row.id}
                  </Link>
                  <span className="text-xs text-slate-400">{row.strategy_name}</span>
                </div>
              ),
            },
            {
              key: "market",
              title: "Market",
              render: (row) => `${row.symbol} · ${row.timeframe}`,
            },
            {
              key: "equity",
              title: "Final equity",
              render: (row) => formatCurrency(row.final_equity),
            },
            {
              key: "return",
              title: "Return",
              render: (row) => formatPercent(row.total_return_pct),
            },
            {
              key: "drawdown",
              title: "Max DD",
              render: (row) => formatPercent(row.max_drawdown_pct),
            },
            {
              key: "trades",
              title: "Trades",
              render: (row) => formatInteger(row.total_trades),
            },
            {
              key: "completed",
              title: "Completed",
              render: (row) => formatDateTime(row.completed_at ?? row.started_at),
            },
            {
              key: "status",
              title: "Status",
              render: (row) => <StatusBadge status={row.status} />,
            },
            {
              key: "select",
              title: "",
              className: "w-14",
              render: (row) => {
                const selected = selectedRunIds.includes(row.id);
                return (
                  <button
                    type="button"
                    onClick={() => toggleRunSelection(row.id)}
                    aria-label={selected ? `Deselect backtest #${row.id}` : `Select backtest #${row.id}`}
                    className={`flex h-6 w-6 items-center justify-center rounded-md border text-sm transition ${
                      selected
                        ? "border-emerald-300/60 bg-emerald-400/15 text-emerald-100"
                        : "border-white/15 bg-slate-950/70 text-transparent hover:border-slate-300/40 hover:text-slate-200"
                    }`}
                  >
                    ✓
                  </button>
                );
              },
            },
            {
              key: "actions",
              title: "Actions",
              render: (row) =>
                row.status === "running" || row.status === "queued" ? (
                  <button
                    type="button"
                    onClick={() => handleStop(row.id)}
                    disabled={stopBacktest.isPending || deleteBacktests.isPending}
                    className="rounded-xl border border-rose-400/30 bg-rose-500/10 px-3 py-2 text-xs font-semibold text-rose-100 transition hover:bg-rose-500/20 disabled:cursor-not-allowed disabled:border-white/10 disabled:bg-slate-900 disabled:text-slate-500"
                  >
                    {stopBacktest.isPending ? "Stopping..." : "Stop"}
                  </button>
                ) : (
                  <span className="text-xs text-slate-500">No action</span>
                ),
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}

const inputClassName =
  "h-11 rounded-xl border border-white/10 bg-slate-950/60 px-3 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-sky-400/40";
