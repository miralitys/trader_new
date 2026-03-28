"use client";

import { useMemo, useState } from "react";

import { DataTable } from "@/components/tables/data-table";
import { MetricCard } from "@/components/ui/metric-card";
import { StatusBadge } from "@/components/ui/status-badge";
import { useRunDataValidation } from "@/lib/query-hooks";
import type { DataValidationReport } from "@/lib/types";
import { formatDateTime, formatInteger, formatPercent, getErrorMessage } from "@/lib/utils";

const validationSymbols = [
  "BTC-USDT",
  "ETH-USDT",
  "SOL-USDT",
  "BNB-USDT",
  "ADA-USDT",
  "ALPINE-USDT",
  "XRP-USDT",
  "1INCH-USDT",
  "LTC-USDT",
  "BCH-USDT",
  "AVAX-USDT",
  "LINK-USDT",
  "DOGE-USDT",
  "ICP-USDT",
  "GALA-USDT",
  "AXS-USDT",
  "ONDO-USDT",
  "IOTA-USDT",
  "FIL-USDT",
] as const;

const validationTimeframes = ["1m", "5m", "15m", "1h", "4h"] as const;

export function DataValidationForm() {
  const validationMutation = useRunDataValidation();
  const [lookbackDays, setLookbackDays] = useState(730);
  const [sampleLimit, setSampleLimit] = useState(5);
  const [report, setReport] = useState<DataValidationReport | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const csvHref = useMemo(() => {
    if (!report) {
      return null;
    }

    const header = [
      "symbol",
      "timeframe",
      "verdict",
      "completion_pct",
      "missing_candles",
      "duplicate_rows",
      "invalid_timestamps",
      "issue_codes",
    ];
    const rows = report.results.map((row) =>
      [
        row.symbol,
        row.timeframe,
        row.verdict,
        String(row.validation_window.completion_pct),
        String(row.gaps.missing_candle_count),
        String(row.duplicates.duplicate_count),
        String(row.timestamp_alignment.invalid_timestamp_count),
        row.issues.map((issue) => issue.code).join("|"),
      ].join(","),
    );
    const csv = [header.join(","), ...rows].join("\n");
    return URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8;" }));
  }, [report]);

  const jsonHref = useMemo(() => {
    if (!report) {
      return null;
    }
    return URL.createObjectURL(new Blob([JSON.stringify(report, null, 2)], { type: "application/json" }));
  }, [report]);

  async function handleRunValidation() {
    setErrorMessage(null);
    try {
      const nextReport = await validationMutation.mutateAsync({
        exchange_code: "binance_us",
        symbols: [...validationSymbols],
        timeframes: [...validationTimeframes],
        lookback_days: lookbackDays,
        sample_limit: sampleLimit,
        perform_resync: false,
        resync_days: 14,
      });
      setReport(nextReport);
    } catch (error) {
      setErrorMessage(getErrorMessage(error, "Unable to run offline validation report."));
    }
  }

  return (
    <div className="space-y-5">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
        <div className="rounded-3xl border border-amber-300/10 bg-amber-300/5 p-4">
          <div className="space-y-3">
            <div>
              <p className="text-[11px] uppercase tracking-[0.25em] text-amber-200/80">Offline Validation Report</p>
              <h3 className="mt-1 text-base font-semibold text-white">Validate the full 95-series dataset</h3>
              <p className="mt-2 text-sm leading-7 text-slate-400">
                This checks every symbol/timeframe combination and gives us one honest report for gaps, duplicates,
                malformed candles, timeframe coverage alignment, and weak 1m datasets.
              </p>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <label className="grid gap-2">
                <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Lookback days</span>
                <input
                  type="number"
                  min={30}
                  max={730}
                  value={lookbackDays}
                  onChange={(event) => setLookbackDays(Number(event.target.value))}
                  className={inputClassName}
                />
              </label>

              <label className="grid gap-2">
                <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Sample limit</span>
                <input
                  type="number"
                  min={1}
                  max={20}
                  value={sampleLimit}
                  onChange={(event) => setSampleLimit(Number(event.target.value))}
                  className={inputClassName}
                />
              </label>
            </div>

            <div className="rounded-2xl border border-white/8 bg-slate-950/45 px-4 py-3 text-sm text-slate-300">
              <p>Universe: {validationSymbols.length} symbols</p>
              <p>Timeframes: {validationTimeframes.join(", ")}</p>
              <p>Total series checked: {validationSymbols.length * validationTimeframes.length}</p>
            </div>

            <div className="flex flex-wrap gap-3">
              <button
                type="button"
                onClick={handleRunValidation}
                disabled={validationMutation.isPending}
                className="rounded-xl bg-amber-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-amber-200 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
              >
                {validationMutation.isPending ? "Running validation..." : "Run offline validation"}
              </button>

              {jsonHref ? (
                <a
                  href={jsonHref}
                  download="data-validation-report.json"
                  className="rounded-xl border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
                >
                  Download JSON
                </a>
              ) : null}

              {csvHref ? (
                <a
                  href={csvHref}
                  download="data-validation-report.csv"
                  className="rounded-xl border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
                >
                  Download CSV
                </a>
              ) : null}
            </div>

            {errorMessage ? <div className="rounded-2xl border border-rose-400/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">{errorMessage}</div> : null}
          </div>
        </div>

        <div className="rounded-3xl border border-white/8 bg-white/[0.03] p-4">
          <p className="text-[11px] uppercase tracking-[0.25em] text-slate-400">What this validates</p>
          <div className="mt-3 grid gap-3 md:grid-cols-2">
            {[
              "No large internal gaps",
              "No duplicate candle rows",
              "No malformed candles",
              "No invalid timestamp grid",
              "Coverage consistency across timeframes",
              "1m laggards vs stronger higher timeframes",
            ].map((item) => (
              <div key={item} className="rounded-2xl border border-white/8 bg-slate-950/35 px-4 py-3 text-sm text-slate-300">
                {item}
              </div>
            ))}
          </div>
        </div>
      </div>

      {report ? (
        <>
          <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Report verdict" value={report.summary.verdict} tone={report.summary.verdict === "PASS" ? "positive" : report.summary.verdict === "FAIL" ? "danger" : "warning"} />
            <MetricCard label="Series checked" value={formatInteger(report.summary.overview.total_series)} hint={formatDateTime(report.summary.generated_at)} />
            <MetricCard label="Internal gaps" value={formatInteger(report.summary.overview.internal_gap_total)} tone={report.summary.overview.internal_gap_total > 0 ? "warning" : "positive"} />
            <MetricCard label="1m laggards tracked" value={formatInteger(report.summary.one_minute_laggards.length)} />
            <MetricCard label="Pass" value={formatInteger(report.summary.overview.pass_count)} tone="positive" />
            <MetricCard label="Warnings" value={formatInteger(report.summary.overview.warning_count)} tone={report.summary.overview.warning_count > 0 ? "warning" : "default"} />
            <MetricCard label="Fail" value={formatInteger(report.summary.overview.fail_count)} tone={report.summary.overview.fail_count > 0 ? "danger" : "default"} />
            <MetricCard
              label="Duplicate / invalid ts"
              value={`${formatInteger(report.summary.overview.duplicate_rows_total)} / ${formatInteger(report.summary.overview.invalid_timestamps_total)}`}
              tone={report.summary.overview.duplicate_rows_total > 0 || report.summary.overview.invalid_timestamps_total > 0 ? "danger" : "positive"}
            />
          </section>

          <section className="grid gap-4 xl:grid-cols-3">
            <div className="space-y-3">
              <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-400">Worst symbols</h4>
              <DataTable
                rows={report.summary.worst_symbols}
                rowKey={(row) => row.symbol}
                emptyTitle="No symbol issues"
                emptyDescription="No symbols were flagged as weak."
                columns={[
                  { key: "symbol", title: "Symbol", render: (row) => row.symbol },
                  { key: "completion", title: "Worst completion", render: (row) => formatPercent(row.worst_completion_pct) },
                  { key: "gaps", title: "Gap count", render: (row) => formatInteger(row.total_gap_count) },
                  { key: "fail", title: "Failing series", render: (row) => formatInteger(row.failing_series_count) },
                ]}
              />
            </div>

            <div className="space-y-3">
              <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-400">Worst timeframes</h4>
              <DataTable
                rows={report.summary.worst_timeframes}
                rowKey={(row) => row.timeframe}
                emptyTitle="No timeframe issues"
                emptyDescription="No timeframe issues were flagged."
                columns={[
                  { key: "tf", title: "Timeframe", render: (row) => row.timeframe },
                  { key: "completion", title: "Avg completion", render: (row) => formatPercent(row.avg_completion_pct) },
                  { key: "gaps", title: "Gap count", render: (row) => formatInteger(row.total_gap_count) },
                  { key: "fail", title: "Failing series", render: (row) => formatInteger(row.failing_series_count) },
                ]}
              />
            </div>

            <div className="space-y-3">
              <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-400">1m laggards</h4>
              <DataTable
                rows={report.summary.one_minute_laggards}
                rowKey={(row) => row.symbol}
                emptyTitle="No 1m laggards"
                emptyDescription="1m coverage is aligned with the stronger higher timeframes."
                columns={[
                  { key: "symbol", title: "Symbol", render: (row) => row.symbol },
                  { key: "completion", title: "1m completion", render: (row) => formatPercent(row.completion_pct) },
                  { key: "gap", title: "Vs best tf", render: (row) => formatPercent(row.gap_vs_best_timeframe_pct) },
                  { key: "gaps", title: "Gap count", render: (row) => formatInteger(row.gap_count) },
                ]}
              />
            </div>
          </section>

          <section className="space-y-3">
            <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-400">Series detail</h4>
            <DataTable
              rows={report.results}
              rowKey={(row, index) => `${row.symbol}-${row.timeframe}-${index}`}
              emptyTitle="No validation rows"
              emptyDescription="Run the offline validation to populate this section."
              columns={[
                { key: "market", title: "Market", render: (row) => `${row.symbol} · ${row.timeframe}` },
                {
                  key: "verdict",
                  title: "Verdict",
                  render: (row) => (
                    <div className="flex items-center gap-2">
                      <StatusBadge
                        status={
                          row.verdict === "PASS"
                            ? "completed"
                            : row.verdict === "FAIL"
                              ? "failed"
                              : "warning"
                        }
                      />
                      <span className="text-xs text-slate-400">{row.verdict}</span>
                    </div>
                  ),
                },
                { key: "completion", title: "Completion", render: (row) => formatPercent(row.validation_window.completion_pct) },
                { key: "gaps", title: "Gap count", render: (row) => formatInteger(row.gaps.missing_candle_count) },
                { key: "dups", title: "Duplicates", render: (row) => formatInteger(row.duplicates.duplicate_count) },
                { key: "ts", title: "Invalid ts", render: (row) => formatInteger(row.timestamp_alignment.invalid_timestamp_count) },
                {
                  key: "issues",
                  title: "Issues",
                  render: (row) => (
                    <div className="flex flex-wrap gap-1">
                      {row.issues.length ? row.issues.map((issue) => (
                        <span key={issue.code} className="rounded-full border border-white/10 px-2 py-0.5 text-xs text-slate-300">
                          {issue.code}
                        </span>
                      )) : <span className="text-xs text-slate-500">none</span>}
                    </div>
                  ),
                },
              ]}
            />
          </section>
        </>
      ) : null}
    </div>
  );
}

const inputClassName =
  "h-11 rounded-xl border border-white/10 bg-slate-950/60 px-3 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-sky-400/40";
