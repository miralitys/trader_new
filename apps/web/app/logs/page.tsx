"use client";

import { useState } from "react";

import { SectionCard } from "@/components/section-card";
import { DataTable } from "@/components/tables/data-table";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading-state";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { useLogs } from "@/lib/query-hooks";
import { formatDateTime, getErrorMessage, prettyJson } from "@/lib/utils";

export default function LogsPage() {
  const [scope, setScope] = useState("");
  const [level, setLevel] = useState("");
  const [limit, setLimit] = useState("100");
  const logsQuery = useLogs({
    scope: scope || undefined,
    level: level || undefined,
    limit: Number(limit) || 100,
  });

  if (logsQuery.isLoading && !logsQuery.data) {
    return <LoadingState label="Loading logs..." />;
  }

  if (logsQuery.error) {
    return <ErrorState message={getErrorMessage(logsQuery.error, "Unable to load logs.")} />;
  }

  const logs = logsQuery.data ?? [];

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Observability"
        title="Application logs"
        description="Inspect ingestion activity, research scans, and backend issues without leaving the workspace."
      />

      <SectionCard title="Log stream" eyebrow="Filters and recent events">
        <div className="mb-5 grid gap-4 border-b border-white/6 pb-5 md:grid-cols-2 xl:grid-cols-3">
          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Scope</span>
            <input value={scope} onChange={(event) => setScope(event.target.value)} className={inputClassName} placeholder="market_data, research, api" />
          </label>

          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Level</span>
            <select value={level} onChange={(event) => setLevel(event.target.value)} className={inputClassName}>
              <option value="">All levels</option>
              <option value="debug">debug</option>
              <option value="info">info</option>
              <option value="warning">warning</option>
              <option value="error">error</option>
            </select>
          </label>

          <label className="grid gap-2">
            <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Limit</span>
            <input value={limit} onChange={(event) => setLimit(event.target.value)} className={inputClassName} inputMode="numeric" />
          </label>
        </div>

        <DataTable
          rows={logs}
          rowKey={(log) => log.id}
          emptyTitle="No logs found"
          emptyDescription="Adjust the filters or wait for new backend activity."
          columns={[
            {
              key: "level",
              title: "Level",
              render: (log) => <StatusBadge status={log.level} />,
            },
            {
              key: "scope",
              title: "Scope",
              render: (log) => (
                <div className="grid gap-1">
                  <span className="font-medium text-white">{log.scope}</span>
                  <span className="text-xs text-slate-400">{formatDateTime(log.created_at)}</span>
                </div>
              ),
            },
            {
              key: "message",
              title: "Message",
              render: (log) => (
                <div className="max-w-[720px]">
                  <p className="text-slate-100">{log.message}</p>
                  {Object.keys(log.payload ?? {}).length ? (
                    <pre className="mt-2 overflow-x-auto rounded-xl border border-white/8 bg-slate-950/60 p-3 text-xs text-slate-400">
                      {prettyJson(log.payload)}
                    </pre>
                  ) : null}
                </div>
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
