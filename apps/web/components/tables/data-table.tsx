import type { ReactNode } from "react";

import { EmptyState } from "@/components/ui/empty-state";
import { cn } from "@/lib/utils";

type Column<T> = {
  key: string;
  title: string;
  render: (row: T) => ReactNode;
  className?: string;
};

type DataTableProps<T> = {
  columns: Column<T>[];
  rows: T[];
  rowKey?: (row: T, rowIndex: number) => string | number;
  emptyTitle?: string;
  emptyDescription?: string;
  className?: string;
};

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  emptyTitle = "No rows found",
  emptyDescription = "Try adjusting the current filters or trigger new activity.",
  className,
}: DataTableProps<T>) {
  return (
    <div className={cn("overflow-hidden rounded-2xl border border-white/10", className)}>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-white/10 text-left text-sm">
          <thead className="bg-white/[0.04]">
            <tr>
              {columns.map((column) => (
                <th
                  key={column.key}
                  className={cn(
                    "whitespace-nowrap px-4 py-3 text-[11px] font-medium uppercase tracking-[0.18em] text-slate-400",
                    column.className,
                  )}
                >
                  {column.title}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-white/6 bg-[rgba(7,12,20,0.55)]">
            {rows.length ? (
              rows.map((row, rowIndex) => (
                <tr key={rowKey ? rowKey(row, rowIndex) : rowIndex} className="align-top hover:bg-white/[0.025]">
                  {columns.map((column) => (
                    <td key={column.key} className={cn("px-4 py-3 text-slate-200", column.className)}>
                      {column.render(row)}
                    </td>
                  ))}
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={columns.length} className="p-5">
                  <EmptyState title={emptyTitle} description={emptyDescription} />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
