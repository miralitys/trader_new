import type { ReactNode } from "react";

import { cn } from "@/lib/utils";

type MetricCardProps = {
  label: string;
  value: ReactNode;
  hint?: ReactNode;
  tone?: "default" | "positive" | "warning" | "danger";
};

const toneStyles: Record<NonNullable<MetricCardProps["tone"]>, string> = {
  default: "border-white/10 bg-white/[0.04]",
  positive: "border-emerald-400/20 bg-emerald-500/[0.08]",
  warning: "border-amber-400/20 bg-amber-500/[0.08]",
  danger: "border-rose-400/20 bg-rose-500/[0.08]",
};

export function MetricCard({ label, value, hint, tone = "default" }: MetricCardProps) {
  return (
    <div className={cn("rounded-2xl border p-4", toneStyles[tone])}>
      <p className="text-[11px] uppercase tracking-[0.22em] text-slate-400">{label}</p>
      <div className="mt-3 text-2xl font-semibold tracking-tight text-white">{value}</div>
      {hint ? <div className="mt-2 text-sm text-slate-400">{hint}</div> : null}
    </div>
  );
}
