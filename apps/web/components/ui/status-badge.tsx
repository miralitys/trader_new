import { cn } from "@/lib/utils";

type StatusBadgeProps = {
  status: string | null | undefined;
};

const statusStyles: Record<string, string> = {
  running: "border-emerald-400/30 bg-emerald-500/10 text-emerald-200",
  completed: "border-emerald-400/30 bg-emerald-500/10 text-emerald-200",
  active: "border-emerald-400/30 bg-emerald-500/10 text-emerald-200",
  ready: "border-emerald-400/30 bg-emerald-500/10 text-emerald-200",
  candidate: "border-emerald-400/30 bg-emerald-500/10 text-emerald-200",
  enter: "border-emerald-400/30 bg-emerald-500/10 text-emerald-200",
  idle: "border-slate-500/30 bg-slate-500/10 text-slate-300",
  stopped: "border-slate-500/30 bg-slate-500/10 text-slate-300",
  created: "border-slate-500/30 bg-slate-500/10 text-slate-300",
  hold: "border-slate-500/30 bg-slate-500/10 text-slate-300",
  queued: "border-amber-400/30 bg-amber-500/10 text-amber-100",
  failed: "border-rose-400/30 bg-rose-500/10 text-rose-100",
  exit: "border-rose-400/30 bg-rose-500/10 text-rose-100",
  error: "border-rose-400/30 bg-rose-500/10 text-rose-100",
  open: "border-sky-400/30 bg-sky-500/10 text-sky-100",
  closed: "border-slate-500/30 bg-slate-500/10 text-slate-300",
  long: "border-sky-400/30 bg-sky-500/10 text-sky-100",
  info: "border-sky-400/30 bg-sky-500/10 text-sky-100",
  debug: "border-slate-500/30 bg-slate-500/10 text-slate-300",
  warning: "border-amber-400/30 bg-amber-500/10 text-amber-100",
  insufficient_history: "border-amber-400/30 bg-amber-500/10 text-amber-100",
  insufficient_sample: "border-amber-400/30 bg-amber-500/10 text-amber-100",
  monitor: "border-sky-400/30 bg-sky-500/10 text-sky-100",
  not_profitable: "border-rose-400/30 bg-rose-500/10 text-rose-100",
};

export function StatusBadge({ status }: StatusBadgeProps) {
  const normalized = (status ?? "unknown").toLowerCase();

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.18em]",
        statusStyles[normalized] ?? "border-white/10 bg-white/[0.05] text-slate-300",
      )}
    >
      {normalized.replaceAll("_", " ")}
    </span>
  );
}
