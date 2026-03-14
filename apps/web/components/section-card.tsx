import { ReactNode } from "react";

import { cn } from "@/lib/utils";

type SectionCardProps = {
  title: string;
  eyebrow?: string;
  children: ReactNode;
  actions?: ReactNode;
  className?: string;
};

export function SectionCard({ title, eyebrow, children, actions, className }: SectionCardProps) {
  return (
    <section
      className={cn(
        "rounded-[28px] border border-white/10 bg-[rgba(8,14,24,0.85)] p-5 shadow-[0_18px_60px_rgba(0,0,0,0.26)] backdrop-blur",
        className,
      )}
    >
      <div className="mb-5 flex flex-col gap-4 border-b border-white/6 pb-4 lg:flex-row lg:items-center lg:justify-between">
        <div>
          {eyebrow ? <p className="text-[11px] uppercase tracking-[0.24em] text-emerald-300">{eyebrow}</p> : null}
          <h2 className="mt-2 text-xl font-semibold tracking-tight text-white">{title}</h2>
        </div>
        {actions ? <div>{actions}</div> : null}
      </div>
      {children}
    </section>
  );
}
