import type { ReactNode } from "react";

type PageHeaderProps = {
  eyebrow: string;
  title: string;
  description: string;
  actions?: ReactNode;
};

export function PageHeader({ eyebrow, title, description, actions }: PageHeaderProps) {
  return (
    <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(37,99,235,0.12),transparent_42%),linear-gradient(180deg,rgba(255,255,255,0.03),transparent),rgba(7,12,20,0.85)] px-6 py-6 shadow-[0_22px_70px_rgba(0,0,0,0.32)]">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div className="max-w-3xl">
          <p className="text-[11px] uppercase tracking-[0.24em] text-emerald-300">{eyebrow}</p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-white md:text-4xl">{title}</h1>
          <p className="mt-3 text-sm leading-6 text-slate-300 md:text-base">{description}</p>
        </div>
        {actions ? <div className="flex flex-wrap items-center gap-3">{actions}</div> : null}
      </div>
    </section>
  );
}
