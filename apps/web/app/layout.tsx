import type { ReactNode } from "react";
import type { Metadata } from "next";
import { IBM_Plex_Mono, Space_Grotesk } from "next/font/google";

import { Providers } from "@/app/providers";
import { NavLink } from "@/components/ui/nav-link";

import "./globals.css";

const displayFont = Space_Grotesk({ subsets: ["latin"], variable: "--font-display" });
const monoFont = IBM_Plex_Mono({ subsets: ["latin"], weight: ["400", "500"], variable: "--font-mono" });

export const metadata: Metadata = {
  title: "Trader MVP",
  description: "Research-first dashboard for Binance.US market data ingestion and recurring pattern discovery.",
};

const links = [
  { href: "/", label: "Research" },
  { href: "/data", label: "Data" },
  { href: "/features", label: "Feature Layer" },
  { href: "/patterns", label: "Patterns" },
  { href: "/pattern-registry", label: "Pattern Registry" },
  { href: "/logs", label: "Logs" },
];

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className={`${displayFont.variable} ${monoFont.variable} bg-[var(--color-bg)] font-sans text-slate-100`}>
        <Providers>
          <div className="min-h-screen bg-[radial-gradient(circle_at_top_right,rgba(56,189,248,0.12),transparent_30%),radial-gradient(circle_at_left_center,rgba(52,211,153,0.08),transparent_30%),linear-gradient(180deg,#07101b_0%,#050912_100%)]">
            <div className="mx-auto grid min-h-screen max-w-[1680px] grid-cols-1 gap-0 xl:grid-cols-[300px_minmax(0,1fr)]">
              <aside className="border-b border-white/10 bg-[rgba(7,12,20,0.74)] px-5 py-6 backdrop-blur xl:sticky xl:top-0 xl:h-screen xl:border-b-0 xl:border-r">
                <div className="xl:max-w-[220px]">
                  <p className="text-[11px] uppercase tracking-[0.24em] text-emerald-300">Binance.US quant desk</p>
                  <h1 className="mt-3 text-3xl font-semibold tracking-tight text-white">Trader MVP</h1>
                  <p className="mt-4 text-sm leading-6 text-slate-400">
                    Unified operator console for market data ingestion, pattern mining, and research-first validation over multi-timeframe history.
                  </p>
                </div>

                <nav className="mt-8 flex flex-wrap gap-3 xl:grid xl:grid-cols-1">
                  {links.map((link) => (
                    <NavLink key={link.href} href={link.href} label={link.label} />
                  ))}
                </nav>

                <div className="mt-8 grid gap-3 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Workspace</p>
                    <p className="mt-2 text-sm text-slate-300">Next.js App Router, React Query, Recharts, Tailwind CSS</p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Execution model</p>
                    <p className="mt-2 text-sm text-slate-300">Data sync, pattern research, and forward validation before any future execution layer</p>
                  </div>
                </div>
              </aside>

              <main className="px-4 py-5 sm:px-6 lg:px-8 xl:px-10 xl:py-8">
                <div className="mx-auto flex max-w-[1280px] flex-col gap-6">{children}</div>
              </main>
            </div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
