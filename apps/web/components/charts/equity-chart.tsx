"use client";

import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { EquityPoint } from "@/lib/types";
import { formatCurrency, formatDateTime, toNumber } from "@/lib/utils";

type EquityChartProps = {
  data: EquityPoint[];
};

export function EquityChart({ data }: EquityChartProps) {
  if (!data.length) {
    return <div className="rounded-2xl border border-dashed border-white/10 px-4 py-10 text-center text-sm text-slate-400">No equity curve data available for this run.</div>;
  }

  const chartData = data.map((point) => ({
    timestamp: formatDateTime(point.timestamp),
    equity: toNumber(point.equity),
  }));

  return (
    <div className="h-[320px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={chartData} margin={{ top: 12, right: 16, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="equity-area" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#34d399" stopOpacity={0.34} />
              <stop offset="100%" stopColor="#34d399" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke="rgba(148, 163, 184, 0.12)" vertical={false} />
          <XAxis
            dataKey="timestamp"
            tick={{ fill: "#94a3b8", fontSize: 11 }}
            tickLine={false}
            axisLine={{ stroke: "rgba(148, 163, 184, 0.14)" }}
            minTickGap={18}
          />
          <YAxis
            tick={{ fill: "#94a3b8", fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            tickFormatter={(value) => formatCurrency(value)}
            width={84}
          />
          <Tooltip
            cursor={{ stroke: "rgba(148, 163, 184, 0.18)" }}
            contentStyle={{
              backgroundColor: "#0b1220",
              borderColor: "rgba(148, 163, 184, 0.16)",
              borderRadius: "16px",
              color: "#e2e8f0",
            }}
            formatter={(value: number) => formatCurrency(value)}
          />
          <Area type="monotone" dataKey="equity" stroke="#34d399" fill="url(#equity-area)" strokeWidth={2.25} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
