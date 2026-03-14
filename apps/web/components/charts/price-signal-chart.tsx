"use client";

import { ComposedChart, Line, ResponsiveContainer, Scatter, Tooltip, XAxis, YAxis } from "recharts";

type PricePoint = {
  time: string;
  close: number;
  enter?: number | null;
  exit?: number | null;
};

type PriceSignalChartProps = {
  data: PricePoint[];
};

export function PriceSignalChart({ data }: PriceSignalChartProps) {
  if (!data.length) {
    return <div className="empty-state">Signal chart will populate after trades and signals appear.</div>;
  }

  return (
    <div className="chart-shell">
      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={data}>
          <XAxis dataKey="time" tick={{ fill: "#8ea3c0", fontSize: 11 }} minTickGap={28} />
          <YAxis tick={{ fill: "#8ea3c0", fontSize: 11 }} />
          <Tooltip
            contentStyle={{ background: "#0f1626", border: "1px solid #26324d", borderRadius: "12px" }}
          />
          <Line type="monotone" dataKey="close" stroke="#7aa2ff" dot={false} strokeWidth={2} />
          <Scatter dataKey="enter" fill="#00d4a3" />
          <Scatter dataKey="exit" fill="#ff6b81" />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
