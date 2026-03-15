"use client";

const dayPresets = [30, 60, 90, 180, 365, 730] as const;

type DateRangePresetsProps = {
  onSelect: (days: number) => void;
};

export function DateRangePresets({ onSelect }: DateRangePresetsProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {dayPresets.map((days) => (
        <button
          key={days}
          type="button"
          onClick={() => onSelect(days)}
          className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1.5 text-sm font-medium text-slate-200 transition hover:border-sky-400/30 hover:bg-sky-500/10 hover:text-sky-100"
        >
          {days}
        </button>
      ))}
      <span className="self-center text-xs text-slate-500">days</span>
    </div>
  );
}
