"use client";

import type { BacktestListItem, PatternScanRun } from "@/lib/types";

export type StrategyCandidateBrief = {
  strategyCode: string;
  priority: number;
  role: string;
  whyItMatters: string;
  nextStep: string;
  backtestFocus: string;
  paperFocus: string;
};

export type StrategyCandidateHit = {
  runId: number;
  lookbackDays: number;
  forwardBars: number;
  maxBarsPerSeries: number;
  verdict: string;
  sampleSize: number;
  avgNetReturnPct: number;
  avgForwardReturnPct: number;
  winRatePct: number;
};

export type StrategyCandidateRow = {
  key: string;
  strategyCode: string;
  priority: number;
  patternName: string;
  patternCode: string;
  symbol: string;
  timeframe: string;
  candidateHits: number;
  monitorHits: number;
  avgSampleSize: number;
  avgNetReturnPct: number;
  bestNetReturnPct: number;
  windows: string[];
  horizons: number[];
  role: string;
  whyItMatters: string;
  nextStep: string;
  backtestFocus: string;
  paperFocus: string;
  bestLookbackDays: number;
  bestForwardBars: number;
  bestMaxBarsPerSeries: number;
  bestHitVerdict: string;
  bestWinRatePct: number;
  baselineStatus: "pending" | "promoted" | "watch" | "archived_after_baseline";
  baselineReason: string;
  latestBacktest: BacktestListItem | null;
};

export const candidateBriefs: Record<string, StrategyCandidateBrief> = {
  "compression_release:AVAX-USDT:1h": {
    strategyCode: "avax_1h_compression_release",
    priority: 1,
    role: "Baseline 1h leader",
    whyItMatters: "Most repeated 1h candidate in the full matrix and one of the cleanest cross-horizon signals.",
    nextStep: "Use as the first reference model for the strategy layer.",
    backtestFocus: "Replay the longest supported window first, then compare 12-bar and 24-bar exits for stability.",
    paperFocus: "Run as the first 1h forward-paper candidate and watch signal quality, net edge, and regime sensitivity.",
  },
  "flush_reclaim:1INCH-USDT:1h": {
    strategyCode: "oneinch_1h_flush_reclaim",
    priority: 2,
    role: "Best 1h reclaim",
    whyItMatters: "Repeats across all major windows and stays economically strong instead of fading after one good run.",
    nextStep: "Promote as the main reclaim-continuation candidate.",
    backtestFocus: "Use the strongest reclaim window as the baseline replay, then stress-test shorter lookbacks for drift.",
    paperFocus: "Paper-test as a reclaim-continuation setup and watch whether newer tape keeps the same follow-through.",
  },
  "range_breakout:GALA-USDT:1h": {
    strategyCode: "gala_1h_range_breakout",
    priority: 3,
    role: "Fast 1h breakout",
    whyItMatters: "Clean repeated breakout behavior with strong net returns on the shorter and medium horizons.",
    nextStep: "Frame as the first fast impulse breakout prototype.",
    backtestFocus: "Compare the fastest validated breakout horizon against the strongest longer-window replay.",
    paperFocus: "Forward-test as an impulse model and track how often signals degrade outside the original breakout regime.",
  },
  "compression_release:ADA-USDT:1h": {
    strategyCode: "ada_1h_compression_release",
    priority: 4,
    role: "Stable 1h compression",
    whyItMatters: "Less flashy than the top two, but repeatable enough to deserve a full strategy candidate pass.",
    nextStep: "Keep in the primary 1h pool and validate exits.",
    backtestFocus: "Backtest as a slower 1h compression setup with conservative exit comparisons.",
    paperFocus: "Paper-test to confirm that the setup still emits clean signals without overtrading.",
  },
  "compression_release:GALA-USDT:1h": {
    strategyCode: "gala_1h_compression_release",
    priority: 5,
    role: "Secondary GALA 1h setup",
    whyItMatters: "Blends repeated candidate and monitor behavior, which suggests a real edge with some regime sensitivity.",
    nextStep: "Compare directly against GALA 1h breakout and decide whether to merge or split the model.",
    backtestFocus: "Replay side-by-side with the 1h breakout variant to see whether they are distinct or redundant.",
    paperFocus: "Paper-test only after the breakout variant so we can compare frequency and overlap cleanly.",
  },
  "range_breakout:BNB-USDT:4h": {
    strategyCode: "bnb_4h_range_breakout",
    priority: 6,
    role: "Slow structural breakout",
    whyItMatters: "Gives you a higher-timeframe anchor that is calmer and easier to reason about than the fast intraday setups.",
    nextStep: "Turn into a 4h structural breakout candidate with conservative exits.",
    backtestFocus: "Run a full-window structural replay and test whether the edge survives slower exits.",
    paperFocus: "Paper-test as a low-frequency anchor setup and judge quality over several weeks rather than days.",
  },
  "flush_reclaim:1INCH-USDT:4h": {
    strategyCode: "oneinch_4h_flush_reclaim",
    priority: 7,
    role: "Higher-timeframe reclaim",
    whyItMatters: "Useful as a slower counterpart to the 1h reclaim model on the same symbol.",
    nextStep: "Validate as the higher-timeframe confirmation variant.",
    backtestFocus: "Backtest as the slower confirmation version of the 1h reclaim logic.",
    paperFocus: "Paper-test as a higher-timeframe confirmation candidate with low expected signal count.",
  },
  "flush_reclaim:GALA-USDT:5m": {
    strategyCode: "gala_5m_flush_reclaim",
    priority: 8,
    role: "Best intraday candidate",
    whyItMatters: "The strongest lower-timeframe result in the matrix, especially on the longer windows and horizons.",
    nextStep: "Promote as the first serious intraday strategy candidate.",
    backtestFocus: "Replay on the longest windows first because this setup only really opened up on 12-bar and 24-bar horizons.",
    paperFocus: "Forward-paper as the lead intraday setup and watch slippage, signal frequency, and edge decay closely.",
  },
  "flush_reclaim:IOTA-USDT:5m": {
    strategyCode: "iota_5m_flush_reclaim",
    priority: 9,
    role: "Second intraday reclaim",
    whyItMatters: "Not as explosive as GALA, but it carries a stronger sample base and more measured economics.",
    nextStep: "Use as the comparison intraday reclaim model.",
    backtestFocus: "Backtest as the steadier intraday comparison model beside GALA 5m.",
    paperFocus: "Paper-test after GALA 5m so we can compare frequency versus stability.",
  },
  "flush_reclaim:IOTA-USDT:15m": {
    strategyCode: "iota_15m_flush_reclaim",
    priority: 10,
    role: "Bridge setup",
    whyItMatters: "Sits between the 5m and 1h layers and helps us compare how reclaim behavior changes with speed.",
    nextStep: "Keep as the bridge candidate between intraday and mid-timeframe strategy logic.",
    backtestFocus: "Replay as the bridge model between the 5m and 1h layers and compare its exit profile.",
    paperFocus: "Paper-test as the bridge setup to see whether it behaves closer to intraday or swing tempo in real time.",
  },
};

export function aggregateApprovedStrategyCandidates(runs: PatternScanRun[]) {
  const registry = new Map<string, StrategyCandidateRow>();

  for (const run of runs) {
    const report = run.report_summary;
    if (!report) {
      continue;
    }

    for (const pattern of report.patterns) {
      if (pattern.verdict !== "candidate" && pattern.verdict !== "monitor") {
        continue;
      }

      const key = `${pattern.pattern_code}:${pattern.symbol}:${pattern.timeframe}`;
      const brief = candidateBriefs[key];
      if (!brief) {
        continue;
      }

      const currentNet = Number(pattern.avg_net_return_pct ?? 0);
      const currentWinRate = Number(pattern.win_rate_pct ?? 0);
      const sampleSize = Number(pattern.sample_size ?? 0);
      const windowLabel = `${run.lookback_days}d`;
      const existing = registry.get(key);

      if (!existing) {
        registry.set(key, {
          key,
          strategyCode: brief.strategyCode,
          priority: brief.priority,
          patternName: pattern.pattern_name,
          patternCode: pattern.pattern_code,
          symbol: pattern.symbol,
          timeframe: pattern.timeframe,
          candidateHits: pattern.verdict === "candidate" ? 1 : 0,
          monitorHits: pattern.verdict === "monitor" ? 1 : 0,
          avgSampleSize: sampleSize,
          avgNetReturnPct: currentNet,
          bestNetReturnPct: currentNet,
          windows: [windowLabel],
          horizons: [run.forward_bars],
          role: brief.role,
          whyItMatters: brief.whyItMatters,
          nextStep: brief.nextStep,
          backtestFocus: brief.backtestFocus,
          paperFocus: brief.paperFocus,
          bestLookbackDays: run.lookback_days,
          bestForwardBars: run.forward_bars,
          bestMaxBarsPerSeries: run.max_bars_per_series,
          bestHitVerdict: pattern.verdict,
          bestWinRatePct: currentWinRate,
          baselineStatus: "pending",
          baselineReason: "No baseline replay has been recorded yet.",
          latestBacktest: null,
        });
        continue;
      }

      const totalHits = existing.candidateHits + existing.monitorHits + 1;
      const previousHits = totalHits - 1;
      const candidateHits = existing.candidateHits + (pattern.verdict === "candidate" ? 1 : 0);
      const isBetterHit =
        compareHitStrength(
          {
            verdict: pattern.verdict,
            avgNetReturnPct: currentNet,
            sampleSize,
            lookbackDays: run.lookback_days,
            forwardBars: run.forward_bars,
          },
          {
            verdict: existing.bestHitVerdict,
            avgNetReturnPct: existing.bestNetReturnPct,
            sampleSize: existing.avgSampleSize,
            lookbackDays: existing.bestLookbackDays,
            forwardBars: existing.bestForwardBars,
          },
        ) > 0;

      registry.set(key, {
        ...existing,
        candidateHits,
        monitorHits: existing.monitorHits + (pattern.verdict === "monitor" ? 1 : 0),
        avgSampleSize: (existing.avgSampleSize * previousHits + sampleSize) / totalHits,
        avgNetReturnPct: (existing.avgNetReturnPct * previousHits + currentNet) / totalHits,
        bestNetReturnPct: Math.max(existing.bestNetReturnPct, currentNet),
        windows: uniqueSortedStrings([...existing.windows, windowLabel]),
        horizons: uniqueSortedNumbers([...existing.horizons, run.forward_bars]),
        bestLookbackDays: isBetterHit ? run.lookback_days : existing.bestLookbackDays,
        bestForwardBars: isBetterHit ? run.forward_bars : existing.bestForwardBars,
        bestMaxBarsPerSeries: isBetterHit ? run.max_bars_per_series : existing.bestMaxBarsPerSeries,
        bestHitVerdict: isBetterHit ? pattern.verdict : existing.bestHitVerdict,
        bestWinRatePct: isBetterHit ? currentWinRate : existing.bestWinRatePct,
      });
    }
  }

  return Array.from(registry.values()).sort((left, right) => {
    if (left.priority !== right.priority) {
      return left.priority - right.priority;
    }
    if (right.candidateHits !== left.candidateHits) {
      return right.candidateHits - left.candidateHits;
    }
    return right.avgNetReturnPct - left.avgNetReturnPct;
  });
}

export function applyBaselineBacktestVerdicts(
  candidates: StrategyCandidateRow[],
  backtests: BacktestListItem[],
) {
  const latestBacktestByStrategy = new Map<string, BacktestListItem>();

  for (const row of backtests) {
    if (!latestBacktestByStrategy.has(row.strategy_code)) {
      latestBacktestByStrategy.set(row.strategy_code, row);
    }
  }

  return candidates.map((candidate) => {
    const latestBacktest = latestBacktestByStrategy.get(candidate.strategyCode) ?? null;
    if (!latestBacktest) {
      return candidate;
    }

    const totalReturnPct = Number(latestBacktest.total_return_pct ?? 0);
    const maxDrawdownPct = Number(latestBacktest.max_drawdown_pct ?? 0);
    const totalTrades = Number(latestBacktest.total_trades ?? 0);

    let baselineStatus: StrategyCandidateRow["baselineStatus"] = "watch";
    let baselineReason = `Baseline replay returned ${formatSignedPercent(totalReturnPct)} with ${formatPercentValue(maxDrawdownPct)} drawdown across ${totalTrades} trades.`;

    if (latestBacktest.status !== "completed") {
      baselineStatus = "pending";
      baselineReason =
        latestBacktest.status === "failed"
          ? latestBacktest.error_text?.trim() || "Baseline replay failed before completion."
          : `Baseline replay is still ${latestBacktest.status}.`;
    } else if (totalReturnPct > 0 && maxDrawdownPct <= 3) {
      baselineStatus = "promoted";
      baselineReason = `Baseline replay held up well: ${formatSignedPercent(totalReturnPct)} return, ${formatPercentValue(maxDrawdownPct)} drawdown, ${totalTrades} trades.`;
    } else if (totalReturnPct > -3 && maxDrawdownPct <= 4) {
      baselineStatus = "watch";
      baselineReason = `Baseline replay stayed near flat: ${formatSignedPercent(totalReturnPct)} return, ${formatPercentValue(maxDrawdownPct)} drawdown, ${totalTrades} trades.`;
    } else {
      baselineStatus = "archived_after_baseline";
      baselineReason = `Baseline replay broke down: ${formatSignedPercent(totalReturnPct)} return, ${formatPercentValue(maxDrawdownPct)} drawdown, ${totalTrades} trades.`;
    }

    return {
      ...candidate,
      baselineStatus,
      baselineReason,
      latestBacktest,
    };
  });
}

export function timeframePaperWindow(timeframe: string) {
  if (timeframe === "4h") {
    return "3-6 weeks";
  }
  if (timeframe === "1h") {
    return "2-4 weeks";
  }
  if (timeframe === "15m") {
    return "10-14 days";
  }
  if (timeframe === "5m") {
    return "7-10 days";
  }
  return "5-7 days";
}

function compareHitStrength(
  left: {
    verdict: string;
    avgNetReturnPct: number;
    sampleSize: number;
    lookbackDays: number;
    forwardBars: number;
  },
  right: {
    verdict: string;
    avgNetReturnPct: number;
    sampleSize: number;
    lookbackDays: number;
    forwardBars: number;
  },
) {
  const leftVerdictScore = left.verdict === "candidate" ? 1 : 0;
  const rightVerdictScore = right.verdict === "candidate" ? 1 : 0;
  if (leftVerdictScore !== rightVerdictScore) {
    return leftVerdictScore - rightVerdictScore;
  }
  if (left.avgNetReturnPct !== right.avgNetReturnPct) {
    return left.avgNetReturnPct - right.avgNetReturnPct;
  }
  if (left.sampleSize !== right.sampleSize) {
    return left.sampleSize - right.sampleSize;
  }
  if (left.lookbackDays !== right.lookbackDays) {
    return left.lookbackDays - right.lookbackDays;
  }
  return left.forwardBars - right.forwardBars;
}

function uniqueSortedStrings(values: string[]) {
  return Array.from(new Set(values)).sort((left, right) => Number.parseInt(left, 10) - Number.parseInt(right, 10));
}

function uniqueSortedNumbers(values: number[]) {
  return Array.from(new Set(values)).sort((left, right) => left - right);
}

function formatSignedPercent(value: number) {
  const normalized = value >= 0 ? `+${value.toFixed(2)}` : value.toFixed(2);
  return `${normalized}%`;
}

function formatPercentValue(value: number) {
  return `${value.toFixed(2)}%`;
}
