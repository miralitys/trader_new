from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

from dateutil.relativedelta import relativedelta

from app.engines.backtest_engine import BacktestEngine
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle
from app.schemas.backtest import BacktestResponse
from app.scripts.benchmark_strategy_categories import (
    aggregate_results,
    fetch_candles,
    parse_datetime,
    parse_symbols,
    run_backtest,
)
from app.strategies.pullback_in_trend_swing import PullbackInTrendSwingStrategy

UTC = timezone.utc


@dataclass(frozen=True)
class ResearchPreset:
    label: str
    symbols: tuple[str, ...]
    overrides: dict[str, Any]
    symbol_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class ResearchVariant:
    label: str
    overrides: dict[str, Any]


@dataclass(frozen=True)
class WalkForwardFold:
    index: int
    start_at: datetime
    end_at: datetime

    def as_dict(self) -> dict[str, str | int]:
        return {
            "index": self.index,
            "start_at": self.start_at.isoformat(),
            "end_at": self.end_at.isoformat(),
        }


def build_research_presets() -> dict[str, ResearchPreset]:
    return {
        "btc_eth_baseline_24h": ResearchPreset(
            label="btc_eth_baseline_24h",
            symbols=("BTC-USDT", "ETH-USDT"),
            overrides={},
        ),
        "btc_only_baseline_24h": ResearchPreset(
            label="btc_only_baseline_24h",
            symbols=("BTC-USDT",),
            overrides={},
        ),
        "eth_only_baseline_24h": ResearchPreset(
            label="eth_only_baseline_24h",
            symbols=("ETH-USDT",),
            overrides={},
        ),
        "eth_only_context_tighter_24h": ResearchPreset(
            label="eth_only_context_tighter_24h",
            symbols=("ETH-USDT",),
            overrides={
                "max_close_above_ema20_1h_pct": 0.025,
                "max_close_above_ema50_1h_pct": 0.04,
            },
        ),
        "eth_only_context_rsi55_24h": ResearchPreset(
            label="eth_only_context_rsi55_24h",
            symbols=("ETH-USDT",),
            overrides={
                "max_close_above_ema20_1h_pct": 0.025,
                "max_close_above_ema50_1h_pct": 0.04,
                "min_htf_rsi": 55.0,
            },
        ),
        "btc_eth_split_best_24h": ResearchPreset(
            label="btc_eth_split_best_24h",
            symbols=("BTC-USDT", "ETH-USDT"),
            overrides={},
            symbol_overrides={
                "ETH-USDT": {
                    "max_close_above_ema20_1h_pct": 0.025,
                    "max_close_above_ema50_1h_pct": 0.04,
                    "min_htf_rsi": 55.0,
                }
            },
        ),
        "btc_eth_split_eth_rsi55_candidate_24h": ResearchPreset(
            label="btc_eth_split_eth_rsi55_candidate_24h",
            symbols=("BTC-USDT", "ETH-USDT"),
            overrides={},
            symbol_overrides={
                "ETH-USDT": {
                    "max_close_above_ema20_1h_pct": 0.025,
                    "max_close_above_ema50_1h_pct": 0.04,
                    "min_htf_rsi": 55.0,
                }
            },
        ),
        "btc_eth_72h": ResearchPreset(
            label="btc_eth_72h",
            symbols=("BTC-USDT", "ETH-USDT"),
            overrides={"max_bars_in_trade": 288},
        ),
        "btc_eth_no_4h": ResearchPreset(
            label="btc_eth_no_4h",
            symbols=("BTC-USDT", "ETH-USDT"),
            overrides={"require_4h_trend_confirmation": False},
        ),
        "sol_exploratory_24h": ResearchPreset(
            label="sol_exploratory_24h",
            symbols=("SOL-USDT",),
            overrides={},
        ),
    }


def build_walkforward_sensitivity_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(label="target_r_1_50", overrides={"target_r_multiple": 1.5}),
        ResearchVariant(label="target_r_1_80", overrides={"target_r_multiple": 1.8}),
        ResearchVariant(label="htf_rsi_48", overrides={"min_htf_rsi": 48.0}),
        ResearchVariant(label="htf_rsi_50", overrides={"min_htf_rsi": 50.0}),
        ResearchVariant(label="htf_rsi_55", overrides={"min_htf_rsi": 55.0}),
        ResearchVariant(label="min_atr_1h_0", overrides={"min_atr_pct_1h": 0.0}),
        ResearchVariant(label="no_4h_confirmation", overrides={"require_4h_trend_confirmation": False}),
    )


def build_btc_only_frequency_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(label="impulse_return_1_8pct", overrides={"impulse_min_return_pct": 0.018}),
        ResearchVariant(label="htf_rsi_50", overrides={"min_htf_rsi": 50.0}),
        ResearchVariant(label="min_atr_1h_0_2pct", overrides={"min_atr_pct_1h": 0.002}),
        ResearchVariant(label="retrace_0_60", overrides={"max_impulse_retrace_ratio": 0.60}),
        ResearchVariant(label="close_near_high_0_55", overrides={"close_near_high_threshold": 0.55}),
        ResearchVariant(label="max_4h_extension_0_10", overrides={"max_distance_above_ema20_4h": 0.10}),
    )


def build_btc_failure_cluster_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(label="htf_rsi_55", overrides={"min_htf_rsi": 55.0}),
        ResearchVariant(label="impulse_return_2_2pct", overrides={"impulse_min_return_pct": 0.022}),
        ResearchVariant(label="impulse_body_1_5pct", overrides={"impulse_min_body_pct": 0.015}),
        ResearchVariant(label="retrace_0_45", overrides={"max_impulse_retrace_ratio": 0.45}),
        ResearchVariant(label="close_near_high_0_70", overrides={"close_near_high_threshold": 0.70}),
        ResearchVariant(label="trigger_body_0_15pct", overrides={"trigger_min_body_pct": 0.0015}),
        ResearchVariant(label="max_4h_extension_0_06", overrides={"max_distance_above_ema20_4h": 0.06}),
    )


def build_btc_stop_geometry_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(label="max_stop_4pct", overrides={"max_stop_pct": 0.04}),
        ResearchVariant(label="max_stop_3pct", overrides={"max_stop_pct": 0.03}),
        ResearchVariant(
            label="impulse_2_2pct_plus_stop_4pct",
            overrides={"impulse_min_return_pct": 0.022, "max_stop_pct": 0.04},
        ),
        ResearchVariant(
            label="retrace_0_45_plus_stop_4pct",
            overrides={"max_impulse_retrace_ratio": 0.45, "max_stop_pct": 0.04},
        ),
    )


def build_eth_only_quality_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(label="impulse_return_2_2pct", overrides={"impulse_min_return_pct": 0.022}),
        ResearchVariant(label="htf_rsi_55", overrides={"min_htf_rsi": 55.0}),
        ResearchVariant(label="retrace_0_50", overrides={"max_impulse_retrace_ratio": 0.50}),
        ResearchVariant(label="close_near_high_0_70", overrides={"close_near_high_threshold": 0.70}),
        ResearchVariant(label="trigger_body_0_15pct", overrides={"trigger_min_body_pct": 0.0015}),
        ResearchVariant(
            label="context_extension_tighter",
            overrides={
                "max_close_above_ema20_1h_pct": 0.025,
                "max_close_above_ema50_1h_pct": 0.04,
            },
        ),
        ResearchVariant(label="max_4h_extension_0_07", overrides={"max_distance_above_ema20_4h": 0.07}),
    )


def build_eth_only_early_defense_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(
            label="context_0_020_0_035",
            overrides={
                "max_close_above_ema20_1h_pct": 0.02,
                "max_close_above_ema50_1h_pct": 0.035,
            },
        ),
        ResearchVariant(
            label="context_0_020_0_030",
            overrides={
                "max_close_above_ema20_1h_pct": 0.02,
                "max_close_above_ema50_1h_pct": 0.03,
            },
        ),
        ResearchVariant(label="max_4h_extension_0_06", overrides={"max_distance_above_ema20_4h": 0.06}),
        ResearchVariant(
            label="context_0_020_0_035_plus_4h_0_06",
            overrides={
                "max_close_above_ema20_1h_pct": 0.02,
                "max_close_above_ema50_1h_pct": 0.035,
                "max_distance_above_ema20_4h": 0.06,
            },
        ),
        ResearchVariant(label="htf_rsi_55", overrides={"min_htf_rsi": 55.0}),
        ResearchVariant(
            label="context_0_020_0_035_plus_rsi_55",
            overrides={
                "max_close_above_ema20_1h_pct": 0.02,
                "max_close_above_ema50_1h_pct": 0.035,
                "min_htf_rsi": 55.0,
            },
        ),
        ResearchVariant(label="trigger_body_0_20pct", overrides={"trigger_min_body_pct": 0.002}),
        ResearchVariant(label="retrace_0_45", overrides={"max_impulse_retrace_ratio": 0.45}),
    )


def build_eth_failure_cluster_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(
            label="context_0_020_0_035",
            overrides={
                "max_close_above_ema20_1h_pct": 0.02,
                "max_close_above_ema50_1h_pct": 0.035,
            },
        ),
        ResearchVariant(label="max_4h_extension_0_06", overrides={"max_distance_above_ema20_4h": 0.06}),
        ResearchVariant(
            label="context_0_020_0_035_plus_4h_0_06",
            overrides={
                "max_close_above_ema20_1h_pct": 0.02,
                "max_close_above_ema50_1h_pct": 0.035,
                "max_distance_above_ema20_4h": 0.06,
            },
        ),
        ResearchVariant(label="impulse_return_2_2pct", overrides={"impulse_min_return_pct": 0.022}),
        ResearchVariant(label="retrace_0_45", overrides={"max_impulse_retrace_ratio": 0.45}),
        ResearchVariant(label="trigger_body_0_20pct", overrides={"trigger_min_body_pct": 0.002}),
        ResearchVariant(label="close_near_high_0_70", overrides={"close_near_high_threshold": 0.70}),
    )


def build_eth_stop_geometry_variants() -> tuple[ResearchVariant, ...]:
    return (
        ResearchVariant(label="baseline", overrides={}),
        ResearchVariant(label="max_stop_4pct", overrides={"max_stop_pct": 0.04}),
        ResearchVariant(label="max_stop_3pct", overrides={"max_stop_pct": 0.03}),
        ResearchVariant(label="retrace_0_45", overrides={"max_impulse_retrace_ratio": 0.45}),
        ResearchVariant(
            label="retrace_0_45_plus_stop_4pct",
            overrides={"max_impulse_retrace_ratio": 0.45, "max_stop_pct": 0.04},
        ),
    )


def parse_args() -> argparse.Namespace:
    presets = build_research_presets()
    parser = argparse.ArgumentParser(description="Run curated research presets for PullbackInTrendSwing.")
    parser.add_argument(
        "--analysis",
        choices=("window", "walkforward", "walkforward_sensitivity"),
        default="window",
    )
    parser.add_argument("--preset", choices=sorted(presets.keys()), default="btc_eth_baseline_24h")
    parser.add_argument("--symbols", help="Optional comma-separated symbol override.")
    parser.add_argument("--start-at", default="2025-09-17T03:15:00+00:00")
    parser.add_argument("--end-at", default="2026-03-16T03:15:00+00:00")
    parser.add_argument("--walkforward-start-at", default="2025-11-16T03:15:00+00:00")
    parser.add_argument("--test-months", type=int, default=1)
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument(
        "--variant-set",
        choices=(
            "default",
            "btc_only_frequency",
            "btc_failure_cluster",
            "btc_stop_geometry",
            "eth_only_quality",
            "eth_only_early_defense",
            "eth_failure_cluster",
            "eth_stop_geometry",
        ),
        default="default",
    )
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--position-size-pct", default="1")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--retry-attempts", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=10)
    parser.add_argument("--output")
    return parser.parse_args()


def summarize_result(result: BacktestResponse) -> dict[str, Any]:
    hold_bars: list[int] = []
    exit_counts: Counter[str] = Counter()
    for trade in result.trades:
        bars = int((trade.exit_time - trade.entry_time).total_seconds() // (15 * 60))
        hold_bars.append(bars)
        exit_counts[trade.exit_reason] += 1

    diagnostics = result.diagnostics or {}
    hold_reasons = diagnostics.get("entry_hold_reasons") or {}
    regime_details = diagnostics.get("regime_blocked_details") or {}
    top_hold_reasons = sorted(
        ((key, value) for key, value in hold_reasons.items() if value),
        key=lambda item: (-item[1], item[0]),
    )[:8]
    top_regime_details = sorted(
        regime_details.items(),
        key=lambda item: (-item[1], item[0]),
    )[:5]
    return {
        "symbol": result.symbol,
        "trades": result.metrics.total_trades,
        "return_pct": round(float(result.metrics.total_return_pct), 2),
        "max_drawdown_pct": round(float(result.metrics.max_drawdown_pct), 2),
        "win_rate_pct": round(float(result.metrics.win_rate_pct), 2),
        "profit_factor": round(float(result.metrics.profit_factor), 2),
        "median_hold_bars": int(median(hold_bars)) if hold_bars else 0,
        "max_hold_bars": max(hold_bars) if hold_bars else 0,
        "exit_counts": dict(exit_counts),
        "top_hold_reasons": top_hold_reasons,
        "top_regime_details": top_regime_details,
    }


def build_walkforward_folds(
    start_at: datetime,
    end_at: datetime,
    test_months: int,
    folds: int,
) -> list[WalkForwardFold]:
    items: list[WalkForwardFold] = []
    cursor = start_at
    for index in range(folds):
        fold_end_at = cursor + relativedelta(months=test_months)
        if fold_end_at > end_at:
            break
        items.append(
            WalkForwardFold(
                index=index + 1,
                start_at=cursor,
                end_at=fold_end_at,
            )
        )
        cursor = fold_end_at
    return items


def _merge_overrides(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    return {
        **base,
        **extra,
    }


def _effective_symbol_overrides(
    preset: ResearchPreset,
    symbol: str,
    extra_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective = _merge_overrides(preset.overrides, preset.symbol_overrides.get(symbol, {}))
    if extra_overrides:
        effective = _merge_overrides(effective, extra_overrides)
    return effective


def _required_preroll_days(
    timeframe: str,
    overrides: dict[str, Any],
) -> int:
    strategy = PullbackInTrendSwingStrategy()
    strategy_config = strategy.parse_config(
        {
            **strategy.default_config(),
            **overrides,
        }
    )
    return strategy.required_preroll_days(timeframe, strategy_config)


def _fetch_candle_cache(
    *,
    symbols: list[str],
    timeframe: str,
    fetch_start: datetime,
    end_at: datetime,
    retry_attempts: int,
    timeout_seconds: int,
) -> dict[str, list[BacktestCandle]]:
    client = BinanceUSClient(
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )
    try:
        return {
            symbol: fetch_candles(client, symbol, timeframe, fetch_start, end_at)
            for symbol in symbols
        }
    finally:
        client.close()


def run_window(
    *,
    preset: ResearchPreset,
    symbols: list[str],
    timeframe: str,
    start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
    position_size_pct: Decimal,
    retry_attempts: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    preroll_days = max(
        _required_preroll_days(timeframe, _effective_symbol_overrides(preset, symbol))
        for symbol in symbols
    )
    fetch_start = start_at - timedelta(days=preroll_days)
    engine = BacktestEngine()
    candle_cache = _fetch_candle_cache(
        symbols=symbols,
        timeframe=timeframe,
        fetch_start=fetch_start,
        end_at=end_at,
        retry_attempts=retry_attempts,
        timeout_seconds=timeout_seconds,
    )

    results: list[BacktestResponse] = []
    per_symbol: list[dict[str, Any]] = []
    for symbol in symbols:
        effective_overrides = _effective_symbol_overrides(preset, symbol)
        result = run_backtest(
            engine=engine,
            strategy=PullbackInTrendSwingStrategy(),
            strategy_code=f"pullback_in_trend_swing:{preset.label}",
            symbol=symbol,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            candles=candle_cache[symbol],
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            overrides=effective_overrides,
            position_size_pct=position_size_pct,
        )
        results.append(result)
        per_symbol.append(summarize_result(result))

    return {
        "window": {
            "start_at": start_at.isoformat(),
            "end_at": end_at.isoformat(),
            "fetch_start": fetch_start.isoformat(),
            "warmup_days": preroll_days,
        },
        "aggregate": aggregate_results(results),
        "per_symbol": per_symbol,
        "loaded_candles": {symbol: len(candles) for symbol, candles in candle_cache.items()},
    }


def run_walkforward(
    *,
    preset: ResearchPreset,
    symbols: list[str],
    timeframe: str,
    walkforward_start_at: datetime,
    end_at: datetime,
    test_months: int,
    folds: int,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
    position_size_pct: Decimal,
    retry_attempts: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    preroll_days = max(
        _required_preroll_days(timeframe, _effective_symbol_overrides(preset, symbol))
        for symbol in symbols
    )
    fetch_start = walkforward_start_at - timedelta(days=preroll_days)
    walkforward_folds = build_walkforward_folds(
        start_at=walkforward_start_at,
        end_at=end_at,
        test_months=test_months,
        folds=folds,
    )
    if not walkforward_folds:
        raise ValueError("No walk-forward folds generated")

    candle_cache = _fetch_candle_cache(
        symbols=symbols,
        timeframe=timeframe,
        fetch_start=fetch_start,
        end_at=end_at,
        retry_attempts=retry_attempts,
        timeout_seconds=timeout_seconds,
    )

    engine = BacktestEngine()
    fold_reports: list[dict[str, Any]] = []
    all_results: list[BacktestResponse] = []
    for fold in walkforward_folds:
        fold_results: list[BacktestResponse] = []
        per_symbol: list[dict[str, Any]] = []
        for symbol in symbols:
            effective_overrides = _effective_symbol_overrides(preset, symbol)
            result = run_backtest(
                engine=engine,
                strategy=PullbackInTrendSwingStrategy(),
                strategy_code=f"pullback_in_trend_swing:{preset.label}:wf:{fold.index}",
                symbol=symbol,
                timeframe=timeframe,
                start_at=fold.start_at,
                end_at=fold.end_at,
                candles=candle_cache[symbol],
                initial_capital=initial_capital,
                fee=fee,
                slippage=slippage,
                overrides=effective_overrides,
                position_size_pct=position_size_pct,
            )
            fold_results.append(result)
            all_results.append(result)
            per_symbol.append(summarize_result(result))
        fold_reports.append(
            {
                "fold": fold.as_dict(),
                "aggregate": aggregate_results(fold_results),
                "per_symbol": per_symbol,
            }
        )

    profitable_folds = sum(
        1
        for report in fold_reports
        if float(report["aggregate"]["portfolio_return_pct"]) > 0.0
    )
    average_fold_return = 0.0
    if fold_reports:
        average_fold_return = sum(
            float(report["aggregate"]["portfolio_return_pct"])
            for report in fold_reports
        ) / float(len(fold_reports))

    return {
        "window": {
            "walkforward_start_at": walkforward_start_at.isoformat(),
            "end_at": end_at.isoformat(),
            "fetch_start": fetch_start.isoformat(),
            "warmup_days": preroll_days,
        },
        "folds": fold_reports,
        "summary": {
            "fold_count": len(fold_reports),
            "profitable_folds": profitable_folds,
            "average_fold_return_pct": round(average_fold_return, 4),
            "aggregate_test": aggregate_results(all_results),
        },
        "loaded_candles": {symbol: len(candles) for symbol, candles in candle_cache.items()},
    }


def run_walkforward_sensitivity(
    *,
    preset: ResearchPreset,
    symbols: list[str],
    timeframe: str,
    walkforward_start_at: datetime,
    end_at: datetime,
    test_months: int,
    folds: int,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
    position_size_pct: Decimal,
    retry_attempts: int,
    timeout_seconds: int,
    variant_set: str,
) -> dict[str, Any]:
    variants = (
        build_btc_only_frequency_variants()
        if variant_set == "btc_only_frequency"
        else build_btc_failure_cluster_variants()
        if variant_set == "btc_failure_cluster"
        else build_btc_stop_geometry_variants()
        if variant_set == "btc_stop_geometry"
        else build_eth_only_quality_variants()
        if variant_set == "eth_only_quality"
        else build_eth_only_early_defense_variants()
        if variant_set == "eth_only_early_defense"
        else build_eth_failure_cluster_variants()
        if variant_set == "eth_failure_cluster"
        else build_eth_stop_geometry_variants()
        if variant_set == "eth_stop_geometry"
        else build_walkforward_sensitivity_variants()
    )
    walkforward_folds = build_walkforward_folds(
        start_at=walkforward_start_at,
        end_at=end_at,
        test_months=test_months,
        folds=folds,
    )
    if not walkforward_folds:
        raise ValueError("No walk-forward folds generated")

    max_preroll_days = max(
        _required_preroll_days(timeframe, _effective_symbol_overrides(preset, symbol, variant.overrides))
        for variant in variants
        for symbol in symbols
    )
    fetch_start = walkforward_start_at - timedelta(days=max_preroll_days)
    candle_cache = _fetch_candle_cache(
        symbols=symbols,
        timeframe=timeframe,
        fetch_start=fetch_start,
        end_at=end_at,
        retry_attempts=retry_attempts,
        timeout_seconds=timeout_seconds,
    )

    engine = BacktestEngine()
    reports: list[dict[str, Any]] = []
    for variant in variants:
        fold_reports: list[dict[str, Any]] = []
        all_results: list[BacktestResponse] = []
        for fold in walkforward_folds:
            fold_results: list[BacktestResponse] = []
            for symbol in symbols:
                effective_overrides = _effective_symbol_overrides(preset, symbol, variant.overrides)
                result = run_backtest(
                    engine=engine,
                    strategy=PullbackInTrendSwingStrategy(),
                    strategy_code=f"pullback_in_trend_swing:{preset.label}:{variant.label}:wf:{fold.index}",
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=fold.start_at,
                    end_at=fold.end_at,
                    candles=candle_cache[symbol],
                    initial_capital=initial_capital,
                    fee=fee,
                    slippage=slippage,
                    overrides=effective_overrides,
                    position_size_pct=position_size_pct,
                )
                fold_results.append(result)
                all_results.append(result)
            fold_reports.append(
                {
                    "fold": fold.as_dict(),
                    "aggregate": aggregate_results(fold_results),
                }
            )

        profitable_folds = sum(
            1
            for report in fold_reports
            if float(report["aggregate"]["portfolio_return_pct"]) > 0.0
        )
        average_fold_return = 0.0
        if fold_reports:
            average_fold_return = sum(
                float(report["aggregate"]["portfolio_return_pct"])
                for report in fold_reports
            ) / float(len(fold_reports))
        aggregate_test = aggregate_results(all_results)
        reports.append(
            {
                "label": variant.label,
                "overrides": effective_overrides,
                "summary": {
                    "fold_count": len(fold_reports),
                    "profitable_folds": profitable_folds,
                    "average_fold_return_pct": round(average_fold_return, 4),
                    "aggregate_test": aggregate_test,
                },
                "folds": fold_reports,
            }
        )

    ranked = sorted(
        reports,
        key=lambda item: (
            item["summary"]["average_fold_return_pct"],
            item["summary"]["profitable_folds"],
            item["summary"]["aggregate_test"]["portfolio_return_pct"],
            -item["summary"]["aggregate_test"]["average_max_drawdown_pct"],
            item["summary"]["aggregate_test"]["profit_factor"],
        ),
        reverse=True,
    )
    return {
        "window": {
            "walkforward_start_at": walkforward_start_at.isoformat(),
            "end_at": end_at.isoformat(),
            "fetch_start": fetch_start.isoformat(),
            "warmup_days": max_preroll_days,
        },
        "variant_set": variant_set,
        "ranked_variants": ranked,
        "summary": {
            "tested_variants": len(ranked),
            "best_variant": ranked[0] if ranked else None,
            "worst_variant": ranked[-1] if ranked else None,
        },
        "loaded_candles": {symbol: len(candles) for symbol, candles in candle_cache.items()},
    }


def main() -> None:
    args = parse_args()
    presets = build_research_presets()
    preset = presets[args.preset]

    start_at = parse_datetime(args.start_at)
    end_at = parse_datetime(args.end_at)
    symbols = parse_symbols(args.symbols) if args.symbols else list(preset.symbols)
    timeframe = args.timeframe
    initial_capital = Decimal(args.initial_capital)
    fee = Decimal(args.fee)
    slippage = Decimal(args.slippage)
    position_size_pct = Decimal(args.position_size_pct)
    if args.analysis == "walkforward":
        walkforward_start_at = parse_datetime(args.walkforward_start_at)
        if not (walkforward_start_at < end_at):
            raise ValueError("Expected walkforward_start_at < end_at")
        analysis_report = run_walkforward(
            preset=preset,
            symbols=symbols,
            timeframe=timeframe,
            walkforward_start_at=walkforward_start_at,
            end_at=end_at,
            test_months=args.test_months,
            folds=args.folds,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            position_size_pct=position_size_pct,
            retry_attempts=args.retry_attempts,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.analysis == "walkforward_sensitivity":
        walkforward_start_at = parse_datetime(args.walkforward_start_at)
        if not (walkforward_start_at < end_at):
            raise ValueError("Expected walkforward_start_at < end_at")
        analysis_report = run_walkforward_sensitivity(
            preset=preset,
            symbols=symbols,
            timeframe=timeframe,
            walkforward_start_at=walkforward_start_at,
            end_at=end_at,
            test_months=args.test_months,
            folds=args.folds,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            position_size_pct=position_size_pct,
            retry_attempts=args.retry_attempts,
            timeout_seconds=args.timeout_seconds,
            variant_set=args.variant_set,
        )
    else:
        analysis_report = run_window(
            preset=preset,
            symbols=symbols,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            position_size_pct=position_size_pct,
            retry_attempts=args.retry_attempts,
            timeout_seconds=args.timeout_seconds,
        )
    output = {
        "strategy_key": PullbackInTrendSwingStrategy.key,
        "analysis": args.analysis,
        "preset": {
            "label": preset.label,
            "symbols": symbols,
            "timeframe": timeframe,
            "overrides": preset.overrides,
            "symbol_overrides": preset.symbol_overrides,
        },
        "friction": {
            "fee": str(fee),
            "slippage": str(slippage),
            "position_size_pct": str(position_size_pct),
        },
        "report": analysis_report,
    }

    payload = json.dumps(output, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
