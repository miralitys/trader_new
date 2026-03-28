from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.scripts.benchmark_btc_cross_source_trend_sweep import (
    fetch_source_candles,
    evaluate_variant_on_source,
)
from app.scripts.benchmark_btc_sleeve_risk_controls import find_flat_priority_variant
from app.scripts.benchmark_regime_aware_adaptive_allocator import add_months
from app.scripts.benchmark_strategy_categories import CategoryVariant, parse_datetime, parse_symbols

UTC = timezone.utc
POSITION_SIZE_PCT = Decimal("1.00")
TRAIN_POSITION_SIZE_PCT = Decimal("0.10")
DRAWDOWN_GUARD_PCT = 4.0
COOLDOWN_PERIODS = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cross-source sweep for altcoin-ready flat-priority variants on ETH/SOL."
    )
    parser.add_argument("--symbols", default="ETH-USDT,SOL-USDT")
    parser.add_argument("--evaluation-start-at", default="2022-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--output")
    return parser.parse_args()


def common_first_candle_at(candles_by_source: dict[str, list[Any]]) -> datetime | None:
    first_times = []
    for candles in candles_by_source.values():
        if not candles:
            return None
        first_times.append(candles[0].open_time)
    return max(first_times) if first_times else None


def build_altcoin_variants() -> tuple[CategoryVariant, ...]:
    base = find_flat_priority_variant()
    base_overrides = deepcopy(base.overrides)

    regime_profiles = (
        (
            "alt_soft_regime",
            {
                "trend_min_gap_pct": 0.005,
                "trend_min_slope_pct": 0.0025,
                "flat_max_width_pct": 0.08,
                "flat_max_center_shift_pct": 0.012,
                "min_average_dollar_volume": 50000,
            },
        ),
        (
            "alt_balanced_regime",
            {
                "trend_min_gap_pct": 0.006,
                "trend_min_slope_pct": 0.003,
                "flat_max_width_pct": 0.075,
                "flat_max_center_shift_pct": 0.010,
                "min_average_dollar_volume": 100000,
            },
        ),
    )
    entry_profiles = (
        (
            "soft_breakout",
            {
                "min_trend_gap_pct": 0.005,
                "min_slow_ema_slope_pct": 0.003,
                "breakout_buffer_pct": 0.0005,
                "breakout_min_close_location": 0.55,
                "max_extension_above_fast_ema_pct": 0.03,
                "min_volume_multiple": 0.75,
                "min_average_dollar_volume": 50000,
            },
        ),
        (
            "soft_extension",
            {
                "min_trend_gap_pct": 0.006,
                "min_slow_ema_slope_pct": 0.003,
                "breakout_buffer_pct": 0.001,
                "breakout_min_close_location": 0.60,
                "max_extension_above_fast_ema_pct": 0.025,
                "min_volume_multiple": 0.80,
                "min_average_dollar_volume": 75000,
            },
        ),
        (
            "balanced_alt",
            {
                "min_trend_gap_pct": 0.007,
                "min_slow_ema_slope_pct": 0.004,
                "breakout_buffer_pct": 0.0015,
                "breakout_min_close_location": 0.65,
                "max_extension_above_fast_ema_pct": 0.02,
                "min_volume_multiple": 0.90,
                "min_average_dollar_volume": 100000,
            },
        ),
    )

    variants: list[CategoryVariant] = []
    for regime_label, regime_updates in regime_profiles:
        for entry_label, entry_updates in entry_profiles:
            overrides = deepcopy(base_overrides)
            overrides.update(regime_updates)
            trend_config = deepcopy(overrides["trend_config"])
            trend_config.update(entry_updates)
            overrides["trend_config"] = trend_config
            variants.append(
                CategoryVariant(
                    label=f"{regime_label}__{entry_label}",
                    overrides=overrides,
                )
            )
    return tuple(variants)


def summarize_symbol_result(
    *,
    symbol: str,
    variant: CategoryVariant,
    min_train_trades: int,
    effective_evaluation_start_at: datetime,
    by_source: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    us = by_source["binance_us"]
    archive = by_source["binance_archive_spot"]
    min_return_pct = min(float(us["total_return_pct"]), float(archive["total_return_pct"]))
    avg_return_pct = (float(us["total_return_pct"]) + float(archive["total_return_pct"])) / 2.0
    gap_return_pct = abs(float(us["total_return_pct"]) - float(archive["total_return_pct"]))
    avg_drawdown_pct = (float(us["max_drawdown_pct"]) + float(archive["max_drawdown_pct"])) / 2.0
    min_profit_factor = min(float(us["profit_factor"]), float(archive["profit_factor"]))
    both_sources_positive = float(us["total_return_pct"]) > 0.0 and float(archive["total_return_pct"]) > 0.0
    robustness_score = round(
        (2.0 if both_sources_positive else 0.0)
        + min_return_pct
        - (gap_return_pct * 0.30)
        - (avg_drawdown_pct * 0.10),
        6,
    )
    return {
        "symbol": symbol,
        "variant": {
            "label": variant.label,
            "overrides": variant.overrides,
        },
        "min_train_trades": min_train_trades,
        "effective_evaluation_start_at": effective_evaluation_start_at.isoformat(),
        "summary": {
            "binance_us_return_pct": round(float(us["total_return_pct"]), 4),
            "binance_archive_return_pct": round(float(archive["total_return_pct"]), 4),
            "binance_us_max_drawdown_pct": round(float(us["max_drawdown_pct"]), 4),
            "binance_archive_max_drawdown_pct": round(float(archive["max_drawdown_pct"]), 4),
            "binance_us_profit_factor": round(float(us["profit_factor"]), 4),
            "binance_archive_profit_factor": round(float(archive["profit_factor"]), 4),
            "binance_us_total_trades": int(us["total_trades"]),
            "binance_archive_total_trades": int(archive["total_trades"]),
            "min_return_pct": round(min_return_pct, 4),
            "avg_return_pct": round(avg_return_pct, 4),
            "gap_return_pct": round(gap_return_pct, 4),
            "avg_drawdown_pct": round(avg_drawdown_pct, 4),
            "min_profit_factor": round(min_profit_factor, 4),
            "both_sources_positive": both_sources_positive,
            "robustness_score": robustness_score,
        },
        "sources": by_source,
    }


def main() -> None:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))
    fetch_start_at = add_months(evaluation_start_at, -2) - timedelta(days=args.warmup_days)
    variants = build_altcoin_variants()

    reports: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for symbol in symbols:
        print(f"Evaluating altcoin cross-source sweep for {symbol}...")
        try:
            candles_by_source = fetch_source_candles(
                symbol=symbol,
                fetch_start_at=fetch_start_at,
                end_at=end_at,
            )
        except Exception as exc:  # pragma: no cover - research script guardrail
            skipped.append({"symbol": symbol, "reason": f"fetch_failed:{type(exc).__name__}:{exc}"})
            continue

        first_common = common_first_candle_at(candles_by_source)
        if first_common is None:
            skipped.append({"symbol": symbol, "reason": "no_common_candles"})
            continue

        effective_evaluation_start_at = max(evaluation_start_at, add_months(first_common, 2))
        if effective_evaluation_start_at >= end_at:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": "effective_window_empty",
                    "effective_evaluation_start_at": effective_evaluation_start_at.isoformat(),
                }
            )
            continue

        for min_train_trades in (1, 2):
            for variant in variants:
                by_source = {}
                for source, candles in candles_by_source.items():
                    by_source[source] = evaluate_variant_on_source(
                        source=source,
                        symbol=symbol,
                        candles=candles,
                        variant=variant,
                        evaluation_start_at=effective_evaluation_start_at,
                        end_at=end_at,
                        initial_capital=initial_capital,
                        fee=fee,
                        slippage=slippage,
                        position_size_pct=POSITION_SIZE_PCT,
                        train_position_size_pct=TRAIN_POSITION_SIZE_PCT,
                        drawdown_guard_pct=DRAWDOWN_GUARD_PCT,
                        cooldown_periods=COOLDOWN_PERIODS,
                        min_train_trades=min_train_trades,
                    )
                reports.append(
                    summarize_symbol_result(
                        symbol=symbol,
                        variant=variant,
                        min_train_trades=min_train_trades,
                        effective_evaluation_start_at=effective_evaluation_start_at,
                        by_source=by_source,
                    )
                )

    reports.sort(
        key=lambda item: (
            item["summary"]["robustness_score"],
            item["summary"]["min_return_pct"],
            -item["summary"]["gap_return_pct"],
        ),
        reverse=True,
    )

    best_by_symbol: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for item in reports:
        symbol = str(item["symbol"])
        if symbol in seen_symbols:
            continue
        best_by_symbol.append(item)
        seen_symbols.add(symbol)

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "symbols": symbols,
        "window": {
            "fetch_start_at": fetch_start_at.isoformat(),
            "requested_evaluation_start_at": evaluation_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "config": {
            "position_size_pct": float(POSITION_SIZE_PCT),
            "train_position_size_pct": float(TRAIN_POSITION_SIZE_PCT),
            "drawdown_guard_pct": DRAWDOWN_GUARD_PCT,
            "cooldown_periods": COOLDOWN_PERIODS,
            "min_train_trades_candidates": [1, 2],
        },
        "reports": reports,
        "best_by_symbol": best_by_symbol,
        "skipped": skipped,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote altcoin cross-source sweep report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
