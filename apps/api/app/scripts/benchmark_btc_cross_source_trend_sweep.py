from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.engines.backtest_engine import BacktestEngine
from app.integrations.binance_archive import BinanceArchiveClient
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle, BacktestResponse, EquityPoint
from app.scripts.benchmark_btc_sleeve_cross_source import (
    fetch_archive_spot_candles,
    resample_to_four_hour,
)
from app.scripts.benchmark_btc_sleeve_risk_controls import (
    drawdown_from_peak_pct,
    find_flat_priority_variant,
    generate_windows,
    make_cash_curve,
    run_variant_backtest,
    scaled_trade_stats,
)
from app.scripts.benchmark_regime_aware_adaptive_allocator import (
    BASE_SYMBOL_CAPITAL,
    add_months,
    append_points,
    build_slice_curve,
    summarize_portfolio,
)
from app.scripts.benchmark_strategy_categories import CategoryVariant, fetch_candles, parse_datetime

UTC = timezone.utc
ZERO = Decimal("0")
POSITION_SIZE_PCT = Decimal("1.00")
TRAIN_POSITION_SIZE_PCT = Decimal("0.10")
DRAWDOWN_GUARD_PCT = 4.0
COOLDOWN_PERIODS = 3
MIN_TRAIN_TRADES = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cross-source sweep of trend-layer variants inside the BTC flat-priority sleeve."
    )
    parser.add_argument("--symbol", default="BTC-USDT")
    parser.add_argument("--evaluation-start-at", default="2022-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--output")
    return parser.parse_args()


def build_variants() -> tuple[CategoryVariant, ...]:
    base = find_flat_priority_variant()
    base_overrides = deepcopy(base.overrides)

    regime_profiles = (
        (
            "base_regime",
            {
                "trend_min_gap_pct": 0.008,
                "trend_min_slope_pct": 0.004,
                "flat_max_width_pct": 0.07,
                "flat_max_center_shift_pct": 0.009,
            },
        ),
        (
            "strict_regime",
            {
                "trend_min_gap_pct": 0.011,
                "trend_min_slope_pct": 0.006,
                "flat_max_width_pct": 0.07,
                "flat_max_center_shift_pct": 0.009,
            },
        ),
        (
            "flat_bias_regime",
            {
                "trend_min_gap_pct": 0.012,
                "trend_min_slope_pct": 0.006,
                "flat_max_width_pct": 0.08,
                "flat_max_center_shift_pct": 0.012,
            },
        ),
    )
    entry_profiles = (
        (
            "base_entry",
            {
                "min_trend_gap_pct": 0.007,
                "min_slow_ema_slope_pct": 0.004,
                "breakout_buffer_pct": 0.001,
                "breakout_min_close_location": 0.60,
                "max_extension_above_fast_ema_pct": 0.035,
            },
        ),
        (
            "strict_entry",
            {
                "min_trend_gap_pct": 0.010,
                "min_slow_ema_slope_pct": 0.006,
                "breakout_buffer_pct": 0.0025,
                "breakout_min_close_location": 0.70,
                "max_extension_above_fast_ema_pct": 0.030,
            },
        ),
        (
            "retest_entry",
            {
                "min_trend_gap_pct": 0.009,
                "min_slow_ema_slope_pct": 0.005,
                "breakout_buffer_pct": 0.0035,
                "breakout_min_close_location": 0.75,
                "max_extension_above_fast_ema_pct": 0.025,
            },
        ),
        (
            "tight_extension_entry",
            {
                "min_trend_gap_pct": 0.007,
                "min_slow_ema_slope_pct": 0.004,
                "breakout_buffer_pct": 0.0015,
                "breakout_min_close_location": 0.65,
                "max_extension_above_fast_ema_pct": 0.020,
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

    no_trend = deepcopy(base_overrides)
    no_trend["trend_min_gap_pct"] = 1.0
    no_trend["trend_min_slope_pct"] = 1.0
    variants.append(CategoryVariant(label="no_trend", overrides=no_trend))
    return tuple(variants)


def fetch_source_candles(
    *,
    symbol: str,
    fetch_start_at: datetime,
    end_at: datetime,
) -> dict[str, list[BacktestCandle]]:
    binance_us_client = BinanceUSClient()
    try:
        binance_us_candles = resample_to_four_hour(
            fetch_candles(
                client=binance_us_client,
                symbol=symbol,
                timeframe="1h",
                start_at=fetch_start_at,
                end_at=end_at,
            )
        )
    finally:
        binance_us_client.close()

    archive_client = BinanceArchiveClient()
    try:
        archive_candles = resample_to_four_hour(
            fetch_archive_spot_candles(
                client=archive_client,
                symbol=symbol,
                timeframe="1h",
                start_at=fetch_start_at,
                end_at=end_at,
            )
        )
    finally:
        archive_client.close()

    return {
        "binance_us": binance_us_candles,
        "binance_archive_spot": archive_candles,
    }


def evaluate_variant_on_source(
    *,
    source: str,
    symbol: str,
    candles: list[BacktestCandle],
    variant: CategoryVariant,
    evaluation_start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
    position_size_pct: Decimal = POSITION_SIZE_PCT,
    train_position_size_pct: Decimal = TRAIN_POSITION_SIZE_PCT,
    drawdown_guard_pct: float = DRAWDOWN_GUARD_PCT,
    cooldown_periods: int = COOLDOWN_PERIODS,
    min_train_trades: int = MIN_TRAIN_TRADES,
) -> dict[str, Any]:
    windows = generate_windows(evaluation_start_at=evaluation_start_at, end_at=end_at)
    train_cache: dict[tuple[str, str], BacktestResponse] = {}
    test_cache: dict[tuple[str, str], BacktestResponse] = {}

    def get_train_result(train_start_at: datetime, train_end_at: datetime) -> BacktestResponse:
        cache_key = (train_start_at.isoformat(), train_end_at.isoformat())
        cached = train_cache.get(cache_key)
        if cached is not None:
            return cached
        cached = run_variant_backtest(
            engine=BacktestEngine(),
            symbol=symbol,
            candles=candles,
            start_at=train_start_at,
            end_at=train_end_at,
            fee=fee,
            slippage=slippage,
            position_size_pct=train_position_size_pct,
            variant=variant,
            strategy_code=f"cross_source_trend_sweep:{variant.label}:{source}:train",
        )
        train_cache[cache_key] = cached
        return cached

    def get_test_result(test_start_at: datetime, test_end_at: datetime) -> BacktestResponse:
        cache_key = (test_start_at.isoformat(), test_end_at.isoformat())
        cached = test_cache.get(cache_key)
        if cached is not None:
            return cached
        cached = run_variant_backtest(
            engine=BacktestEngine(),
            symbol=symbol,
            candles=candles,
            start_at=test_start_at,
            end_at=test_end_at,
            fee=fee,
            slippage=slippage,
            position_size_pct=position_size_pct,
            variant=variant,
            strategy_code=f"cross_source_trend_sweep:{variant.label}:{source}:test",
        )
        test_cache[cache_key] = cached
        return cached

    capital = initial_capital
    peak_equity = initial_capital
    prior_drawdown_pct = 0.0
    cooldown_remaining = 0
    guard_triggers = 0
    positive_periods = 0
    cash_periods = 0
    total_trades = 0
    winning_trades = 0
    gross_profit = ZERO
    gross_loss = ZERO
    equity_curve: list[EquityPoint] = [
        EquityPoint(
            timestamp=evaluation_start_at,
            equity=initial_capital,
            cash=initial_capital,
            close_price=initial_capital,
            position_qty=ZERO,
        )
    ]

    for window in windows:
        train_result = get_train_result(window["train_start_at"], window["train_end_at"])
        if cooldown_remaining > 0:
            cash_periods += 1
            cooldown_remaining -= 1
            append_points(equity_curve, make_cash_curve(capital=capital, timestamp=window["test_end_at"]))
            prior_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
            continue
        if int(train_result.metrics.total_trades) < min_train_trades:
            cash_periods += 1
            append_points(equity_curve, make_cash_curve(capital=capital, timestamp=window["test_end_at"]))
            prior_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
            continue

        base_result = get_test_result(window["test_start_at"], window["test_end_at"])
        scale_factor = capital / BASE_SYMBOL_CAPITAL
        slice_curve = build_slice_curve(
            scaled_results=((base_result, scale_factor, capital),),
            initial_capital=capital,
            test_end_at=window["test_end_at"],
        )
        append_points(equity_curve, slice_curve)
        final_capital = base_result.final_equity * scale_factor
        slice_total_trades, slice_winning_trades, slice_gross_profit, slice_gross_loss = scaled_trade_stats(
            response=base_result,
            scale_factor=scale_factor,
        )
        slice_summary = summarize_portfolio(
            initial_capital=capital,
            final_capital=final_capital,
            equity_curve=[
                EquityPoint(
                    timestamp=window["test_start_at"],
                    equity=capital,
                    cash=capital,
                    close_price=capital,
                    position_qty=ZERO,
                ),
                *slice_curve,
            ],
            total_trades=slice_total_trades,
            winning_trades=slice_winning_trades,
            gross_profit=slice_gross_profit,
            gross_loss=slice_gross_loss,
        )
        if slice_summary["total_return_pct"] > 0:
            positive_periods += 1

        total_trades += slice_total_trades
        winning_trades += slice_winning_trades
        gross_profit += slice_gross_profit
        gross_loss += slice_gross_loss
        capital = final_capital
        if capital > peak_equity:
            peak_equity = capital
        current_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
        if current_drawdown_pct >= drawdown_guard_pct and prior_drawdown_pct < drawdown_guard_pct:
            cooldown_remaining = cooldown_periods
            guard_triggers += 1
        prior_drawdown_pct = current_drawdown_pct

    summary = summarize_portfolio(
        initial_capital=initial_capital,
        final_capital=capital,
        equity_curve=equity_curve,
        total_trades=total_trades,
        winning_trades=winning_trades,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
    )
    summary["positive_periods"] = positive_periods
    summary["cash_periods"] = cash_periods
    summary["guard_triggers"] = guard_triggers
    summary["period_count"] = len(windows)
    return summary


def summarize_pair(
    variant: CategoryVariant,
    by_source: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    us = by_source["binance_us"]
    archive = by_source["binance_archive_spot"]
    min_return_pct = min(float(us["total_return_pct"]), float(archive["total_return_pct"]))
    avg_return_pct = (float(us["total_return_pct"]) + float(archive["total_return_pct"])) / 2.0
    gap_return_pct = abs(float(us["total_return_pct"]) - float(archive["total_return_pct"]))
    avg_drawdown_pct = (float(us["max_drawdown_pct"]) + float(archive["max_drawdown_pct"])) / 2.0
    min_profit_factor = min(float(us["profit_factor"]), float(archive["profit_factor"]))
    both_positive = float(us["total_return_pct"]) > 0.0 and float(archive["total_return_pct"]) > 0.0
    bonus = 2.0 if both_positive else 0.0
    robustness_score = round(
        bonus + min_return_pct - (gap_return_pct * 0.30) - (avg_drawdown_pct * 0.10),
        6,
    )
    return {
        "variant": {
            "label": variant.label,
            "overrides": variant.overrides,
        },
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
            "both_sources_positive": both_positive,
            "robustness_score": robustness_score,
        },
        "sources": by_source,
    }


def main() -> None:
    args = parse_args()
    symbol = args.symbol.strip().upper()
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))
    fetch_start_at = add_months(evaluation_start_at, -2) - timedelta(days=args.warmup_days)

    candles_by_source = fetch_source_candles(
        symbol=symbol,
        fetch_start_at=fetch_start_at,
        end_at=end_at,
    )
    variants = build_variants()

    reports: list[dict[str, Any]] = []
    for variant in variants:
        print(f"Evaluating cross-source trend variant {variant.label}...")
        source_summaries = {
            source: evaluate_variant_on_source(
                source=source,
                symbol=symbol,
                candles=candles,
                variant=variant,
                evaluation_start_at=evaluation_start_at,
                end_at=end_at,
                initial_capital=initial_capital,
                fee=fee,
                slippage=slippage,
            )
            for source, candles in candles_by_source.items()
        }
        reports.append(summarize_pair(variant=variant, by_source=source_summaries))

    reports.sort(
        key=lambda item: (
            item["summary"]["robustness_score"],
            item["summary"]["min_return_pct"],
            -item["summary"]["gap_return_pct"],
        ),
        reverse=True,
    )
    winner = reports[0]
    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "symbol": symbol,
        "window": {
            "fetch_start_at": fetch_start_at.isoformat(),
            "evaluation_start_at": evaluation_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "config": {
            "position_size_pct": float(POSITION_SIZE_PCT),
            "train_position_size_pct": float(TRAIN_POSITION_SIZE_PCT),
            "drawdown_guard_pct": DRAWDOWN_GUARD_PCT,
            "cooldown_periods": COOLDOWN_PERIODS,
            "min_train_trades": MIN_TRAIN_TRADES,
        },
        "reports": reports,
        "winner": winner,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote cross-source trend sweep report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
