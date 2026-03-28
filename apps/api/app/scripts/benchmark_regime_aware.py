from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from app.benchmarks.category_strategy_suite import RegimeAwareStrategy, TrendFollowingStrategy
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle
from app.scripts.benchmark_strategy_categories import (
    CategoryVariant,
    aggregate_results,
    fetch_candles,
    parse_datetime,
    parse_symbols,
    run_backtest,
)
from app.engines.backtest_engine import BacktestEngine

UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark regime-aware strategy against standalone trend following.")
    parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,ADA-USDT,DOGE-USDT,LTC-USDT")
    parser.add_argument("--evaluation-start-at", default="2025-03-15T00:00:00+00:00")
    parser.add_argument("--train-end-at", default="2025-09-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--output")
    return parser.parse_args()


def build_regime_variants() -> tuple[CategoryVariant, ...]:
    trend_balanced = {
        "fast_ema_period": 20,
        "slow_ema_period": 100,
        "breakout_lookback": 20,
        "exit_ema_period": 20,
        "min_trend_gap_pct": 0.005,
        "slope_lookback_bars": 10,
        "min_slow_ema_slope_pct": 0.003,
        "volume_period": 20,
        "min_volume_multiple": 0.9,
        "min_average_dollar_volume": 200000,
        "atr_period": 14,
        "min_atr_pct": 0.008,
        "max_atr_pct": 0.07,
        "breakout_buffer_pct": 0.001,
        "breakout_min_body_pct": 0.002,
        "breakout_min_close_location": 0.6,
        "recent_pullback_lookback": 10,
        "pullback_proximity_pct": 0.015,
        "max_extension_above_fast_ema_pct": 0.035,
        "max_bars_in_trade": 168,
        "stop_loss_pct": 0.04,
    }
    range_tight = {
        "range_lookback": 30,
        "min_range_width_pct": 0.02,
        "max_range_width_pct": 0.06,
        "max_center_shift_pct": 0.008,
        "entry_zone_pct": 0.2,
        "exit_zone_pct": 0.75,
        "max_bars_in_trade": 18,
        "stop_buffer_pct": 0.003,
    }
    mean_balanced = {
        "lookback": 20,
        "entry_zscore": 2.0,
        "max_bars_in_trade": 12,
        "stop_buffer_pct": 0.003,
        "stop_loss_pct": 0.02,
    }
    return (
        CategoryVariant(
            label="balanced",
            overrides={
                "regime_fast_ema_period": 30,
                "regime_slow_ema_period": 120,
                "regime_slope_lookback_bars": 10,
                "trend_min_gap_pct": 0.006,
                "trend_min_slope_pct": 0.003,
                "flat_range_lookback": 30,
                "flat_max_width_pct": 0.06,
                "flat_max_center_shift_pct": 0.008,
                "flat_min_atr_pct": 0.006,
                "flat_max_atr_pct": 0.05,
                "atr_period": 14,
                "volume_period": 20,
                "min_average_dollar_volume": 200000,
                "trend_config": trend_balanced,
                "range_config": range_tight,
                "mean_config": mean_balanced,
            },
        ),
        CategoryVariant(
            label="trend_priority",
            overrides={
                "regime_fast_ema_period": 30,
                "regime_slow_ema_period": 120,
                "regime_slope_lookback_bars": 8,
                "trend_min_gap_pct": 0.005,
                "trend_min_slope_pct": 0.0025,
                "flat_range_lookback": 24,
                "flat_max_width_pct": 0.05,
                "flat_max_center_shift_pct": 0.007,
                "flat_min_atr_pct": 0.006,
                "flat_max_atr_pct": 0.045,
                "atr_period": 14,
                "volume_period": 20,
                "min_average_dollar_volume": 180000,
                "trend_config": {
                    **trend_balanced,
                    "breakout_lookback": 10,
                    "recent_pullback_lookback": 8,
                    "pullback_proximity_pct": 0.02,
                    "max_extension_above_fast_ema_pct": 0.04,
                },
                "range_config": {
                    **range_tight,
                    "range_lookback": 24,
                    "max_bars_in_trade": 14,
                },
                "mean_config": {
                    **mean_balanced,
                    "entry_zscore": 2.2,
                    "max_bars_in_trade": 10,
                },
            },
        ),
        CategoryVariant(
            label="flat_priority",
            overrides={
                "regime_fast_ema_period": 30,
                "regime_slow_ema_period": 120,
                "regime_slope_lookback_bars": 12,
                "trend_min_gap_pct": 0.008,
                "trend_min_slope_pct": 0.004,
                "flat_range_lookback": 36,
                "flat_max_width_pct": 0.07,
                "flat_max_center_shift_pct": 0.009,
                "flat_min_atr_pct": 0.005,
                "flat_max_atr_pct": 0.055,
                "atr_period": 14,
                "volume_period": 20,
                "min_average_dollar_volume": 200000,
                "trend_config": {
                    **trend_balanced,
                    "min_trend_gap_pct": 0.007,
                    "min_slow_ema_slope_pct": 0.004,
                },
                "range_config": {
                    **range_tight,
                    "range_lookback": 36,
                    "entry_zone_pct": 0.25,
                    "exit_zone_pct": 0.8,
                    "max_bars_in_trade": 22,
                },
                "mean_config": {
                    **mean_balanced,
                    "lookback": 24,
                    "entry_zscore": 1.8,
                    "max_bars_in_trade": 14,
                },
            },
        ),
        CategoryVariant(
            label="flat_priority_cross_source",
            overrides={
                "regime_fast_ema_period": 30,
                "regime_slow_ema_period": 120,
                "regime_slope_lookback_bars": 12,
                "trend_min_gap_pct": 0.008,
                "trend_min_slope_pct": 0.004,
                "flat_range_lookback": 36,
                "flat_max_width_pct": 0.07,
                "flat_max_center_shift_pct": 0.009,
                "flat_min_atr_pct": 0.005,
                "flat_max_atr_pct": 0.055,
                "atr_period": 14,
                "volume_period": 20,
                "min_average_dollar_volume": 200000,
                "trend_config": {
                    **trend_balanced,
                    "min_trend_gap_pct": 0.007,
                    "min_slow_ema_slope_pct": 0.004,
                    "breakout_buffer_pct": 0.0015,
                    "breakout_min_close_location": 0.65,
                    "max_extension_above_fast_ema_pct": 0.015,
                },
                "range_config": {
                    **range_tight,
                    "range_lookback": 36,
                    "entry_zone_pct": 0.25,
                    "exit_zone_pct": 0.8,
                    "max_bars_in_trade": 22,
                },
                "mean_config": {
                    **mean_balanced,
                    "lookback": 24,
                    "entry_zscore": 1.8,
                    "max_bars_in_trade": 14,
                },
            },
        ),
    )


def standalone_trend_variant() -> CategoryVariant:
    return CategoryVariant(
        label="ema20_100_balanced",
        overrides={
            "fast_ema_period": 20,
            "slow_ema_period": 100,
            "breakout_lookback": 20,
            "exit_ema_period": 20,
            "min_trend_gap_pct": 0.005,
            "slope_lookback_bars": 10,
            "min_slow_ema_slope_pct": 0.003,
            "volume_period": 20,
            "min_volume_multiple": 0.9,
            "min_average_dollar_volume": 200000,
            "atr_period": 14,
            "min_atr_pct": 0.008,
            "max_atr_pct": 0.07,
            "breakout_buffer_pct": 0.001,
            "breakout_min_body_pct": 0.002,
            "breakout_min_close_location": 0.6,
            "recent_pullback_lookback": 10,
            "pullback_proximity_pct": 0.015,
            "max_extension_above_fast_ema_pct": 0.035,
            "max_bars_in_trade": 168,
            "stop_loss_pct": 0.04,
        },
    )


def evaluate_variant(
    strategy_factory,
    strategy_code_prefix: str,
    variant: CategoryVariant,
    symbols: list[str],
    candle_cache: dict[str, list[BacktestCandle]],
    start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
) -> dict[str, object]:
    engine = BacktestEngine()
    results = []
    for symbol in symbols:
        results.append(
            run_backtest(
                engine=engine,
                strategy=strategy_factory(),
                strategy_code=f"{strategy_code_prefix}:{variant.label}",
                symbol=symbol,
                timeframe="4h" if strategy_code_prefix != "standalone_mean" else "1h",
                start_at=start_at,
                end_at=end_at,
                candles=candle_cache[symbol],
                initial_capital=initial_capital,
                fee=fee,
                slippage=slippage,
                overrides=variant.overrides,
            )
        )
    aggregate = aggregate_results(results)
    score = float(aggregate["portfolio_return_pct"]) - (float(aggregate["average_max_drawdown_pct"]) * 0.15)
    return {
        "label": variant.label,
        "overrides": variant.overrides,
        "metrics": aggregate,
        "score": round(score, 6),
    }


def main() -> None:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    train_end_at = parse_datetime(args.train_end_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))

    warmup_start_at = evaluation_start_at - timedelta(days=args.warmup_days)

    client = BinanceUSClient()
    try:
        candle_cache = {}
        for symbol in symbols:
            print(f"Fetching {symbol} 4h candles from {warmup_start_at.isoformat()} to {end_at.isoformat()}...")
            candle_cache[symbol] = fetch_candles(
                client=client,
                symbol=symbol,
                timeframe="4h",
                start_at=warmup_start_at,
                end_at=end_at,
            )
            print(f"Loaded {len(candle_cache[symbol])} candles for {symbol} 4h")
    finally:
        client.close()

    train_reports = []
    for variant in build_regime_variants():
        print(f"Evaluating regime-aware variant {variant.label}...")
        train_reports.append(
            evaluate_variant(
                strategy_factory=RegimeAwareStrategy,
                strategy_code_prefix="regime_aware",
                variant=variant,
                symbols=symbols,
                candle_cache=candle_cache,
                start_at=evaluation_start_at,
                end_at=train_end_at,
                initial_capital=initial_capital,
                fee=fee,
                slippage=slippage,
            )
        )

    best_variant = max(
        train_reports,
        key=lambda item: (item["score"], item["metrics"]["portfolio_return_pct"]),
    )
    selected = CategoryVariant(
        label=str(best_variant["label"]),
        overrides=dict(best_variant["overrides"]),
    )
    test_report = evaluate_variant(
        strategy_factory=RegimeAwareStrategy,
        strategy_code_prefix="regime_aware",
        variant=selected,
        symbols=symbols,
        candle_cache=candle_cache,
        start_at=train_end_at,
        end_at=end_at,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
    )

    baseline_variant = standalone_trend_variant()
    baseline_report = evaluate_variant(
        strategy_factory=TrendFollowingStrategy,
        strategy_code_prefix="trend_following",
        variant=baseline_variant,
        symbols=symbols,
        candle_cache=candle_cache,
        start_at=train_end_at,
        end_at=end_at,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
    )

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "symbols": symbols,
        "window": {
            "warmup_start_at": warmup_start_at.isoformat(),
            "evaluation_start_at": evaluation_start_at.isoformat(),
            "train_end_at": train_end_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "regime_aware": {
            "selected_variant": {
                "label": selected.label,
                "overrides": selected.overrides,
            },
            "train_candidates": train_reports,
            "test": test_report,
        },
        "baseline_trend_following": baseline_report,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote regime-aware benchmark report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
