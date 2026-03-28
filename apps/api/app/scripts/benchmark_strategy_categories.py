from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from app.benchmarks.category_strategy_suite import (
    BreakoutStrategy,
    MeanReversionStrategy,
    MomentumStrategy,
    PullbackStrategy,
    RangeTradingStrategy,
    ScalpingStrategy,
    TrendFollowingStrategy,
)
from app.engines.backtest_engine import BacktestEngine
from app.integrations.binance_us import BinanceUSTimeframe, normalize_binance_us_candles
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle, BacktestRequest, BacktestResponse
from app.strategies.base import BaseStrategy


UTC = timezone.utc


@dataclass(frozen=True)
class CategoryVariant:
    label: str
    overrides: dict[str, Any]


@dataclass(frozen=True)
class CategoryBenchmark:
    key: str
    name: str
    strategy_factory: Callable[[], BaseStrategy]
    timeframes: tuple[str, ...]
    variants: tuple[CategoryVariant, ...]


def build_category_benchmarks() -> tuple[CategoryBenchmark, ...]:
    return (
        CategoryBenchmark(
            key="trend_following",
            name="Trend following",
            strategy_factory=TrendFollowingStrategy,
            timeframes=("1h", "4h"),
            variants=(
                CategoryVariant(
                    label="ema20_100_fast_reset",
                    overrides={
                        "fast_ema_period": 20,
                        "slow_ema_period": 100,
                        "breakout_lookback": 10,
                        "exit_ema_period": 10,
                        "min_trend_gap_pct": 0.004,
                        "slope_lookback_bars": 8,
                        "min_slow_ema_slope_pct": 0.0025,
                        "volume_period": 20,
                        "min_volume_multiple": 0.95,
                        "min_average_dollar_volume": 150000,
                        "atr_period": 14,
                        "min_atr_pct": 0.01,
                        "max_atr_pct": 0.08,
                        "breakout_buffer_pct": 0.001,
                        "breakout_min_body_pct": 0.002,
                        "breakout_min_close_location": 0.6,
                        "recent_pullback_lookback": 8,
                        "pullback_proximity_pct": 0.02,
                        "max_extension_above_fast_ema_pct": 0.04,
                        "max_bars_in_trade": 120,
                        "stop_loss_pct": 0.035,
                    },
                ),
                CategoryVariant(
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
                ),
                CategoryVariant(
                    label="ema50_200_liquid",
                    overrides={
                        "fast_ema_period": 50,
                        "slow_ema_period": 200,
                        "breakout_lookback": 20,
                        "exit_ema_period": 20,
                        "min_trend_gap_pct": 0.01,
                        "slope_lookback_bars": 12,
                        "min_slow_ema_slope_pct": 0.004,
                        "volume_period": 20,
                        "min_volume_multiple": 0.9,
                        "min_average_dollar_volume": 300000,
                        "atr_period": 14,
                        "min_atr_pct": 0.01,
                        "max_atr_pct": 0.06,
                        "breakout_buffer_pct": 0.001,
                        "breakout_min_body_pct": 0.0025,
                        "breakout_min_close_location": 0.65,
                        "recent_pullback_lookback": 12,
                        "pullback_proximity_pct": 0.012,
                        "max_extension_above_fast_ema_pct": 0.03,
                        "max_bars_in_trade": 240,
                        "stop_loss_pct": 0.045,
                    },
                ),
                CategoryVariant(
                    label="ema50_200_conservative",
                    overrides={
                        "fast_ema_period": 50,
                        "slow_ema_period": 200,
                        "breakout_lookback": 30,
                        "exit_ema_period": 20,
                        "min_trend_gap_pct": 0.012,
                        "slope_lookback_bars": 16,
                        "min_slow_ema_slope_pct": 0.005,
                        "volume_period": 20,
                        "min_volume_multiple": 1.0,
                        "min_average_dollar_volume": 400000,
                        "atr_period": 14,
                        "min_atr_pct": 0.012,
                        "max_atr_pct": 0.05,
                        "breakout_buffer_pct": 0.0025,
                        "breakout_min_body_pct": 0.003,
                        "breakout_min_close_location": 0.7,
                        "recent_pullback_lookback": 16,
                        "pullback_proximity_pct": 0.01,
                        "max_extension_above_fast_ema_pct": 0.025,
                        "max_bars_in_trade": 192,
                        "stop_loss_pct": 0.04,
                    },
                ),
            ),
        ),
        CategoryBenchmark(
            key="mean_reversion",
            name="Mean reversion",
            strategy_factory=MeanReversionStrategy,
            timeframes=("15m", "1h"),
            variants=(
                CategoryVariant(
                    label="z1_5_fast",
                    overrides={
                        "lookback": 20,
                        "entry_zscore": 1.5,
                        "max_bars_in_trade": 8,
                        "stop_buffer_pct": 0.002,
                        "stop_loss_pct": 0.015,
                    },
                ),
                CategoryVariant(
                    label="z2_medium",
                    overrides={
                        "lookback": 20,
                        "entry_zscore": 2.0,
                        "max_bars_in_trade": 12,
                        "stop_buffer_pct": 0.003,
                        "stop_loss_pct": 0.02,
                    },
                ),
                CategoryVariant(
                    label="z2_2_slow",
                    overrides={
                        "lookback": 30,
                        "entry_zscore": 2.2,
                        "max_bars_in_trade": 24,
                        "stop_buffer_pct": 0.004,
                        "stop_loss_pct": 0.025,
                    },
                ),
            ),
        ),
        CategoryBenchmark(
            key="breakout",
            name="Breakout",
            strategy_factory=BreakoutStrategy,
            timeframes=("15m", "1h"),
            variants=(
                CategoryVariant(
                    label="tight20",
                    overrides={
                        "breakout_lookback": 20,
                        "compression_lookback": 15,
                        "max_range_width_pct": 0.035,
                        "min_volume_multiple": 1.1,
                        "max_bars_in_trade": 24,
                        "stop_loss_pct": 0.025,
                    },
                ),
                CategoryVariant(
                    label="classic20",
                    overrides={
                        "breakout_lookback": 20,
                        "compression_lookback": 20,
                        "max_range_width_pct": 0.04,
                        "min_volume_multiple": 1.2,
                        "max_bars_in_trade": 48,
                        "stop_loss_pct": 0.03,
                    },
                ),
                CategoryVariant(
                    label="wide30",
                    overrides={
                        "breakout_lookback": 30,
                        "compression_lookback": 30,
                        "max_range_width_pct": 0.05,
                        "min_volume_multiple": 1.0,
                        "max_bars_in_trade": 72,
                        "stop_loss_pct": 0.035,
                    },
                ),
            ),
        ),
        CategoryBenchmark(
            key="pullback",
            name="Pullback / Retracement",
            strategy_factory=PullbackStrategy,
            timeframes=("15m", "1h", "4h"),
            variants=(
                CategoryVariant(
                    label="ema20_50_shallow",
                    overrides={
                        "fast_ema_period": 20,
                        "slow_ema_period": 50,
                        "signal_ema_period": 10,
                        "pullback_lookback": 20,
                        "min_pullback_pct": 0.01,
                        "max_pullback_pct": 0.04,
                        "max_bars_in_trade": 48,
                    },
                ),
                CategoryVariant(
                    label="ema20_100_medium",
                    overrides={
                        "fast_ema_period": 20,
                        "slow_ema_period": 100,
                        "signal_ema_period": 10,
                        "pullback_lookback": 20,
                        "min_pullback_pct": 0.015,
                        "max_pullback_pct": 0.06,
                        "max_bars_in_trade": 96,
                    },
                ),
                CategoryVariant(
                    label="ema50_200_wide",
                    overrides={
                        "fast_ema_period": 50,
                        "slow_ema_period": 200,
                        "signal_ema_period": 20,
                        "pullback_lookback": 30,
                        "min_pullback_pct": 0.02,
                        "max_pullback_pct": 0.08,
                        "max_bars_in_trade": 144,
                    },
                ),
            ),
        ),
        CategoryBenchmark(
            key="range_trading",
            name="Range trading",
            strategy_factory=RangeTradingStrategy,
            timeframes=("15m", "1h"),
            variants=(
                CategoryVariant(
                    label="tight30",
                    overrides={
                        "range_lookback": 30,
                        "min_range_width_pct": 0.02,
                        "max_range_width_pct": 0.06,
                        "max_center_shift_pct": 0.008,
                        "entry_zone_pct": 0.2,
                        "exit_zone_pct": 0.75,
                        "max_bars_in_trade": 18,
                    },
                ),
                CategoryVariant(
                    label="classic50",
                    overrides={
                        "range_lookback": 50,
                        "min_range_width_pct": 0.025,
                        "max_range_width_pct": 0.08,
                        "max_center_shift_pct": 0.01,
                        "entry_zone_pct": 0.25,
                        "exit_zone_pct": 0.8,
                        "max_bars_in_trade": 30,
                    },
                ),
                CategoryVariant(
                    label="fast20",
                    overrides={
                        "range_lookback": 20,
                        "min_range_width_pct": 0.015,
                        "max_range_width_pct": 0.04,
                        "max_center_shift_pct": 0.006,
                        "entry_zone_pct": 0.25,
                        "exit_zone_pct": 0.7,
                        "max_bars_in_trade": 12,
                    },
                ),
            ),
        ),
        CategoryBenchmark(
            key="momentum",
            name="Momentum",
            strategy_factory=MomentumStrategy,
            timeframes=("15m", "1h"),
            variants=(
                CategoryVariant(
                    label="impulse3_fast",
                    overrides={
                        "trend_ema_period": 50,
                        "exit_ema_period": 20,
                        "impulse_lookback": 3,
                        "min_return_pct": 0.012,
                        "min_volume_multiple": 1.2,
                        "max_bars_in_trade": 12,
                        "stop_loss_pct": 0.02,
                        "take_profit_pct": 0.04,
                    },
                ),
                CategoryVariant(
                    label="impulse4_balanced",
                    overrides={
                        "trend_ema_period": 50,
                        "exit_ema_period": 20,
                        "impulse_lookback": 4,
                        "min_return_pct": 0.015,
                        "min_volume_multiple": 1.1,
                        "max_bars_in_trade": 8,
                        "stop_loss_pct": 0.015,
                        "take_profit_pct": 0.03,
                    },
                ),
                CategoryVariant(
                    label="impulse6_swing",
                    overrides={
                        "trend_ema_period": 100,
                        "exit_ema_period": 30,
                        "impulse_lookback": 6,
                        "min_return_pct": 0.02,
                        "min_volume_multiple": 1.4,
                        "max_bars_in_trade": 16,
                        "stop_loss_pct": 0.025,
                        "take_profit_pct": 0.06,
                    },
                ),
            ),
        ),
        CategoryBenchmark(
            key="scalping",
            name="Scalping",
            strategy_factory=ScalpingStrategy,
            timeframes=("5m", "15m"),
            variants=(
                CategoryVariant(
                    label="micro9_fast",
                    overrides={
                        "trend_ema_period": 30,
                        "micro_ema_period": 9,
                        "dip_threshold_pct": 0.002,
                        "reclaim_buffer_pct": 0.0005,
                        "min_volume_multiple": 0.8,
                        "max_bars_in_trade": 4,
                        "stop_loss_pct": 0.004,
                        "take_profit_pct": 0.006,
                    },
                ),
                CategoryVariant(
                    label="micro7_tight",
                    overrides={
                        "trend_ema_period": 20,
                        "micro_ema_period": 7,
                        "dip_threshold_pct": 0.003,
                        "reclaim_buffer_pct": 0.0007,
                        "min_volume_multiple": 0.9,
                        "max_bars_in_trade": 3,
                        "stop_loss_pct": 0.0035,
                        "take_profit_pct": 0.005,
                    },
                ),
                CategoryVariant(
                    label="micro12_relaxed",
                    overrides={
                        "trend_ema_period": 50,
                        "micro_ema_period": 12,
                        "dip_threshold_pct": 0.0025,
                        "reclaim_buffer_pct": 0.0005,
                        "min_volume_multiple": 0.75,
                        "max_bars_in_trade": 6,
                        "stop_loss_pct": 0.005,
                        "take_profit_pct": 0.008,
                    },
                ),
            ),
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark 7 strategy categories on Binance.US spot data.")
    parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT")
    parser.add_argument("--evaluation-start-at", default="2025-09-15T00:00:00+00:00")
    parser.add_argument("--train-end-at", default="2025-12-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=45)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--max-candidates-per-category", type=int, default=0)
    parser.add_argument("--output")
    return parser.parse_args()


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_symbols(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def fetch_candles(
    client: BinanceUSClient,
    symbol: str,
    timeframe: str,
    start_at: datetime,
    end_at: datetime,
) -> list[BacktestCandle]:
    raw_rows = client.fetch_historical_candles(symbol=symbol, timeframe=timeframe, start_at=start_at, end_at=end_at)
    normalized = normalize_binance_us_candles(raw_rows, BinanceUSTimeframe.from_code(timeframe))
    candles = [
        BacktestCandle(
            open_time=candle.open_time,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
        )
        for candle in normalized
        if candle.open_time < end_at
    ]
    if not candles:
        raise ValueError(f"No candles loaded for {symbol} {timeframe}")
    return candles


def run_backtest(
    engine: BacktestEngine,
    strategy: BaseStrategy,
    strategy_code: str,
    symbol: str,
    timeframe: str,
    start_at: datetime,
    end_at: datetime,
    candles: list[BacktestCandle],
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
    overrides: dict[str, Any],
    position_size_pct: Decimal = Decimal("0.10"),
) -> BacktestResponse:
    candles_in_scope = [candle for candle in candles if candle.open_time < end_at]
    request = BacktestRequest(
        strategy_code=strategy_code,
        symbol=symbol,
        timeframe=timeframe,
        start_at=start_at,
        end_at=end_at,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
        position_size_pct=position_size_pct,
        strategy_config_override=overrides,
    )
    return engine.run(request=request, strategy=strategy, candles=candles_in_scope)


def aggregate_results(results: list[BacktestResponse]) -> dict[str, Any]:
    total_initial = sum(float(result.initial_capital) for result in results)
    total_final = sum(float(result.final_equity) for result in results)
    total_return_pct = 0.0
    if total_initial > 0:
        total_return_pct = ((total_final - total_initial) / total_initial) * 100.0

    total_trades = sum(result.metrics.total_trades for result in results)
    average_drawdown_pct = 0.0
    if results:
        average_drawdown_pct = sum(float(result.metrics.max_drawdown_pct) for result in results) / float(len(results))

    winning_trades = 0
    gross_profit = 0.0
    gross_loss = 0.0
    per_symbol: list[dict[str, Any]] = []
    for result in results:
        symbol_pnl = float(result.final_equity) - float(result.initial_capital)
        per_symbol.append(
            {
                "symbol": result.symbol,
                "timeframe": result.timeframe,
                "final_equity": round(float(result.final_equity), 4),
                "return_pct": round(float(result.metrics.total_return_pct), 4),
                "max_drawdown_pct": round(float(result.metrics.max_drawdown_pct), 4),
                "total_trades": result.metrics.total_trades,
                "pnl": round(symbol_pnl, 4),
            }
        )
        for trade in result.trades:
            pnl = float(trade.pnl)
            if pnl > 0:
                winning_trades += 1
                gross_profit += pnl
            elif pnl < 0:
                gross_loss += abs(pnl)

    win_rate_pct = 0.0
    if total_trades > 0:
        win_rate_pct = (winning_trades / float(total_trades)) * 100.0

    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = gross_profit
    else:
        profit_factor = 0.0

    return {
        "portfolio_final_equity": round(total_final, 4),
        "portfolio_return_pct": round(total_return_pct, 4),
        "portfolio_pnl": round(total_final - total_initial, 4),
        "average_max_drawdown_pct": round(average_drawdown_pct, 4),
        "total_trades": total_trades,
        "win_rate_pct": round(win_rate_pct, 4),
        "profit_factor": round(profit_factor, 4),
        "per_symbol": per_symbol,
    }


def score_aggregate(aggregate: dict[str, Any]) -> float:
    if aggregate["total_trades"] <= 0:
        return -1_000_000_000.0
    return float(aggregate["portfolio_return_pct"]) - (float(aggregate["average_max_drawdown_pct"]) * 0.15)


def benchmark_category(
    category: CategoryBenchmark,
    symbols: list[str],
    candle_cache: dict[tuple[str, str], list[BacktestCandle]],
    train_start_at: datetime,
    train_end_at: datetime,
    test_end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
    max_candidates_per_category: int = 0,
) -> dict[str, Any]:
    engine = BacktestEngine()
    candidates = category.variants
    if max_candidates_per_category > 0:
        candidates = candidates[:max_candidates_per_category]

    train_evaluations: list[dict[str, Any]] = []
    for timeframe in category.timeframes:
        for variant in candidates:
            train_results: list[BacktestResponse] = []
            for symbol in symbols:
                candles = candle_cache[(symbol, timeframe)]
                strategy = category.strategy_factory()
                result = run_backtest(
                    engine=engine,
                    strategy=strategy,
                    strategy_code=f"{category.key}:{variant.label}:{timeframe}",
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=train_start_at,
                    end_at=train_end_at,
                    candles=candles,
                    initial_capital=initial_capital,
                    fee=fee,
                    slippage=slippage,
                    overrides=variant.overrides,
                )
                train_results.append(result)

            aggregate = aggregate_results(train_results)
            train_evaluations.append(
                {
                    "timeframe": timeframe,
                    "variant": variant.label,
                    "overrides": variant.overrides,
                    "train": aggregate,
                    "score": round(score_aggregate(aggregate), 6),
                }
            )

    best_train = max(
        train_evaluations,
        key=lambda item: (
            item["score"],
            item["train"]["portfolio_return_pct"],
            -item["train"]["average_max_drawdown_pct"],
            item["train"]["total_trades"],
        ),
    )

    test_results: list[BacktestResponse] = []
    selected_timeframe = str(best_train["timeframe"])
    selected_overrides = dict(best_train["overrides"])
    for symbol in symbols:
        candles = candle_cache[(symbol, selected_timeframe)]
        strategy = category.strategy_factory()
        result = run_backtest(
            engine=engine,
            strategy=strategy,
            strategy_code=f"{category.key}:{best_train['variant']}:{selected_timeframe}:test",
            symbol=symbol,
            timeframe=selected_timeframe,
            start_at=train_end_at,
            end_at=test_end_at,
            candles=candles,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            overrides=selected_overrides,
        )
        test_results.append(result)

    return {
        "key": category.key,
        "name": category.name,
        "selected_model": {
            "timeframe": selected_timeframe,
            "variant": best_train["variant"],
            "overrides": selected_overrides,
        },
        "train": best_train["train"],
        "test": aggregate_results(test_results),
        "train_candidates": train_evaluations,
    }


def main() -> None:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    train_end_at = parse_datetime(args.train_end_at)
    end_at = parse_datetime(args.end_at)
    if not (evaluation_start_at < train_end_at < end_at):
        raise ValueError("Expected evaluation_start_at < train_end_at < end_at")

    warmup_start_at = evaluation_start_at - timedelta(days=args.warmup_days)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))

    category_benchmarks = build_category_benchmarks()
    required_timeframes = sorted({timeframe for category in category_benchmarks for timeframe in category.timeframes})
    candle_cache: dict[tuple[str, str], list[BacktestCandle]] = {}

    client = BinanceUSClient()
    try:
        for symbol in symbols:
            for timeframe in required_timeframes:
                print(f"Fetching {symbol} {timeframe} candles from {warmup_start_at.isoformat()} to {end_at.isoformat()}...")
                candle_cache[(symbol, timeframe)] = fetch_candles(
                    client=client,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_at=warmup_start_at,
                    end_at=end_at,
                )
                print(f"Loaded {len(candle_cache[(symbol, timeframe)])} candles for {symbol} {timeframe}")
    finally:
        client.close()

    category_results: list[dict[str, Any]] = []
    for category in category_benchmarks:
        print(f"Benchmarking {category.name}...")
        result = benchmark_category(
            category=category,
            symbols=symbols,
            candle_cache=candle_cache,
            train_start_at=evaluation_start_at,
            train_end_at=train_end_at,
            test_end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            max_candidates_per_category=args.max_candidates_per_category,
        )
        category_results.append(result)
        test_metrics = result["test"]
        print(
            f"  Selected {result['selected_model']['variant']} on {result['selected_model']['timeframe']} "
            f"-> test return {test_metrics['portfolio_return_pct']}%, "
            f"DD {test_metrics['average_max_drawdown_pct']}%, trades {test_metrics['total_trades']}"
        )

    ranked = sorted(
        category_results,
        key=lambda item: (
            float(item["test"]["portfolio_return_pct"]),
            -float(item["test"]["average_max_drawdown_pct"]),
            float(item["test"]["profit_factor"]),
        ),
        reverse=True,
    )

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "data_source": "Binance.US spot klines via /api/v3/klines",
        "benchmark_window": {
            "warmup_start_at": warmup_start_at.isoformat(),
            "evaluation_start_at": evaluation_start_at.isoformat(),
            "train_end_at": train_end_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "symbols": symbols,
        "fee": str(fee),
        "slippage": str(slippage),
        "ranking": [
            {
                "rank": index + 1,
                "strategy": item["name"],
                "key": item["key"],
                "selected_timeframe": item["selected_model"]["timeframe"],
                "selected_variant": item["selected_model"]["variant"],
                "test_return_pct": item["test"]["portfolio_return_pct"],
                "test_max_drawdown_pct": item["test"]["average_max_drawdown_pct"],
                "test_profit_factor": item["test"]["profit_factor"],
                "test_total_trades": item["test"]["total_trades"],
            }
            for index, item in enumerate(ranked)
        ],
        "categories": ranked,
        "winner": ranked[0] if ranked else None,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote benchmark report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
