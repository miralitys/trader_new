from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.engines.backtest_engine import BacktestEngine
from app.integrations.binance_archive import BinanceArchiveClient
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle, BacktestResponse, EquityPoint
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
from app.scripts.benchmark_strategy_categories import fetch_candles, parse_datetime

UTC = timezone.utc
ZERO = Decimal("0")
POSITION_SIZE_PCT = Decimal("1.00")
DRAWDOWN_GUARD_PCT = 4.0
COOLDOWN_PERIODS = 3
MIN_TRAIN_TRADES = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare BTC flat-priority sleeve robustness across candle data sources."
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


def fetch_archive_spot_candles(
    *,
    client: BinanceArchiveClient,
    symbol: str,
    timeframe: str,
    start_at: datetime,
    end_at: datetime,
) -> list[BacktestCandle]:
    candles: dict[datetime, BacktestCandle] = {}
    for chunk in client.iter_spot_klines(
        symbol=symbol,
        timeframe=timeframe,
        start_at=start_at,
        end_at=end_at,
    ):
        for row in chunk:
            timestamp = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=UTC)
            candles[timestamp] = BacktestCandle(
                open_time=timestamp,
                open=Decimal(str(row[1])),
                high=Decimal(str(row[2])),
                low=Decimal(str(row[3])),
                close=Decimal(str(row[4])),
                volume=Decimal(str(row[5])),
            )
    ordered = sorted(candles.values(), key=lambda candle: candle.open_time)
    return [candle for candle in ordered if candle.open_time <= end_at]


def to_four_hour_bucket(timestamp: datetime) -> datetime:
    normalized = timestamp.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    bucket_hour = (normalized.hour // 4) * 4
    return normalized.replace(hour=bucket_hour)


def resample_to_four_hour(candles: list[BacktestCandle]) -> list[BacktestCandle]:
    if not candles:
        return []

    buckets: dict[datetime, list[BacktestCandle]] = {}
    for candle in candles:
        bucket = to_four_hour_bucket(candle.open_time)
        buckets.setdefault(bucket, []).append(candle)

    resampled: list[BacktestCandle] = []
    for bucket_start in sorted(buckets):
        bucket_candles = sorted(buckets[bucket_start], key=lambda candle: candle.open_time)
        resampled.append(
            BacktestCandle(
                open_time=bucket_start,
                open=bucket_candles[0].open,
                high=max(candle.high for candle in bucket_candles),
                low=min(candle.low for candle in bucket_candles),
                close=bucket_candles[-1].close,
                volume=sum(candle.volume for candle in bucket_candles),
            )
        )
    return resampled


def evaluate_source(
    *,
    source: str,
    symbol: str,
    candles: list[BacktestCandle],
    evaluation_start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
) -> dict[str, Any]:
    variant = find_flat_priority_variant()
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
            position_size_pct=Decimal("0.10"),
            variant=variant,
            strategy_code=f"btc_cross_source:{source}:train",
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
            position_size_pct=POSITION_SIZE_PCT,
            variant=variant,
            strategy_code=f"btc_cross_source:{source}:test",
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
    periods: list[dict[str, Any]] = []

    for window in windows:
        train_result = get_train_result(window["train_start_at"], window["train_end_at"])
        train_trade_count = int(train_result.metrics.total_trades)

        if cooldown_remaining > 0:
            cash_periods += 1
            cooldown_remaining -= 1
            append_points(equity_curve, make_cash_curve(capital=capital, timestamp=window["test_end_at"]))
            current_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
            prior_drawdown_pct = current_drawdown_pct
            periods.append(
                {
                    "train_start_at": window["train_start_at"].isoformat(),
                    "train_end_at": window["train_end_at"].isoformat(),
                    "test_start_at": window["test_start_at"].isoformat(),
                    "test_end_at": window["test_end_at"].isoformat(),
                    "reason": "cooldown",
                    "train_total_trades": train_trade_count,
                    "total_return_pct": 0.0,
                    "drawdown_from_peak_pct": current_drawdown_pct,
                    "guard_triggered": False,
                    "cooldown_remaining_after": cooldown_remaining,
                }
            )
            continue

        if train_trade_count < MIN_TRAIN_TRADES:
            cash_periods += 1
            append_points(equity_curve, make_cash_curve(capital=capital, timestamp=window["test_end_at"]))
            current_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
            prior_drawdown_pct = current_drawdown_pct
            periods.append(
                {
                    "train_start_at": window["train_start_at"].isoformat(),
                    "train_end_at": window["train_end_at"].isoformat(),
                    "test_start_at": window["test_start_at"].isoformat(),
                    "test_end_at": window["test_end_at"].isoformat(),
                    "reason": "insufficient_train_trades",
                    "train_total_trades": train_trade_count,
                    "total_return_pct": 0.0,
                    "drawdown_from_peak_pct": current_drawdown_pct,
                    "guard_triggered": False,
                    "cooldown_remaining_after": 0,
                }
            )
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
        summary = summarize_portfolio(
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
        if summary["total_return_pct"] > 0:
            positive_periods += 1

        total_trades += slice_total_trades
        winning_trades += slice_winning_trades
        gross_profit += slice_gross_profit
        gross_loss += slice_gross_loss
        capital = final_capital
        if capital > peak_equity:
            peak_equity = capital

        current_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
        guard_triggered = False
        if current_drawdown_pct >= DRAWDOWN_GUARD_PCT and prior_drawdown_pct < DRAWDOWN_GUARD_PCT:
            cooldown_remaining = COOLDOWN_PERIODS
            guard_triggers += 1
            guard_triggered = True
        prior_drawdown_pct = current_drawdown_pct

        periods.append(
            {
                "train_start_at": window["train_start_at"].isoformat(),
                "train_end_at": window["train_end_at"].isoformat(),
                "test_start_at": window["test_start_at"].isoformat(),
                "test_end_at": window["test_end_at"].isoformat(),
                "reason": "active",
                "train_total_trades": train_trade_count,
                **summary,
                "drawdown_from_peak_pct": current_drawdown_pct,
                "guard_triggered": guard_triggered,
                "cooldown_remaining_after": cooldown_remaining,
            }
        )

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
    summary["period_count"] = len(periods)
    return {
        "source": source,
        "candle_count": len(candles),
        "first_candle_at": candles[0].open_time.isoformat() if candles else None,
        "last_candle_at": candles[-1].open_time.isoformat() if candles else None,
        "summary": summary,
        "periods": periods,
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

    reports: list[dict[str, Any]] = []

    binance_us_client = BinanceUSClient()
    try:
        binance_us_candles_1h = fetch_candles(
            client=binance_us_client,
            symbol=symbol,
            timeframe="1h",
            start_at=fetch_start_at,
            end_at=end_at,
        )
    finally:
        binance_us_client.close()
    binance_us_candles = resample_to_four_hour(binance_us_candles_1h)
    reports.append(
        evaluate_source(
            source="binance_us",
            symbol=symbol,
            candles=binance_us_candles,
            evaluation_start_at=evaluation_start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
        )
    )

    archive_client = BinanceArchiveClient()
    try:
        archive_spot_candles_1h = fetch_archive_spot_candles(
            client=archive_client,
            symbol=symbol,
            timeframe="1h",
            start_at=fetch_start_at,
            end_at=end_at,
        )
    finally:
        archive_client.close()
    archive_spot_candles = resample_to_four_hour(archive_spot_candles_1h)
    reports.append(
        evaluate_source(
            source="binance_archive_spot",
            symbol=symbol,
            candles=archive_spot_candles,
            evaluation_start_at=evaluation_start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
        )
    )

    reports.sort(
        key=lambda item: (
            item["summary"]["total_return_pct"],
            -item["summary"]["max_drawdown_pct"],
        ),
        reverse=True,
    )
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
            "drawdown_guard_pct": DRAWDOWN_GUARD_PCT,
            "cooldown_periods": COOLDOWN_PERIODS,
            "min_train_trades": MIN_TRAIN_TRADES,
            "variant_policy": "flat_only",
        },
        "reports": reports,
        "winner": reports[0],
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote BTC sleeve cross-source report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
