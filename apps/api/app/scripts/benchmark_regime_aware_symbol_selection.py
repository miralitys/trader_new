from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from math import log10
from pathlib import Path
from statistics import median
from typing import Any, Callable, Sequence

from app.benchmarks.category_strategy_suite import RegimeAwareStrategy
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle, BacktestResponse
from app.scripts.benchmark_regime_aware import build_regime_variants, evaluate_variant
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
    parser = argparse.ArgumentParser(
        description="Benchmark regime-aware strategy with train-time symbol whitelist/ranking policies."
    )
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


def _trailing_average_dollar_volume(
    candles: Sequence[BacktestCandle],
    end_at: datetime,
    period: int,
) -> float:
    window = [candle for candle in candles if candle.open_time < end_at]
    if len(window) < period or period <= 0:
        return 0.0
    sample = window[-period:]
    return sum(float(candle.close) * float(candle.volume) for candle in sample) / float(period)


def _profit_factor(result: BacktestResponse) -> float:
    gross_profit = 0.0
    gross_loss = 0.0
    for trade in result.trades:
        pnl = float(trade.pnl)
        if pnl > 0:
            gross_profit += pnl
        elif pnl < 0:
            gross_loss += abs(pnl)
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return gross_profit
    return 0.0


def _symbol_quality_score(
    *,
    return_pct: float,
    max_drawdown_pct: float,
    total_trades: int,
    profit_factor: float,
    average_dollar_volume: float,
) -> float:
    liquidity_bonus = max(0.0, min(log10(max(average_dollar_volume, 1.0)) - 5.0, 2.5)) * 0.35
    trade_bonus = min(float(total_trades), 6.0) * 0.2
    profit_factor_bonus = min(profit_factor, 3.0) * 1.25
    base_score = (
        return_pct
        - (max_drawdown_pct * 0.2)
        + trade_bonus
        + profit_factor_bonus
        + liquidity_bonus
    )
    if total_trades <= 0:
        return -1_000_000.0 + liquidity_bonus
    if return_pct <= 0:
        base_score += return_pct * 0.2
    return round(base_score, 6)


def evaluate_symbol_reports(
    *,
    variant: CategoryVariant,
    symbols: list[str],
    candle_cache: dict[str, list[BacktestCandle]],
    start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
) -> list[dict[str, Any]]:
    engine = BacktestEngine()
    volume_period = int(variant.overrides.get("volume_period", 20))
    reports: list[dict[str, Any]] = []
    for symbol in symbols:
        result = run_backtest(
            engine=engine,
            strategy=RegimeAwareStrategy(),
            strategy_code=f"regime_aware_symbol_selection:{variant.label}",
            symbol=symbol,
            timeframe="4h",
            start_at=start_at,
            end_at=end_at,
            candles=candle_cache[symbol],
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            overrides=variant.overrides,
        )
        average_dollar_volume = _trailing_average_dollar_volume(
            candles=candle_cache[symbol],
            end_at=end_at,
            period=volume_period,
        )
        return_pct = float(result.metrics.total_return_pct)
        max_drawdown_pct = float(result.metrics.max_drawdown_pct)
        total_trades = int(result.metrics.total_trades)
        profit_factor = _profit_factor(result)
        reports.append(
            {
                "symbol": symbol,
                "return_pct": round(return_pct, 4),
                "max_drawdown_pct": round(max_drawdown_pct, 4),
                "total_trades": total_trades,
                "profit_factor": round(profit_factor, 4),
                "average_dollar_volume": round(average_dollar_volume, 2),
                "score": _symbol_quality_score(
                    return_pct=return_pct,
                    max_drawdown_pct=max_drawdown_pct,
                    total_trades=total_trades,
                    profit_factor=profit_factor,
                    average_dollar_volume=average_dollar_volume,
                ),
            }
        )
    return sorted(
        reports,
        key=lambda item: (
            float(item["score"]),
            float(item["return_pct"]),
            float(item["profit_factor"]),
            -float(item["max_drawdown_pct"]),
        ),
        reverse=True,
    )


def _select_from_reports(
    reports: Sequence[dict[str, Any]],
    *,
    limit: int | None = None,
    predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[str]:
    filtered = [report for report in reports if predicate(report)] if predicate else list(reports)
    if not filtered:
        filtered = [report for report in reports if int(report["total_trades"]) > 0]
    if not filtered:
        filtered = list(reports)
    if limit is not None:
        filtered = filtered[:limit]
    return [str(report["symbol"]) for report in filtered]


def build_selection_policies(
    reports: Sequence[dict[str, Any]],
    minimum_liquidity: float,
) -> list[dict[str, Any]]:
    median_dollar_volume = median(float(report["average_dollar_volume"]) for report in reports)
    liquidity_floor = max(minimum_liquidity, median_dollar_volume)
    return [
        {
            "label": "full_universe",
            "description": "Baseline without symbol selection.",
            "selected_symbols": _select_from_reports(reports),
        },
        {
            "label": "top_2_quality",
            "description": "Top 2 symbols by train-time quality score.",
            "selected_symbols": _select_from_reports(reports, limit=2),
        },
        {
            "label": "top_3_quality",
            "description": "Top 3 symbols by train-time quality score.",
            "selected_symbols": _select_from_reports(reports, limit=3),
        },
        {
            "label": "top_4_quality",
            "description": "Top 4 symbols by train-time quality score.",
            "selected_symbols": _select_from_reports(reports, limit=4),
        },
        {
            "label": "positive_only",
            "description": "Keep only symbols with positive train return and at least 1 trade.",
            "selected_symbols": _select_from_reports(
                reports,
                predicate=lambda report: (
                    int(report["total_trades"]) > 0 and float(report["return_pct"]) > 0.0
                ),
            ),
        },
        {
            "label": "liquid_positive",
            "description": "Positive train symbols that also pass the higher liquidity floor.",
            "selected_symbols": _select_from_reports(
                reports,
                predicate=lambda report: (
                    int(report["total_trades"]) > 0
                    and float(report["return_pct"]) > 0.0
                    and float(report["average_dollar_volume"]) >= liquidity_floor
                ),
            ),
        },
        {
            "label": "liquid_top_3",
            "description": "Top 3 quality symbols among the more liquid half of the universe.",
            "selected_symbols": _select_from_reports(
                reports,
                limit=3,
                predicate=lambda report: (
                    int(report["total_trades"]) > 0
                    and float(report["average_dollar_volume"]) >= liquidity_floor
                ),
            ),
        },
    ]


def _policy_selection_summary(
    selected_symbols: Sequence[str],
    train_reports: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    selected_set = set(selected_symbols)
    selected_reports = [report for report in train_reports if str(report["symbol"]) in selected_set]
    average_score = 0.0
    average_dollar_volume = 0.0
    if selected_reports:
        average_score = sum(float(report["score"]) for report in selected_reports) / float(len(selected_reports))
        average_dollar_volume = sum(
            float(report["average_dollar_volume"]) for report in selected_reports
        ) / float(len(selected_reports))
    return {
        "selected_count": len(selected_symbols),
        "average_symbol_score": round(average_score, 4),
        "average_dollar_volume": round(average_dollar_volume, 2),
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
        candle_cache: dict[str, list[BacktestCandle]] = {}
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

    train_variant_reports = []
    for variant in build_regime_variants():
        print(f"Evaluating regime-aware variant {variant.label} on full train universe...")
        train_variant_reports.append(
            evaluate_variant(
                strategy_factory=RegimeAwareStrategy,
                strategy_code_prefix="regime_aware_symbol_selection",
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

    best_variant_report = max(
        train_variant_reports,
        key=lambda item: (item["score"], item["metrics"]["portfolio_return_pct"]),
    )
    selected_variant = CategoryVariant(
        label=str(best_variant_report["label"]),
        overrides=dict(best_variant_report["overrides"]),
    )
    selected_variant_min_liquidity = float(selected_variant.overrides.get("min_average_dollar_volume", 0.0))

    baseline_train = evaluate_variant(
        strategy_factory=RegimeAwareStrategy,
        strategy_code_prefix="regime_aware_symbol_selection",
        variant=selected_variant,
        symbols=symbols,
        candle_cache=candle_cache,
        start_at=evaluation_start_at,
        end_at=train_end_at,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
    )
    baseline_test = evaluate_variant(
        strategy_factory=RegimeAwareStrategy,
        strategy_code_prefix="regime_aware_symbol_selection",
        variant=selected_variant,
        symbols=symbols,
        candle_cache=candle_cache,
        start_at=train_end_at,
        end_at=end_at,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
    )

    train_symbol_reports = evaluate_symbol_reports(
        variant=selected_variant,
        symbols=symbols,
        candle_cache=candle_cache,
        start_at=evaluation_start_at,
        end_at=train_end_at,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
    )
    selection_policies = build_selection_policies(
        reports=train_symbol_reports,
        minimum_liquidity=selected_variant_min_liquidity,
    )

    policy_reports = []
    for policy in selection_policies:
        selected_symbols = list(policy["selected_symbols"])
        print(f"Evaluating selection policy {policy['label']} on symbols: {', '.join(selected_symbols)}")
        train_report = evaluate_variant(
            strategy_factory=RegimeAwareStrategy,
            strategy_code_prefix=f"regime_aware_symbol_selection:{policy['label']}",
            variant=selected_variant,
            symbols=selected_symbols,
            candle_cache=candle_cache,
            start_at=evaluation_start_at,
            end_at=train_end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
        )
        test_report = evaluate_variant(
            strategy_factory=RegimeAwareStrategy,
            strategy_code_prefix=f"regime_aware_symbol_selection:{policy['label']}",
            variant=selected_variant,
            symbols=selected_symbols,
            candle_cache=candle_cache,
            start_at=train_end_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
        )
        score = float(test_report["metrics"]["portfolio_return_pct"]) - (
            float(test_report["metrics"]["average_max_drawdown_pct"]) * 0.15
        )
        policy_reports.append(
            {
                "label": str(policy["label"]),
                "description": str(policy["description"]),
                "selected_symbols": selected_symbols,
                "selection_summary": _policy_selection_summary(
                    selected_symbols=selected_symbols,
                    train_reports=train_symbol_reports,
                ),
                "train": train_report,
                "test": test_report,
                "score": round(score, 6),
            }
        )

    best_policy = max(
        policy_reports,
        key=lambda item: (
            item["score"],
            item["test"]["metrics"]["portfolio_return_pct"],
        ),
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
        "selected_regime_variant": {
            "label": selected_variant.label,
            "overrides": selected_variant.overrides,
            "train_selection_report": best_variant_report,
            "baseline_full_universe": {
                "train": baseline_train,
                "test": baseline_test,
            },
        },
        "train_symbol_reports": train_symbol_reports,
        "selection_policies": policy_reports,
        "best_policy": best_policy,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote regime-aware symbol selection report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
