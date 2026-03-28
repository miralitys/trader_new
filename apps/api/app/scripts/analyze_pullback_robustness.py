from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from dateutil.relativedelta import relativedelta

from app.benchmarks.category_strategy_suite import PullbackStrategy
from app.engines.backtest_engine import BacktestEngine
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle, BacktestResponse
from app.scripts.benchmark_strategy_categories import (
    CategoryVariant,
    aggregate_results,
    fetch_candles,
    parse_datetime,
    parse_symbols,
    run_backtest,
    score_aggregate,
)

UTC = timezone.utc
WINNER_TIMEFRAME = "4h"
BASE_VARIANT_LABEL = "ema50_200_wide"
BASE_OVERRIDES: dict[str, Any] = {
    "fast_ema_period": 50,
    "slow_ema_period": 200,
    "signal_ema_period": 20,
    "pullback_lookback": 30,
    "min_pullback_pct": 0.02,
    "max_pullback_pct": 0.08,
    "max_bars_in_trade": 144,
}


@dataclass(frozen=True)
class StressScenario:
    label: str
    fee: Decimal
    slippage: Decimal


@dataclass(frozen=True)
class WalkForwardFold:
    index: int
    train_start_at: datetime
    train_end_at: datetime
    test_end_at: datetime

    @property
    def test_start_at(self) -> datetime:
        return self.train_end_at

    def as_dict(self) -> dict[str, str | int]:
        return {
            "index": self.index,
            "train_start_at": self.train_start_at.isoformat(),
            "train_end_at": self.train_end_at.isoformat(),
            "test_start_at": self.test_start_at.isoformat(),
            "test_end_at": self.test_end_at.isoformat(),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Robustness analysis for the winning pullback strategy.")
    parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,ADA-USDT,ALPINE-USDT,XRP-USDT,1INCH-USDT,LTC-USDT,BCH-USDT,AVAX-USDT,LINK-USDT,DOGE-USDT")
    parser.add_argument("--walkforward-start-at", default="2025-09-15T00:00:00+00:00")
    parser.add_argument("--train-months", type=int, default=2)
    parser.add_argument("--test-months", type=int, default=1)
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument("--final-test-start-at", default="2025-12-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=75)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--output")
    return parser.parse_args()


def build_neighbor_variants() -> tuple[CategoryVariant, ...]:
    return (
        CategoryVariant(label="ema40_180_fast", overrides={
            "fast_ema_period": 40,
            "slow_ema_period": 180,
            "signal_ema_period": 15,
            "pullback_lookback": 24,
            "min_pullback_pct": 0.015,
            "max_pullback_pct": 0.07,
            "max_bars_in_trade": 120,
        }),
        CategoryVariant(label="ema40_180_balanced", overrides={
            "fast_ema_period": 40,
            "slow_ema_period": 180,
            "signal_ema_period": 20,
            "pullback_lookback": 30,
            "min_pullback_pct": 0.02,
            "max_pullback_pct": 0.08,
            "max_bars_in_trade": 120,
        }),
        CategoryVariant(label=BASE_VARIANT_LABEL, overrides=dict(BASE_OVERRIDES)),
        CategoryVariant(label="ema50_200_tighter", overrides={
            "fast_ema_period": 50,
            "slow_ema_period": 200,
            "signal_ema_period": 20,
            "pullback_lookback": 24,
            "min_pullback_pct": 0.02,
            "max_pullback_pct": 0.06,
            "max_bars_in_trade": 120,
        }),
        CategoryVariant(label="ema50_200_earlier_trigger", overrides={
            "fast_ema_period": 50,
            "slow_ema_period": 200,
            "signal_ema_period": 10,
            "pullback_lookback": 30,
            "min_pullback_pct": 0.02,
            "max_pullback_pct": 0.08,
            "max_bars_in_trade": 96,
        }),
        CategoryVariant(label="ema50_200_later_trigger", overrides={
            "fast_ema_period": 50,
            "slow_ema_period": 200,
            "signal_ema_period": 30,
            "pullback_lookback": 30,
            "min_pullback_pct": 0.02,
            "max_pullback_pct": 0.08,
            "max_bars_in_trade": 192,
        }),
        CategoryVariant(label="ema50_200_wider", overrides={
            "fast_ema_period": 50,
            "slow_ema_period": 200,
            "signal_ema_period": 20,
            "pullback_lookback": 36,
            "min_pullback_pct": 0.015,
            "max_pullback_pct": 0.10,
            "max_bars_in_trade": 168,
        }),
        CategoryVariant(label="ema60_220_balanced", overrides={
            "fast_ema_period": 60,
            "slow_ema_period": 220,
            "signal_ema_period": 20,
            "pullback_lookback": 30,
            "min_pullback_pct": 0.02,
            "max_pullback_pct": 0.08,
            "max_bars_in_trade": 168,
        }),
        CategoryVariant(label="ema60_250_wide", overrides={
            "fast_ema_period": 60,
            "slow_ema_period": 250,
            "signal_ema_period": 25,
            "pullback_lookback": 36,
            "min_pullback_pct": 0.025,
            "max_pullback_pct": 0.10,
            "max_bars_in_trade": 168,
        }),
    )


def build_stress_scenarios(base_fee: Decimal, base_slippage: Decimal) -> tuple[StressScenario, ...]:
    return (
        StressScenario(label="lighter_friction", fee=Decimal("0.00075"), slippage=Decimal("0.00025")),
        StressScenario(label="base", fee=base_fee, slippage=base_slippage),
        StressScenario(label="heavy_friction", fee=Decimal("0.0015"), slippage=Decimal("0.00075")),
        StressScenario(label="very_heavy_friction", fee=Decimal("0.002"), slippage=Decimal("0.001")),
    )


def build_walkforward_folds(
    start_at: datetime,
    train_months: int,
    test_months: int,
    folds: int,
    final_end_at: datetime,
) -> list[WalkForwardFold]:
    items: list[WalkForwardFold] = []
    cursor = start_at
    for index in range(folds):
        train_end_at = cursor + relativedelta(months=train_months)
        test_end_at = train_end_at + relativedelta(months=test_months)
        if test_end_at > final_end_at:
            break
        items.append(
            WalkForwardFold(
                index=index + 1,
                train_start_at=cursor,
                train_end_at=train_end_at,
                test_end_at=test_end_at,
            )
        )
        cursor = cursor + relativedelta(months=test_months)
    return items


def evaluate_variant_on_window(
    engine: BacktestEngine,
    variant: CategoryVariant,
    symbols: list[str],
    candle_cache: dict[str, list[BacktestCandle]],
    start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
) -> tuple[dict[str, Any], list[BacktestResponse]]:
    results: list[BacktestResponse] = []
    for symbol in symbols:
        strategy = PullbackStrategy()
        result = run_backtest(
            engine=engine,
            strategy=strategy,
            strategy_code=f"pullback_robustness:{variant.label}",
            symbol=symbol,
            timeframe=WINNER_TIMEFRAME,
            start_at=start_at,
            end_at=end_at,
            candles=candle_cache[symbol],
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            overrides=variant.overrides,
        )
        results.append(result)

    aggregate = aggregate_results(results)
    return aggregate, results


def run_walkforward(
    folds: list[WalkForwardFold],
    variants: tuple[CategoryVariant, ...],
    symbols: list[str],
    candle_cache: dict[str, list[BacktestCandle]],
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
) -> dict[str, Any]:
    engine = BacktestEngine()
    fold_reports: list[dict[str, Any]] = []
    all_test_results: list[BacktestResponse] = []
    selection_counter: Counter[str] = Counter()

    for fold in folds:
        print(
            f"Walk-forward fold {fold.index}: "
            f"train {fold.train_start_at.date()} -> {fold.train_end_at.date()}, "
            f"test {fold.test_start_at.date()} -> {fold.test_end_at.date()}"
        )
        train_candidates: list[dict[str, Any]] = []
        for variant in variants:
            train_aggregate, _ = evaluate_variant_on_window(
                engine=engine,
                variant=variant,
                symbols=symbols,
                candle_cache=candle_cache,
                start_at=fold.train_start_at,
                end_at=fold.train_end_at,
                initial_capital=initial_capital,
                fee=fee,
                slippage=slippage,
            )
            train_candidates.append(
                {
                    "label": variant.label,
                    "overrides": variant.overrides,
                    "train": train_aggregate,
                    "score": round(score_aggregate(train_aggregate), 6),
                }
            )

        best_train = max(
            train_candidates,
            key=lambda item: (
                item["score"],
                item["train"]["portfolio_return_pct"],
                -item["train"]["average_max_drawdown_pct"],
                item["train"]["profit_factor"],
            ),
        )
        chosen_variant = next(variant for variant in variants if variant.label == best_train["label"])
        selection_counter[chosen_variant.label] += 1
        test_aggregate, test_results = evaluate_variant_on_window(
            engine=engine,
            variant=chosen_variant,
            symbols=symbols,
            candle_cache=candle_cache,
            start_at=fold.test_start_at,
            end_at=fold.test_end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
        )
        all_test_results.extend(test_results)
        fold_reports.append(
            {
                "fold": fold.as_dict(),
                "selected_variant": {
                    "label": chosen_variant.label,
                    "overrides": chosen_variant.overrides,
                },
                "train": best_train["train"],
                "test": test_aggregate,
            }
        )
        print(
            f"  selected {chosen_variant.label} -> "
            f"test return {test_aggregate['portfolio_return_pct']}%, "
            f"dd {test_aggregate['average_max_drawdown_pct']}%, "
            f"trades {test_aggregate['total_trades']}"
        )

    profitable_folds = sum(1 for report in fold_reports if float(report["test"]["portfolio_return_pct"]) > 0.0)
    average_fold_return = 0.0
    if fold_reports:
        average_fold_return = sum(float(report["test"]["portfolio_return_pct"]) for report in fold_reports) / float(len(fold_reports))

    return {
        "folds": fold_reports,
        "summary": {
            "fold_count": len(fold_reports),
            "profitable_folds": profitable_folds,
            "selection_frequency": dict(selection_counter),
            "average_test_return_pct": round(average_fold_return, 4),
            "aggregate_test": aggregate_results(all_test_results),
        },
    }


def run_parameter_sensitivity(
    variants: tuple[CategoryVariant, ...],
    symbols: list[str],
    candle_cache: dict[str, list[BacktestCandle]],
    start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
) -> dict[str, Any]:
    engine = BacktestEngine()
    reports: list[dict[str, Any]] = []
    for variant in variants:
        aggregate, _ = evaluate_variant_on_window(
            engine=engine,
            variant=variant,
            symbols=symbols,
            candle_cache=candle_cache,
            start_at=start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
        )
        reports.append(
            {
                "label": variant.label,
                "overrides": variant.overrides,
                "metrics": aggregate,
                "score": round(score_aggregate(aggregate), 6),
            }
        )

    ranked = sorted(
        reports,
        key=lambda item: (
            item["metrics"]["portfolio_return_pct"],
            -item["metrics"]["average_max_drawdown_pct"],
            item["metrics"]["profit_factor"],
        ),
        reverse=True,
    )
    profitable_count = sum(1 for item in ranked if float(item["metrics"]["portfolio_return_pct"]) > 0.0)
    profitable_labels = [item["label"] for item in ranked if float(item["metrics"]["portfolio_return_pct"]) > 0.0]
    return {
        "ranked_variants": ranked,
        "summary": {
            "tested_variants": len(ranked),
            "profitable_variants": profitable_count,
            "profitable_share": round(profitable_count / float(len(ranked)), 4) if ranked else 0.0,
            "profitable_labels": profitable_labels,
            "best_variant": ranked[0] if ranked else None,
            "worst_variant": ranked[-1] if ranked else None,
        },
    }


def run_stress_test(
    scenarios: tuple[StressScenario, ...],
    symbols: list[str],
    candle_cache: dict[str, list[BacktestCandle]],
    start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
) -> dict[str, Any]:
    engine = BacktestEngine()
    variant = CategoryVariant(label=BASE_VARIANT_LABEL, overrides=dict(BASE_OVERRIDES))
    reports: list[dict[str, Any]] = []
    for scenario in scenarios:
        aggregate, _ = evaluate_variant_on_window(
            engine=engine,
            variant=variant,
            symbols=symbols,
            candle_cache=candle_cache,
            start_at=start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=scenario.fee,
            slippage=scenario.slippage,
        )
        reports.append(
            {
                "label": scenario.label,
                "fee": str(scenario.fee),
                "slippage": str(scenario.slippage),
                "metrics": aggregate,
            }
        )
        print(
            f"Stress {scenario.label}: return {aggregate['portfolio_return_pct']}%, "
            f"dd {aggregate['average_max_drawdown_pct']}%, trades {aggregate['total_trades']}"
        )
    return {
        "scenarios": reports,
    }


def main() -> None:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    walkforward_start_at = parse_datetime(args.walkforward_start_at)
    final_test_start_at = parse_datetime(args.final_test_start_at)
    end_at = parse_datetime(args.end_at)
    if not (walkforward_start_at < final_test_start_at < end_at):
        raise ValueError("Expected walkforward_start_at < final_test_start_at < end_at")

    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))
    warmup_start_at = walkforward_start_at - timedelta(days=args.warmup_days)

    client = BinanceUSClient()
    candle_cache: dict[str, list[BacktestCandle]] = {}
    try:
        for symbol in symbols:
            print(
                f"Fetching {symbol} {WINNER_TIMEFRAME} candles "
                f"from {warmup_start_at.isoformat()} to {end_at.isoformat()}..."
            )
            candle_cache[symbol] = fetch_candles(
                client=client,
                symbol=symbol,
                timeframe=WINNER_TIMEFRAME,
                start_at=warmup_start_at,
                end_at=end_at,
            )
            print(f"Loaded {len(candle_cache[symbol])} candles for {symbol} {WINNER_TIMEFRAME}")
    finally:
        client.close()

    variants = build_neighbor_variants()
    folds = build_walkforward_folds(
        start_at=walkforward_start_at,
        train_months=args.train_months,
        test_months=args.test_months,
        folds=args.folds,
        final_end_at=end_at,
    )
    if not folds:
        raise ValueError("No walk-forward folds generated")

    walkforward = run_walkforward(
        folds=folds,
        variants=variants,
        symbols=symbols,
        candle_cache=candle_cache,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
    )
    sensitivity = run_parameter_sensitivity(
        variants=variants,
        symbols=symbols,
        candle_cache=candle_cache,
        start_at=final_test_start_at,
        end_at=end_at,
        initial_capital=initial_capital,
        fee=fee,
        slippage=slippage,
    )
    stress = run_stress_test(
        scenarios=build_stress_scenarios(base_fee=fee, base_slippage=slippage),
        symbols=symbols,
        candle_cache=candle_cache,
        start_at=final_test_start_at,
        end_at=end_at,
        initial_capital=initial_capital,
    )

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "strategy": {
            "name": "Pullback / Retracement",
            "timeframe": WINNER_TIMEFRAME,
            "base_variant": {
                "label": BASE_VARIANT_LABEL,
                "overrides": dict(BASE_OVERRIDES),
            },
        },
        "symbols": symbols,
        "data_source": "Binance.US spot klines via /api/v3/klines",
        "window": {
            "warmup_start_at": warmup_start_at.isoformat(),
            "walkforward_start_at": walkforward_start_at.isoformat(),
            "final_test_start_at": final_test_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "friction": {
            "base_fee": str(fee),
            "base_slippage": str(slippage),
        },
        "walkforward": walkforward,
        "parameter_sensitivity": sensitivity,
        "stress_test": stress,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote robustness report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
