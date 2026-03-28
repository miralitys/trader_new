from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Sequence

from app.benchmarks.category_strategy_suite import RegimeAwareStrategy
from app.engines.backtest_engine import BacktestEngine
from app.integrations.binance_us.client import BinanceUSClient
from app.schemas.backtest import BacktestCandle, BacktestResponse, EquityPoint
from app.scripts.benchmark_regime_aware import build_regime_variants, evaluate_variant
from app.scripts.benchmark_regime_aware_adaptive_allocator import (
    BASE_SYMBOL_CAPITAL,
    add_months,
    append_points,
    build_slice_curve,
    max_drawdown_pct,
    next_rebalance_at,
    summarize_portfolio,
)
from app.scripts.benchmark_regime_aware_symbol_selection import evaluate_symbol_reports
from app.scripts.benchmark_strategy_categories import (
    CategoryVariant,
    fetch_candles,
    parse_datetime,
    run_backtest,
)

UTC = timezone.utc
ZERO = Decimal("0")


@dataclass(frozen=True)
class BtcAdaptiveFilterConfig:
    label: str
    lookback_months: int
    rebalance_mode: str
    min_trades: int
    min_profit_factor: Optional[float]
    min_symbol_score: Optional[float]
    variant_policy: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "lookback_months": self.lookback_months,
            "rebalance_mode": self.rebalance_mode,
            "min_trades": self.min_trades,
            "min_profit_factor": self.min_profit_factor,
            "min_symbol_score": self.min_symbol_score,
            "variant_policy": self.variant_policy,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep BTC-only adaptive allocator meta-filters.")
    parser.add_argument("--symbol", default="BTC-USDT")
    parser.add_argument("--evaluation-start-at", default="2025-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--output")
    return parser.parse_args()


def build_configs() -> tuple[BtcAdaptiveFilterConfig, ...]:
    configs: list[BtcAdaptiveFilterConfig] = []
    for rebalance_mode in ("monthly", "biweekly"):
        for lookback_months in (2, 3, 4):
            for min_trades in (1, 2):
                for min_profit_factor in (None, 0.25, 0.5):
                    for min_symbol_score in (None, -1.5, -1.0):
                        for variant_policy in ("any", "no_balanced", "flat_only"):
                            pf_label = "pf_any" if min_profit_factor is None else f"pf{str(min_profit_factor).replace('.', '_')}"
                            score_label = (
                                "score_any"
                                if min_symbol_score is None
                                else f"score_{str(min_symbol_score).replace('-', 'm').replace('.', '_')}"
                            )
                            configs.append(
                                BtcAdaptiveFilterConfig(
                                    label=(
                                        f"{rebalance_mode}_lb{lookback_months}m"
                                        f"_t{min_trades}_{pf_label}_{score_label}_{variant_policy}"
                                    ),
                                    lookback_months=lookback_months,
                                    rebalance_mode=rebalance_mode,
                                    min_trades=min_trades,
                                    min_profit_factor=min_profit_factor,
                                    min_symbol_score=min_symbol_score,
                                    variant_policy=variant_policy,
                                )
                            )
    return tuple(configs)


def generate_windows(
    evaluation_start_at: datetime,
    end_at: datetime,
    lookback_months: int,
    rebalance_mode: str,
) -> list[dict[str, datetime]]:
    windows: list[dict[str, datetime]] = []
    test_start_at = evaluation_start_at
    while test_start_at < end_at:
        test_end_at = min(next_rebalance_at(test_start_at, rebalance_mode), end_at)
        windows.append(
            {
                "train_start_at": add_months(test_start_at, -lookback_months),
                "train_end_at": test_start_at,
                "test_start_at": test_start_at,
                "test_end_at": test_end_at,
            }
        )
        if test_end_at <= test_start_at:
            break
        test_start_at = test_end_at
    return windows


def variant_allowed(label: str, variant_policy: str) -> bool:
    if variant_policy == "any":
        return True
    if variant_policy == "no_balanced":
        return label != "balanced"
    if variant_policy == "flat_only":
        return label == "flat_priority"
    raise ValueError(f"Unsupported variant_policy: {variant_policy}")


def score_summary(summary: dict[str, Any]) -> float:
    return round(
        float(summary["total_return_pct"]) - (float(summary["max_drawdown_pct"]) * 0.15),
        6,
    )


def main() -> None:
    args = parse_args()
    symbol = args.symbol.strip().upper()
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))
    configs = build_configs()
    max_lookback_months = max(config.lookback_months for config in configs)
    fetch_start_at = add_months(evaluation_start_at, -max_lookback_months) - timedelta(days=args.warmup_days)

    client = BinanceUSClient()
    try:
        candle_cache: dict[str, list[BacktestCandle]] = {
            symbol: fetch_candles(
                client=client,
                symbol=symbol,
                timeframe="4h",
                start_at=fetch_start_at,
                end_at=end_at,
            )
        }
    finally:
        client.close()

    engine = BacktestEngine()
    variant_report_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
    train_symbol_cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    test_result_cache: dict[tuple[str, str, str], BacktestResponse] = {}

    def get_variant_reports(train_start_at: datetime, train_end_at: datetime) -> list[dict[str, Any]]:
        reports = []
        for variant in build_regime_variants():
            key = (train_start_at.isoformat(), train_end_at.isoformat(), variant.label)
            cached = variant_report_cache.get(key)
            if cached is None:
                cached = evaluate_variant(
                    strategy_factory=RegimeAwareStrategy,
                    strategy_code_prefix="btc_adaptive_filter_sweep",
                    variant=variant,
                    symbols=[symbol],
                    candle_cache=candle_cache,
                    start_at=train_start_at,
                    end_at=train_end_at,
                    initial_capital=BASE_SYMBOL_CAPITAL,
                    fee=fee,
                    slippage=slippage,
                )
                variant_report_cache[key] = cached
            reports.append(cached)
        return reports

    def get_train_symbol_report(
        train_start_at: datetime,
        train_end_at: datetime,
        variant: CategoryVariant,
    ) -> dict[str, Any]:
        key = (train_start_at.isoformat(), train_end_at.isoformat(), variant.label)
        cached = train_symbol_cache.get(key)
        if cached is None:
            cached = evaluate_symbol_reports(
                variant=variant,
                symbols=[symbol],
                candle_cache=candle_cache,
                start_at=train_start_at,
                end_at=train_end_at,
                initial_capital=BASE_SYMBOL_CAPITAL,
                fee=fee,
                slippage=slippage,
            )
            train_symbol_cache[key] = cached
        return dict(cached[0])

    def get_test_result(
        test_start_at: datetime,
        test_end_at: datetime,
        variant: CategoryVariant,
    ) -> BacktestResponse:
        key = (test_start_at.isoformat(), test_end_at.isoformat(), variant.label)
        cached = test_result_cache.get(key)
        if cached is None:
            cached = run_backtest(
                engine=engine,
                strategy=RegimeAwareStrategy(),
                strategy_code=f"btc_adaptive_filter_sweep:{variant.label}",
                symbol=symbol,
                timeframe="4h",
                start_at=test_start_at,
                end_at=test_end_at,
                candles=candle_cache[symbol],
                initial_capital=BASE_SYMBOL_CAPITAL,
                fee=fee,
                slippage=slippage,
                overrides=variant.overrides,
            )
            test_result_cache[key] = cached
        return cached

    reports: list[dict[str, Any]] = []
    for config in configs:
        print(f"Evaluating {config.label}...")
        windows = generate_windows(
            evaluation_start_at=evaluation_start_at,
            end_at=end_at,
            lookback_months=config.lookback_months,
            rebalance_mode=config.rebalance_mode,
        )
        capital = initial_capital
        equity_curve: list[EquityPoint] = [
            EquityPoint(
                timestamp=evaluation_start_at,
                equity=initial_capital,
                cash=initial_capital,
                close_price=initial_capital,
                position_qty=ZERO,
            )
        ]
        total_trades = 0
        winning_trades = 0
        gross_profit = ZERO
        gross_loss = ZERO
        positive_periods = 0
        cash_periods = 0
        periods: list[dict[str, Any]] = []

        for window in windows:
            variant_reports = [
                report
                for report in get_variant_reports(window["train_start_at"], window["train_end_at"])
                if variant_allowed(str(report["label"]), config.variant_policy)
            ]
            if not variant_reports:
                cash_periods += 1
                append_points(
                    equity_curve,
                    build_slice_curve((), initial_capital=capital, test_end_at=window["test_end_at"]),
                )
                periods.append(
                    {
                        "train_start_at": window["train_start_at"].isoformat(),
                        "train_end_at": window["train_end_at"].isoformat(),
                        "test_start_at": window["test_start_at"].isoformat(),
                        "test_end_at": window["test_end_at"].isoformat(),
                        "cash_mode": True,
                        "reason": "no_variant_allowed",
                        "ending_capital": round(float(capital), 4),
                    }
                )
                continue

            best_variant_report = max(
                variant_reports,
                key=lambda item: (item["score"], item["metrics"]["portfolio_return_pct"]),
            )
            variant = CategoryVariant(
                label=str(best_variant_report["label"]),
                overrides=dict(best_variant_report["overrides"]),
            )
            train_symbol_report = get_train_symbol_report(
                train_start_at=window["train_start_at"],
                train_end_at=window["train_end_at"],
                variant=variant,
            )

            should_trade = True
            skip_reason = ""
            if int(train_symbol_report["total_trades"]) < config.min_trades:
                should_trade = False
                skip_reason = "min_trades_not_met"
            if should_trade and config.min_profit_factor is not None:
                if float(train_symbol_report["profit_factor"]) < config.min_profit_factor:
                    should_trade = False
                    skip_reason = "min_profit_factor_not_met"
            if should_trade and config.min_symbol_score is not None:
                if float(train_symbol_report["score"]) < config.min_symbol_score:
                    should_trade = False
                    skip_reason = "min_symbol_score_not_met"

            if not should_trade:
                cash_periods += 1
                append_points(
                    equity_curve,
                    build_slice_curve((), initial_capital=capital, test_end_at=window["test_end_at"]),
                )
                periods.append(
                    {
                        "train_start_at": window["train_start_at"].isoformat(),
                        "train_end_at": window["train_end_at"].isoformat(),
                        "test_start_at": window["test_start_at"].isoformat(),
                        "test_end_at": window["test_end_at"].isoformat(),
                        "selected_variant": variant.label,
                        "train_symbol_report": train_symbol_report,
                        "cash_mode": True,
                        "reason": skip_reason,
                        "ending_capital": round(float(capital), 4),
                    }
                )
                continue

            base_result = get_test_result(
                test_start_at=window["test_start_at"],
                test_end_at=window["test_end_at"],
                variant=variant,
            )
            scale_factor = capital / BASE_SYMBOL_CAPITAL
            slice_curve = build_slice_curve(
                scaled_results=((base_result, scale_factor, capital),),
                initial_capital=capital,
                test_end_at=window["test_end_at"],
            )
            append_points(equity_curve, slice_curve)
            slice_final_capital = base_result.final_equity * scale_factor

            slice_total_trades = base_result.metrics.total_trades
            slice_winning_trades = 0
            slice_gross_profit = ZERO
            slice_gross_loss = ZERO
            for trade in base_result.trades:
                pnl = trade.pnl * scale_factor
                if pnl > ZERO:
                    slice_winning_trades += 1
                    slice_gross_profit += pnl
                elif pnl < ZERO:
                    slice_gross_loss += abs(pnl)

            slice_summary = summarize_portfolio(
                initial_capital=capital,
                final_capital=slice_final_capital,
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
            capital = slice_final_capital
            periods.append(
                {
                    "train_start_at": window["train_start_at"].isoformat(),
                    "train_end_at": window["train_end_at"].isoformat(),
                    "test_start_at": window["test_start_at"].isoformat(),
                    "test_end_at": window["test_end_at"].isoformat(),
                    "selected_variant": variant.label,
                    "train_symbol_report": train_symbol_report,
                    "cash_mode": False,
                    **slice_summary,
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
        summary["period_count"] = len(periods)
        summary["score"] = score_summary(summary)
        reports.append(
            {
                "config": config.as_dict(),
                "summary": summary,
                "periods": periods,
            }
        )

    reports.sort(
        key=lambda item: (
            item["summary"]["total_return_pct"],
            item["summary"]["score"],
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
        "reports": reports,
        "winner": reports[0],
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote BTC adaptive filter report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
