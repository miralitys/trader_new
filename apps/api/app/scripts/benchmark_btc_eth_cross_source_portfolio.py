from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Sequence

from app.engines.backtest_engine import BacktestEngine
from app.schemas.backtest import BacktestCandle, BacktestResponse, EquityPoint
from app.scripts.benchmark_btc_cross_source_trend_sweep import fetch_source_candles
from app.scripts.benchmark_btc_sleeve_risk_controls import (
    drawdown_from_peak_pct,
    generate_windows,
    make_cash_curve,
    run_variant_backtest,
    scaled_trade_stats,
)
from app.scripts.benchmark_eth_cross_source_refine import build_variants as build_eth_refine_variants
from app.scripts.benchmark_regime_aware import build_regime_variants
from app.scripts.benchmark_regime_aware_adaptive_allocator import (
    BASE_SYMBOL_CAPITAL,
    add_months,
    append_points,
    build_slice_curve,
    summarize_portfolio,
)
from app.scripts.benchmark_strategy_categories import CategoryVariant, parse_datetime

UTC = timezone.utc
ZERO = Decimal("0")
TRAIN_POSITION_SIZE_PCT = Decimal("0.10")
TEST_POSITION_SIZE_PCT = Decimal("1.00")
LOOKBACK_MONTHS = 2
DRAWDOWN_GUARD_PCT = 4.0
COOLDOWN_PERIODS = 3
BTC_SYMBOL = "BTC-USDT"
ETH_SYMBOL = "ETH-USDT"
SCENARIO_COSTS: dict[str, tuple[Decimal, Decimal]] = {
    "base": (Decimal("0.001"), Decimal("0.0005")),
    "moderate_costs": (Decimal("0.0015"), Decimal("0.00075")),
    "heavy_costs": (Decimal("0.002"), Decimal("0.001")),
}


@dataclass(frozen=True)
class PortfolioConfig:
    label: str
    include_eth: bool
    eth_min_train_trades: int = 0
    eth_min_profit_factor: Optional[float] = None
    eth_require_positive_train_return: bool = False
    eth_stress_gate: Optional[str] = None
    eth_stress_min_return_pct: float = 0.0
    eth_stress_min_profit_factor: Optional[float] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "include_eth": self.include_eth,
            "eth_min_train_trades": self.eth_min_train_trades,
            "eth_min_profit_factor": self.eth_min_profit_factor,
            "eth_require_positive_train_return": self.eth_require_positive_train_return,
            "eth_stress_gate": self.eth_stress_gate,
            "eth_stress_min_return_pct": self.eth_stress_min_return_pct,
            "eth_stress_min_profit_factor": self.eth_stress_min_profit_factor,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cross-source portfolio benchmark for BTC-only versus BTC plus optional ETH."
    )
    parser.add_argument("--evaluation-start-at", default="2022-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--config-labels")
    parser.add_argument("--output")
    return parser.parse_args()


def parse_config_labels(value: Optional[str]) -> set[str]:
    if value is None:
        return set()
    return {item.strip() for item in value.split(",") if item.strip()}


def build_configs() -> tuple[PortfolioConfig, ...]:
    return (
        PortfolioConfig(label="btc_only", include_eth=False),
        # Current primary portfolio sleeve: BTC always on, ETH only after train and cost-aware gating.
        PortfolioConfig(
            label="btc_eth_optional_primary",
            include_eth=True,
            eth_min_train_trades=1,
            eth_min_profit_factor=1.0,
            eth_require_positive_train_return=True,
            eth_stress_gate="moderate_costs",
            eth_stress_min_return_pct=0.0,
        ),
        PortfolioConfig(
            label="btc_eth_t1_pf1_modret",
            include_eth=True,
            eth_min_train_trades=1,
            eth_min_profit_factor=1.0,
            eth_require_positive_train_return=True,
            eth_stress_gate="moderate_costs",
            eth_stress_min_return_pct=0.0,
        ),
        PortfolioConfig(
            label="btc_eth_t1_pf1_modpf1",
            include_eth=True,
            eth_min_train_trades=1,
            eth_min_profit_factor=1.0,
            eth_require_positive_train_return=True,
            eth_stress_gate="moderate_costs",
            eth_stress_min_return_pct=0.0,
            eth_stress_min_profit_factor=1.0,
        ),
        PortfolioConfig(
            label="btc_eth_t2_pf1_modret",
            include_eth=True,
            eth_min_train_trades=2,
            eth_min_profit_factor=1.0,
            eth_require_positive_train_return=True,
            eth_stress_gate="moderate_costs",
            eth_stress_min_return_pct=0.0,
        ),
        PortfolioConfig(
            label="btc_eth_t1_pf11_modret",
            include_eth=True,
            eth_min_train_trades=1,
            eth_min_profit_factor=1.1,
            eth_require_positive_train_return=True,
            eth_stress_gate="moderate_costs",
            eth_stress_min_return_pct=0.0,
        ),
        PortfolioConfig(
            label="btc_eth_t1_pf1_heavyret",
            include_eth=True,
            eth_min_train_trades=1,
            eth_min_profit_factor=1.0,
            eth_require_positive_train_return=True,
            eth_stress_gate="heavy_costs",
            eth_stress_min_return_pct=0.0,
        ),
    )


def find_btc_variant() -> CategoryVariant:
    for variant in build_regime_variants():
        if variant.label == "flat_priority_cross_source":
            return variant
    raise ValueError("flat_priority_cross_source regime variant is not available")


def find_eth_variant() -> CategoryVariant:
    for config in build_eth_refine_variants():
        if config["label"] == "eth_seed_m1":
            return config["variant"]
    raise ValueError("eth_seed_m1 ETH cross-source variant is not available")


def resolve_effective_start_at(
    *,
    requested_start_at: datetime,
    candles_by_source: dict[str, dict[str, list[BacktestCandle]]],
) -> datetime:
    first_candle_times: list[datetime] = []
    for source_candles in candles_by_source.values():
        for candles in source_candles.values():
            if not candles:
                raise ValueError("Missing candles for one of the portfolio sleeves")
            first_candle_times.append(candles[0].open_time)
    if not first_candle_times:
        raise ValueError("No candles loaded for portfolio benchmark")
    first_common_at = max(first_candle_times)
    return max(requested_start_at, add_months(first_common_at, LOOKBACK_MONTHS))


def scenario_costs(
    *,
    current_fee: Decimal,
    current_slippage: Decimal,
    gate_label: str,
) -> tuple[Decimal, Decimal]:
    gate_fee, gate_slippage = SCENARIO_COSTS[gate_label]
    return max(current_fee, gate_fee), max(current_slippage, gate_slippage)


def is_train_eligible(
    *,
    result: BacktestResponse,
    min_trades: int,
    min_profit_factor: Optional[float],
    require_positive_return: bool,
) -> bool:
    if int(result.metrics.total_trades) < min_trades:
        return False
    if min_profit_factor is not None and float(result.metrics.profit_factor) < min_profit_factor:
        return False
    if require_positive_return and float(result.metrics.total_return_pct) <= 0.0:
        return False
    return True


def pair_summary(
    *,
    label: str,
    base_by_source: dict[str, dict[str, Any]],
    stress_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    us = base_by_source["binance_us"]
    archive = base_by_source["binance_archive_spot"]
    min_return_pct = min(float(us["total_return_pct"]), float(archive["total_return_pct"]))
    avg_return_pct = (float(us["total_return_pct"]) + float(archive["total_return_pct"])) / 2.0
    gap_return_pct = abs(float(us["total_return_pct"]) - float(archive["total_return_pct"]))
    avg_drawdown_pct = (float(us["max_drawdown_pct"]) + float(archive["max_drawdown_pct"])) / 2.0
    min_profit_factor = min(float(us["profit_factor"]), float(archive["profit_factor"]))
    stress_floor_return_pct = min(
        min(
            float(report["sources"]["binance_us"]["total_return_pct"]),
            float(report["sources"]["binance_archive_spot"]["total_return_pct"]),
        )
        for report in stress_reports
    )
    robustness_score = round(
        stress_floor_return_pct
        + (min_return_pct * 0.50)
        - (gap_return_pct * 0.20)
        - (avg_drawdown_pct * 0.10),
        6,
    )
    return {
        "label": label,
        "summary": {
            "binance_us_return_pct": round(float(us["total_return_pct"]), 4),
            "binance_archive_return_pct": round(float(archive["total_return_pct"]), 4),
            "binance_us_max_drawdown_pct": round(float(us["max_drawdown_pct"]), 4),
            "binance_archive_max_drawdown_pct": round(float(archive["max_drawdown_pct"]), 4),
            "binance_us_profit_factor": round(float(us["profit_factor"]), 4),
            "binance_archive_profit_factor": round(float(archive["profit_factor"]), 4),
            "binance_us_total_trades": int(us["total_trades"]),
            "binance_archive_total_trades": int(archive["total_trades"]),
            "binance_us_eth_selected_periods": int(us["eth_selected_periods"]),
            "binance_archive_eth_selected_periods": int(archive["eth_selected_periods"]),
            "min_return_pct": round(min_return_pct, 4),
            "avg_return_pct": round(avg_return_pct, 4),
            "gap_return_pct": round(gap_return_pct, 4),
            "avg_drawdown_pct": round(avg_drawdown_pct, 4),
            "min_profit_factor": round(min_profit_factor, 4),
            "stress_floor_return_pct": round(stress_floor_return_pct, 4),
            "robustness_score": robustness_score,
        },
    }


def evaluate_portfolio_on_source(
    *,
    source: str,
    candles_by_symbol: dict[str, list[BacktestCandle]],
    config: PortfolioConfig,
    btc_variant: CategoryVariant,
    eth_variant: CategoryVariant,
    evaluation_start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    fee: Decimal,
    slippage: Decimal,
    response_cache: dict[tuple[str, str, str, str, str, str, str, str, str], BacktestResponse],
) -> dict[str, Any]:
    windows = generate_windows(evaluation_start_at=evaluation_start_at, end_at=end_at)
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
    btc_selected_periods = 0
    btc_rejected_periods = 0
    eth_selected_periods = 0
    eth_rejected_periods = 0
    no_active_sleeves_periods = 0
    periods: list[dict[str, Any]] = []
    equity_curve: list[EquityPoint] = [
        EquityPoint(
            timestamp=evaluation_start_at,
            equity=initial_capital,
            cash=initial_capital,
            close_price=initial_capital,
            position_qty=ZERO,
        )
    ]

    def get_result(
        *,
        symbol: str,
        variant: CategoryVariant,
        start_at: datetime,
        stop_at: datetime,
        run_fee: Decimal,
        run_slippage: Decimal,
        position_size_pct: Decimal,
        scope: str,
    ) -> BacktestResponse:
        cache_key = (
            source,
            symbol,
            variant.label,
            scope,
            start_at.isoformat(),
            stop_at.isoformat(),
            str(run_fee),
            str(run_slippage),
            str(position_size_pct),
        )
        cached = response_cache.get(cache_key)
        if cached is not None:
            return cached
        cached = run_variant_backtest(
            engine=BacktestEngine(),
            symbol=symbol,
            candles=candles_by_symbol[symbol],
            start_at=start_at,
            end_at=stop_at,
            fee=run_fee,
            slippage=run_slippage,
            position_size_pct=position_size_pct,
            variant=variant,
            strategy_code=(
                f"btc_eth_cross_source_portfolio:{config.label}:{variant.label}:{source}:{symbol}:{scope}"
            ),
        )
        response_cache[cache_key] = cached
        return cached

    for window in windows:
        period_details: dict[str, Any] = {
            "train_start_at": window["train_start_at"].isoformat(),
            "train_end_at": window["train_end_at"].isoformat(),
            "test_start_at": window["test_start_at"].isoformat(),
            "test_end_at": window["test_end_at"].isoformat(),
            "cash_mode": False,
            "reason": "active",
            "selected_sleeves": [],
            "btc_train": None,
            "eth_train": None,
        }
        if cooldown_remaining > 0:
            cash_periods += 1
            cooldown_remaining -= 1
            append_points(equity_curve, make_cash_curve(capital=capital, timestamp=window["test_end_at"]))
            prior_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
            period_details["cash_mode"] = True
            period_details["reason"] = "cooldown"
            period_details["ending_capital"] = round(float(capital), 4)
            period_details["drawdown_from_peak_pct"] = prior_drawdown_pct
            periods.append(period_details)
            continue

        active_sleeves: list[tuple[str, CategoryVariant]] = []
        btc_train_result = get_result(
            symbol=BTC_SYMBOL,
            variant=btc_variant,
            start_at=window["train_start_at"],
            stop_at=window["train_end_at"],
            run_fee=fee,
            run_slippage=slippage,
            position_size_pct=TRAIN_POSITION_SIZE_PCT,
            scope="train",
        )
        btc_allowed = int(btc_train_result.metrics.total_trades) >= 2
        period_details["btc_train"] = {
            "total_trades": int(btc_train_result.metrics.total_trades),
            "return_pct": round(float(btc_train_result.metrics.total_return_pct), 4),
            "profit_factor": round(float(btc_train_result.metrics.profit_factor), 4),
            "allowed": btc_allowed,
            "reason": "passed" if btc_allowed else "min_trades_not_met",
        }
        if btc_allowed:
            active_sleeves.append((BTC_SYMBOL, btc_variant))
            btc_selected_periods += 1
        else:
            btc_rejected_periods += 1

        eth_allowed = False
        eth_reject_reason = "not_included"
        eth_stress_train: Optional[BacktestResponse] = None
        if config.include_eth:
            eth_train_result = get_result(
                symbol=ETH_SYMBOL,
                variant=eth_variant,
                start_at=window["train_start_at"],
                stop_at=window["train_end_at"],
                run_fee=fee,
                run_slippage=slippage,
                position_size_pct=TRAIN_POSITION_SIZE_PCT,
                scope="train",
            )
            eth_allowed = True
            if int(eth_train_result.metrics.total_trades) < config.eth_min_train_trades:
                eth_allowed = False
                eth_reject_reason = "min_trades_not_met"
            elif (
                config.eth_min_profit_factor is not None
                and float(eth_train_result.metrics.profit_factor) < config.eth_min_profit_factor
            ):
                eth_allowed = False
                eth_reject_reason = "min_profit_factor_not_met"
            elif (
                config.eth_require_positive_train_return
                and float(eth_train_result.metrics.total_return_pct) <= 0.0
            ):
                eth_allowed = False
                eth_reject_reason = "non_positive_train_return"
            else:
                eth_reject_reason = "passed"
            if eth_allowed and config.eth_stress_gate is not None:
                stress_fee, stress_slippage = scenario_costs(
                    current_fee=fee,
                    current_slippage=slippage,
                    gate_label=config.eth_stress_gate,
                )
                eth_stress_train = get_result(
                    symbol=ETH_SYMBOL,
                    variant=eth_variant,
                    start_at=window["train_start_at"],
                    stop_at=window["train_end_at"],
                    run_fee=stress_fee,
                    run_slippage=stress_slippage,
                    position_size_pct=TRAIN_POSITION_SIZE_PCT,
                    scope=f"train_{config.eth_stress_gate}",
                )
                if float(eth_stress_train.metrics.total_return_pct) < config.eth_stress_min_return_pct:
                    eth_allowed = False
                    eth_reject_reason = f"{config.eth_stress_gate}_return_below_floor"
                if (
                    eth_allowed
                    and config.eth_stress_min_profit_factor is not None
                    and float(eth_stress_train.metrics.profit_factor) < config.eth_stress_min_profit_factor
                ):
                    eth_allowed = False
                    eth_reject_reason = f"{config.eth_stress_gate}_profit_factor_below_floor"
            period_details["eth_train"] = {
                "total_trades": int(eth_train_result.metrics.total_trades),
                "return_pct": round(float(eth_train_result.metrics.total_return_pct), 4),
                "profit_factor": round(float(eth_train_result.metrics.profit_factor), 4),
                "allowed": eth_allowed,
                "reason": eth_reject_reason,
                "stress_gate": config.eth_stress_gate,
                "stress_return_pct": (
                    round(float(eth_stress_train.metrics.total_return_pct), 4)
                    if eth_stress_train is not None
                    else None
                ),
                "stress_profit_factor": (
                    round(float(eth_stress_train.metrics.profit_factor), 4)
                    if eth_stress_train is not None
                    else None
                ),
            }
            if eth_allowed:
                active_sleeves.append((ETH_SYMBOL, eth_variant))
                eth_selected_periods += 1
            else:
                eth_rejected_periods += 1

        if not active_sleeves:
            cash_periods += 1
            no_active_sleeves_periods += 1
            append_points(equity_curve, make_cash_curve(capital=capital, timestamp=window["test_end_at"]))
            prior_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
            period_details["cash_mode"] = True
            period_details["reason"] = "no_active_sleeves"
            period_details["ending_capital"] = round(float(capital), 4)
            period_details["drawdown_from_peak_pct"] = prior_drawdown_pct
            periods.append(period_details)
            continue

        period_details["selected_sleeves"] = [symbol for symbol, _ in active_sleeves]
        allocated_capital = capital / Decimal(len(active_sleeves))
        scaled_results: list[tuple[BacktestResponse, Decimal, Decimal]] = []
        final_capital = ZERO
        slice_total_trades = 0
        slice_winning_trades = 0
        slice_gross_profit = ZERO
        slice_gross_loss = ZERO

        for symbol, variant in active_sleeves:
            response = get_result(
                symbol=symbol,
                variant=variant,
                start_at=window["test_start_at"],
                stop_at=window["test_end_at"],
                run_fee=fee,
                run_slippage=slippage,
                position_size_pct=TEST_POSITION_SIZE_PCT,
                scope="test",
            )
            scale_factor = allocated_capital / BASE_SYMBOL_CAPITAL
            scaled_results.append((response, scale_factor, allocated_capital))
            final_capital += response.final_equity * scale_factor
            trades, wins, profit, loss = scaled_trade_stats(response=response, scale_factor=scale_factor)
            slice_total_trades += trades
            slice_winning_trades += wins
            slice_gross_profit += profit
            slice_gross_loss += loss

        slice_curve = build_slice_curve(
            scaled_results=scaled_results,
            initial_capital=capital,
            test_end_at=window["test_end_at"],
        )
        append_points(equity_curve, slice_curve)
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
        if current_drawdown_pct >= DRAWDOWN_GUARD_PCT and prior_drawdown_pct < DRAWDOWN_GUARD_PCT:
            cooldown_remaining = COOLDOWN_PERIODS
            guard_triggers += 1
        prior_drawdown_pct = current_drawdown_pct
        period_details["ending_capital"] = round(float(capital), 4)
        period_details["drawdown_from_peak_pct"] = current_drawdown_pct
        period_details["period_return_pct"] = slice_summary["total_return_pct"]
        period_details["period_max_drawdown_pct"] = slice_summary["max_drawdown_pct"]
        period_details["period_total_trades"] = slice_summary["total_trades"]
        period_details["period_profit_factor"] = slice_summary["profit_factor"]
        periods.append(period_details)

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
    summary["btc_selected_periods"] = btc_selected_periods
    summary["btc_rejected_periods"] = btc_rejected_periods
    summary["eth_selected_periods"] = eth_selected_periods
    summary["eth_rejected_periods"] = eth_rejected_periods
    summary["no_active_sleeves_periods"] = no_active_sleeves_periods
    summary["periods"] = periods
    return summary


def scenario_report(
    *,
    scenario_label: str,
    config: PortfolioConfig,
    candles_by_source: dict[str, dict[str, list[BacktestCandle]]],
    btc_variant: CategoryVariant,
    eth_variant: CategoryVariant,
    evaluation_start_at: datetime,
    end_at: datetime,
    initial_capital: Decimal,
    response_cache: dict[tuple[str, str, str, str, str, str, str, str, str], BacktestResponse],
) -> dict[str, Any]:
    fee, slippage = SCENARIO_COSTS[scenario_label]
    by_source: dict[str, dict[str, Any]] = {}
    for source, source_candles in candles_by_source.items():
        by_source[source] = evaluate_portfolio_on_source(
            source=source,
            candles_by_symbol=source_candles,
            config=config,
            btc_variant=btc_variant,
            eth_variant=eth_variant,
            evaluation_start_at=evaluation_start_at,
            end_at=end_at,
            initial_capital=initial_capital,
            fee=fee,
            slippage=slippage,
            response_cache=response_cache,
        )
    return {
        "label": scenario_label,
        "fee": float(fee),
        "slippage": float(slippage),
        "sources": by_source,
    }


def build_delta(
    *,
    base_report: dict[str, Any],
    baseline_report: dict[str, Any],
) -> dict[str, Any]:
    fields = (
        "min_return_pct",
        "avg_return_pct",
        "gap_return_pct",
        "stress_floor_return_pct",
        "robustness_score",
    )
    return {
        field: round(
            float(base_report["summary"][field]) - float(baseline_report["summary"][field]),
            4,
        )
        for field in fields
    }


def main() -> None:
    args = parse_args()
    requested_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    requested_labels = parse_config_labels(args.config_labels)

    configs = build_configs()
    if requested_labels:
        configs = tuple(config for config in configs if config.label in requested_labels)
        if not configs:
            raise ValueError("No portfolio configs matched --config-labels")

    fetch_start_at = add_months(requested_start_at, -LOOKBACK_MONTHS) - timedelta(days=args.warmup_days)
    candles_by_source: dict[str, dict[str, list[BacktestCandle]]] = {
        "binance_us": {},
        "binance_archive_spot": {},
    }
    for symbol in (BTC_SYMBOL, ETH_SYMBOL):
        print(f"Fetching cross-source candles for {symbol}...")
        source_candles = fetch_source_candles(
            symbol=symbol,
            fetch_start_at=fetch_start_at,
            end_at=end_at,
        )
        for source, candles in source_candles.items():
            candles_by_source[source][symbol] = candles
            print(f"Loaded {len(candles)} candles for {symbol} on {source}")

    effective_start_at = resolve_effective_start_at(
        requested_start_at=requested_start_at,
        candles_by_source=candles_by_source,
    )
    btc_variant = find_btc_variant()
    eth_variant = find_eth_variant()
    response_cache: dict[tuple[str, str, str, str, str, str, str, str, str], BacktestResponse] = {}

    reports: list[dict[str, Any]] = []
    for config in configs:
        print(f"Evaluating portfolio config {config.label}...")
        stress_reports = [
            scenario_report(
                scenario_label=scenario_label,
                config=config,
                candles_by_source=candles_by_source,
                btc_variant=btc_variant,
                eth_variant=eth_variant,
                evaluation_start_at=effective_start_at,
                end_at=end_at,
                initial_capital=initial_capital,
                response_cache=response_cache,
            )
            for scenario_label in ("base", "moderate_costs", "heavy_costs")
        ]
        base_report = pair_summary(
            label=config.label,
            base_by_source=stress_reports[0]["sources"],
            stress_reports=stress_reports,
        )
        reports.append(
            {
                "config": config.as_dict(),
                "base_report": base_report,
                "stress_reports": stress_reports,
            }
        )

    reports.sort(
        key=lambda item: (
            item["base_report"]["summary"]["robustness_score"],
            item["base_report"]["summary"]["stress_floor_return_pct"],
            item["base_report"]["summary"]["min_return_pct"],
        ),
        reverse=True,
    )

    baseline = next((report for report in reports if report["config"]["label"] == "btc_only"), None)
    if baseline is not None:
        for report in reports:
            report["vs_btc_only"] = build_delta(
                base_report=report["base_report"],
                baseline_report=baseline["base_report"],
            )

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "window": {
            "fetch_start_at": fetch_start_at.isoformat(),
            "requested_evaluation_start_at": requested_start_at.isoformat(),
            "effective_evaluation_start_at": effective_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "sleeves": {
            "btc": {
                "symbol": BTC_SYMBOL,
                "variant_label": btc_variant.label,
                "min_train_trades": 2,
            },
            "eth": {
                "symbol": ETH_SYMBOL,
                "variant_label": eth_variant.label,
                "min_train_trades": 1,
            },
        },
        "reports": reports,
        "winner": reports[0] if reports else None,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote BTC/ETH cross-source portfolio report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
