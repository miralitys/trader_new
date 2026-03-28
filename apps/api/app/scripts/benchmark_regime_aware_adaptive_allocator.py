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
from app.scripts.benchmark_regime_aware_symbol_selection import evaluate_symbol_reports
from app.scripts.benchmark_strategy_categories import (
    CategoryVariant,
    fetch_candles,
    parse_datetime,
    parse_symbols,
    run_backtest,
)

UTC = timezone.utc
ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
BASE_SYMBOL_CAPITAL = Decimal("10000")


@dataclass(frozen=True)
class AdaptiveAllocatorConfig:
    label: str
    lookback_months: int
    rebalance_mode: str
    max_symbols: Optional[int]
    min_trades: int
    require_positive_return: bool = False
    min_profit_factor: Optional[float] = None
    allowed_symbols: tuple[str, ...] = tuple()
    variant_policy: str = "any"
    position_size_pct: Decimal = Decimal("0.10")
    drawdown_guard_pct: Optional[float] = None
    cooldown_periods: int = 0
    forced_variant_label: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "lookback_months": self.lookback_months,
            "rebalance_mode": self.rebalance_mode,
            "max_symbols": self.max_symbols,
            "min_trades": self.min_trades,
            "require_positive_return": self.require_positive_return,
            "min_profit_factor": self.min_profit_factor,
            "allowed_symbols": list(self.allowed_symbols),
            "variant_policy": self.variant_policy,
            "position_size_pct": float(self.position_size_pct),
            "drawdown_guard_pct": self.drawdown_guard_pct,
            "cooldown_periods": self.cooldown_periods,
            "forced_variant_label": self.forced_variant_label,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Walk-forward benchmark for a regime-aware adaptive symbol allocator."
    )
    parser.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,ADA-USDT,DOGE-USDT,LTC-USDT")
    parser.add_argument("--evaluation-start-at", default="2025-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--config-labels")
    parser.add_argument("--output")
    return parser.parse_args()


def build_allocator_configs() -> tuple[AdaptiveAllocatorConfig, ...]:
    configs: list[AdaptiveAllocatorConfig] = []
    for rebalance_mode in ("monthly", "biweekly"):
        for lookback_months in (2, 3, 4):
            prefix = f"{rebalance_mode}_lb{lookback_months}m"
            configs.extend(
                (
                    AdaptiveAllocatorConfig(
                        label=f"{prefix}_top1_t1",
                        lookback_months=lookback_months,
                        rebalance_mode=rebalance_mode,
                        max_symbols=1,
                        min_trades=1,
                    ),
                    AdaptiveAllocatorConfig(
                        label=f"{prefix}_top2_t1",
                        lookback_months=lookback_months,
                        rebalance_mode=rebalance_mode,
                        max_symbols=2,
                        min_trades=1,
                    ),
                    AdaptiveAllocatorConfig(
                        label=f"{prefix}_positive_t1",
                        lookback_months=lookback_months,
                        rebalance_mode=rebalance_mode,
                        max_symbols=None,
                        min_trades=1,
                        require_positive_return=True,
                    ),
                    AdaptiveAllocatorConfig(
                        label=f"{prefix}_positive_t2",
                        lookback_months=lookback_months,
                        rebalance_mode=rebalance_mode,
                        max_symbols=None,
                        min_trades=2,
                        require_positive_return=True,
                    ),
                    AdaptiveAllocatorConfig(
                        label=f"{prefix}_pf1_t1",
                        lookback_months=lookback_months,
                        rebalance_mode=rebalance_mode,
                        max_symbols=None,
                        min_trades=1,
                        min_profit_factor=1.0,
                    ),
                    AdaptiveAllocatorConfig(
                        label=f"{prefix}_pf1_t2",
                        lookback_months=lookback_months,
                        rebalance_mode=rebalance_mode,
                        max_symbols=None,
                        min_trades=2,
                        min_profit_factor=1.0,
                    ),
                )
            )
    configs.append(
        AdaptiveAllocatorConfig(
            label="btc_flat_priority_sleeve",
            lookback_months=2,
            rebalance_mode="monthly",
            max_symbols=1,
            min_trades=2,
            allowed_symbols=("BTC-USDT",),
            variant_policy="flat_only",
        )
    )
    configs.append(
        AdaptiveAllocatorConfig(
            label="btc_flat_priority_sleeve_ps100_dd4_cd3",
            lookback_months=2,
            rebalance_mode="monthly",
            max_symbols=1,
            min_trades=2,
            allowed_symbols=("BTC-USDT",),
            variant_policy="flat_only",
            position_size_pct=Decimal("1.00"),
            drawdown_guard_pct=4.0,
            cooldown_periods=3,
        )
    )
    configs.append(
        AdaptiveAllocatorConfig(
            label="btc_flat_priority_xsrc_ps100_dd4_cd3",
            lookback_months=2,
            rebalance_mode="monthly",
            max_symbols=1,
            min_trades=2,
            allowed_symbols=("BTC-USDT",),
            variant_policy="flat_only",
            position_size_pct=Decimal("1.00"),
            drawdown_guard_pct=4.0,
            cooldown_periods=3,
            forced_variant_label="flat_priority_cross_source",
        )
    )
    return tuple(configs)


def parse_config_labels(value: Optional[str]) -> set[str]:
    if value is None:
        return set()
    return {item.strip() for item in value.split(",") if item.strip()}


def add_months(value: datetime, months: int) -> datetime:
    year = value.year + ((value.month - 1 + months) // 12)
    month = ((value.month - 1 + months) % 12) + 1
    month_lengths = [
        31,
        29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ]
    day = min(value.day, month_lengths[month - 1])
    return value.replace(year=year, month=month, day=day)


def next_rebalance_at(start_at: datetime, rebalance_mode: str) -> datetime:
    if rebalance_mode == "monthly":
        return add_months(start_at, 1)
    if rebalance_mode == "biweekly":
        return start_at + timedelta(days=14)
    raise ValueError(f"Unsupported rebalance_mode: {rebalance_mode}")


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


def score_summary(summary: dict[str, Any]) -> float:
    return round(
        float(summary["total_return_pct"]) - (float(summary["max_drawdown_pct"]) * 0.15),
        6,
    )


def drawdown_from_peak_pct(current_equity: Decimal, peak_equity: Decimal) -> float:
    if peak_equity <= ZERO:
        return 0.0
    return round(float(((peak_equity - current_equity) / peak_equity) * HUNDRED), 4)


def variant_allowed(label: str, variant_policy: str) -> bool:
    if variant_policy == "any":
        return True
    if variant_policy == "no_balanced":
        return label != "balanced"
    if variant_policy == "flat_only":
        return label == "flat_priority"
    raise ValueError(f"Unsupported variant_policy: {variant_policy}")


def max_drawdown_pct(curve: Sequence[EquityPoint]) -> float:
    if not curve:
        return 0.0
    peak = float(curve[0].equity)
    max_drawdown = 0.0
    for point in curve:
        equity = float(point.equity)
        if equity > peak:
            peak = equity
        if peak <= 0:
            continue
        drawdown = ((peak - equity) / peak) * 100.0
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return round(max_drawdown, 4)


def append_points(curve: list[EquityPoint], points: Sequence[EquityPoint]) -> None:
    for point in points:
        if curve and curve[-1].timestamp == point.timestamp:
            curve[-1] = point
        else:
            curve.append(point)


def build_slice_curve(
    scaled_results: Sequence[tuple[BacktestResponse, Decimal, Decimal]],
    initial_capital: Decimal,
    test_end_at: datetime,
) -> list[EquityPoint]:
    if not scaled_results:
        return [
            EquityPoint(
                timestamp=test_end_at,
                equity=initial_capital,
                cash=initial_capital,
                close_price=initial_capital,
                position_qty=ZERO,
            )
        ]

    timestamps = sorted(
        {
            point.timestamp
            for response, _, _ in scaled_results
            for point in response.equity_curve
        }
    )
    if not timestamps:
        return [
            EquityPoint(
                timestamp=test_end_at,
                equity=initial_capital,
                cash=initial_capital,
                close_price=initial_capital,
                position_qty=ZERO,
            )
        ]

    current_by_symbol: list[tuple[Decimal, Decimal]] = [
        (allocated_capital, allocated_capital)
        for _, _, allocated_capital in scaled_results
    ]
    point_maps: list[dict[datetime, tuple[Decimal, Decimal]]] = []
    for response, factor, _ in scaled_results:
        scaled_points: dict[datetime, tuple[Decimal, Decimal]] = {}
        for point in response.equity_curve:
            scaled_points[point.timestamp] = (
                point.equity * factor,
                point.cash * factor,
            )
        point_maps.append(scaled_points)

    combined: list[EquityPoint] = []
    for timestamp in timestamps:
        total_equity = ZERO
        total_cash = ZERO
        for index, point_map in enumerate(point_maps):
            latest = point_map.get(timestamp)
            if latest is not None:
                current_by_symbol[index] = latest
            equity_value, cash_value = current_by_symbol[index]
            total_equity += equity_value
            total_cash += cash_value
        combined.append(
            EquityPoint(
                timestamp=timestamp,
                equity=total_equity,
                cash=total_cash,
                close_price=total_equity,
                position_qty=ZERO,
            )
        )
    return combined


def summarize_portfolio(
    *,
    initial_capital: Decimal,
    final_capital: Decimal,
    equity_curve: Sequence[EquityPoint],
    total_trades: int,
    winning_trades: int,
    gross_profit: Decimal,
    gross_loss: Decimal,
) -> dict[str, Any]:
    total_return_pct = ZERO
    if initial_capital > ZERO:
        total_return_pct = ((final_capital - initial_capital) / initial_capital) * HUNDRED

    win_rate_pct = ZERO
    if total_trades > 0:
        win_rate_pct = (Decimal(winning_trades) / Decimal(total_trades)) * HUNDRED

    if gross_loss > ZERO:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > ZERO:
        profit_factor = gross_profit
    else:
        profit_factor = ZERO

    return {
        "starting_capital": round(float(initial_capital), 4),
        "ending_capital": round(float(final_capital), 4),
        "pnl": round(float(final_capital - initial_capital), 4),
        "total_return_pct": round(float(total_return_pct), 4),
        "max_drawdown_pct": max_drawdown_pct(equity_curve),
        "total_trades": total_trades,
        "win_rate_pct": round(float(win_rate_pct), 4),
        "profit_factor": round(float(profit_factor), 4),
    }


def select_symbols(
    train_symbol_reports: Sequence[dict[str, Any]],
    config: AdaptiveAllocatorConfig,
) -> list[str]:
    eligible = [
        report
        for report in train_symbol_reports
        if int(report["total_trades"]) >= config.min_trades
    ]
    if config.allowed_symbols:
        allowed = set(config.allowed_symbols)
        eligible = [report for report in eligible if str(report["symbol"]) in allowed]
    if config.require_positive_return:
        eligible = [report for report in eligible if float(report["return_pct"]) > 0.0]
    if config.min_profit_factor is not None:
        eligible = [
            report
            for report in eligible
            if float(report["profit_factor"]) > config.min_profit_factor
        ]
    if config.max_symbols is not None:
        eligible = eligible[: config.max_symbols]
    return [str(report["symbol"]) for report in eligible]


def main() -> None:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    portfolio_initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))

    allocator_configs = build_allocator_configs()
    requested_config_labels = parse_config_labels(args.config_labels)
    if requested_config_labels:
        allocator_configs = tuple(
            config for config in allocator_configs if config.label in requested_config_labels
        )
        if not allocator_configs:
            raise ValueError("No allocator configs matched --config-labels")
    max_lookback_months = max(config.lookback_months for config in allocator_configs)
    fetch_start_at = add_months(evaluation_start_at, -max_lookback_months) - timedelta(days=args.warmup_days)

    client = BinanceUSClient()
    try:
        candle_cache: dict[str, list[BacktestCandle]] = {}
        for symbol in symbols:
            print(f"Fetching {symbol} 4h candles from {fetch_start_at.isoformat()} to {end_at.isoformat()}...")
            candle_cache[symbol] = fetch_candles(
                client=client,
                symbol=symbol,
                timeframe="4h",
                start_at=fetch_start_at,
                end_at=end_at,
            )
            print(f"Loaded {len(candle_cache[symbol])} candles for {symbol} 4h")
    finally:
        client.close()

    base_engine = BacktestEngine()
    variant_cache: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    selected_variant_cache: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    train_symbol_report_cache: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    symbol_test_cache: dict[tuple[str, str, str, str, str], BacktestResponse] = {}

    def symbol_scope_key(candidate_symbols: Sequence[str]) -> str:
        return ",".join(candidate_symbols)

    def symbols_available_until(candidate_symbols: Sequence[str], end_at: datetime) -> list[str]:
        available: list[str] = []
        for symbol in candidate_symbols:
            symbol_candles = candle_cache.get(symbol, [])
            if symbol_candles and symbol_candles[0].open_time <= end_at:
                available.append(symbol)
        return available

    def get_selected_variant(
        train_start_at: datetime,
        train_end_at: datetime,
        candidate_symbols: Sequence[str],
        variant_policy: str,
        forced_variant_label: Optional[str],
    ) -> dict[str, Any]:
        key = (
            train_start_at.isoformat(),
            train_end_at.isoformat(),
            symbol_scope_key(candidate_symbols),
            variant_policy,
            forced_variant_label or "",
        )
        cached = selected_variant_cache.get(key)
        if cached is not None:
            return cached

        train_reports: list[dict[str, Any]] = []
        for variant in build_regime_variants():
            if forced_variant_label is not None and variant.label != forced_variant_label:
                continue
            variant_key = (
                train_start_at.isoformat(),
                train_end_at.isoformat(),
                symbol_scope_key(candidate_symbols),
                variant.label,
            )
            report = variant_cache.get(variant_key)
            if report is None:
                report = evaluate_variant(
                    strategy_factory=RegimeAwareStrategy,
                    strategy_code_prefix="regime_aware_adaptive_allocator",
                    variant=variant,
                    symbols=list(candidate_symbols),
                    candle_cache=candle_cache,
                    start_at=train_start_at,
                    end_at=train_end_at,
                    initial_capital=BASE_SYMBOL_CAPITAL,
                    fee=fee,
                    slippage=slippage,
                )
                variant_cache[variant_key] = report
            if forced_variant_label is not None or variant_allowed(str(report["label"]), variant_policy):
                train_reports.append(report)

        if not train_reports:
            raise ValueError("No regime variants available after variant_policy filtering")

        best_report = max(
            train_reports,
            key=lambda item: (item["score"], item["metrics"]["portfolio_return_pct"]),
        )
        payload = {
            "variant": CategoryVariant(
                label=str(best_report["label"]),
                overrides=dict(best_report["overrides"]),
            ),
            "train_report": best_report,
        }
        selected_variant_cache[key] = payload
        return payload

    def get_train_symbol_reports(
        train_start_at: datetime,
        train_end_at: datetime,
        variant: CategoryVariant,
        candidate_symbols: Sequence[str],
    ) -> list[dict[str, Any]]:
        cache_key = (
            train_start_at.isoformat(),
            train_end_at.isoformat(),
            variant.label,
            symbol_scope_key(candidate_symbols),
        )
        cached = train_symbol_report_cache.get(cache_key)
        if cached is not None:
            return cached

        reports = evaluate_symbol_reports(
            variant=variant,
            symbols=list(candidate_symbols),
            candle_cache=candle_cache,
            start_at=train_start_at,
            end_at=train_end_at,
            initial_capital=BASE_SYMBOL_CAPITAL,
            fee=fee,
            slippage=slippage,
        )
        train_symbol_report_cache[cache_key] = reports
        return reports

    def get_symbol_test_result(
        test_start_at: datetime,
        test_end_at: datetime,
        variant: CategoryVariant,
        symbol: str,
        position_size_pct: Decimal,
    ) -> BacktestResponse:
        cache_key = (
            test_start_at.isoformat(),
            test_end_at.isoformat(),
            variant.label,
            symbol,
            str(position_size_pct),
        )
        cached = symbol_test_cache.get(cache_key)
        if cached is not None:
            return cached

        result = run_backtest(
            engine=base_engine,
            strategy=RegimeAwareStrategy(),
            strategy_code=f"adaptive_allocator:{variant.label}:{symbol}",
            symbol=symbol,
            timeframe="4h",
            start_at=test_start_at,
            end_at=test_end_at,
            candles=candle_cache[symbol],
            initial_capital=BASE_SYMBOL_CAPITAL,
            fee=fee,
            slippage=slippage,
            overrides=variant.overrides,
            position_size_pct=position_size_pct,
        )
        symbol_test_cache[cache_key] = result
        return result

    config_reports: list[dict[str, Any]] = []
    for config in allocator_configs:
        print(f"Evaluating adaptive allocator {config.label}...")
        windows = generate_windows(
            evaluation_start_at=evaluation_start_at,
            end_at=end_at,
            lookback_months=config.lookback_months,
            rebalance_mode=config.rebalance_mode,
        )
        portfolio_capital = portfolio_initial_capital
        equity_curve: list[EquityPoint] = [
            EquityPoint(
                timestamp=evaluation_start_at,
                equity=portfolio_initial_capital,
                cash=portfolio_initial_capital,
                close_price=portfolio_initial_capital,
                position_qty=ZERO,
            )
        ]
        total_trades = 0
        winning_trades = 0
        gross_profit = ZERO
        gross_loss = ZERO
        positive_periods = 0
        cash_periods = 0
        guard_triggers = 0
        peak_equity = portfolio_initial_capital
        prior_drawdown_pct = 0.0
        cooldown_remaining = 0
        periods: list[dict[str, Any]] = []

        for window in windows:
            if cooldown_remaining > 0:
                cash_periods += 1
                cooldown_remaining -= 1
                slice_curve = build_slice_curve(
                    scaled_results=(),
                    initial_capital=portfolio_capital,
                    test_end_at=window["test_end_at"],
                )
                append_points(equity_curve, slice_curve)
                current_drawdown_pct = drawdown_from_peak_pct(
                    current_equity=portfolio_capital,
                    peak_equity=peak_equity,
                )
                prior_drawdown_pct = current_drawdown_pct
                periods.append(
                    {
                        "train_start_at": window["train_start_at"].isoformat(),
                        "train_end_at": window["train_end_at"].isoformat(),
                        "test_start_at": window["test_start_at"].isoformat(),
                        "test_end_at": window["test_end_at"].isoformat(),
                        "selected_variant": None,
                        "selected_symbols": [],
                        "cash_mode": True,
                        "reason": "cooldown",
                        "period_return_pct": 0.0,
                        "max_drawdown_pct": 0.0,
                        "total_trades": 0,
                        "profit_factor": 0.0,
                        "ending_capital": round(float(portfolio_capital), 4),
                        "peak_equity": round(float(peak_equity), 4),
                        "drawdown_from_peak_pct": current_drawdown_pct,
                        "guard_triggered": False,
                        "cooldown_remaining_after": cooldown_remaining,
                    }
                )
                continue
            candidate_symbols = (
                [symbol for symbol in config.allowed_symbols if symbol in candle_cache]
                if config.allowed_symbols
                else list(symbols)
            )
            candidate_symbols = symbols_available_until(
                candidate_symbols=candidate_symbols,
                end_at=window["train_end_at"],
            )
            if not candidate_symbols:
                cash_periods += 1
                slice_curve = build_slice_curve(
                    scaled_results=(),
                    initial_capital=portfolio_capital,
                    test_end_at=window["test_end_at"],
                )
                append_points(equity_curve, slice_curve)
                periods.append(
                    {
                        "train_start_at": window["train_start_at"].isoformat(),
                        "train_end_at": window["train_end_at"].isoformat(),
                        "test_start_at": window["test_start_at"].isoformat(),
                        "test_end_at": window["test_end_at"].isoformat(),
                        "selected_variant": None,
                        "selected_symbols": [],
                        "cash_mode": True,
                        "reason": "no_candidate_symbols",
                        "period_return_pct": 0.0,
                        "max_drawdown_pct": 0.0,
                        "total_trades": 0,
                        "profit_factor": 0.0,
                        "ending_capital": round(float(portfolio_capital), 4),
                        "peak_equity": round(float(peak_equity), 4),
                        "drawdown_from_peak_pct": drawdown_from_peak_pct(
                            current_equity=portfolio_capital,
                            peak_equity=peak_equity,
                        ),
                        "guard_triggered": False,
                        "cooldown_remaining_after": cooldown_remaining,
                    }
                )
                continue
            selected_variant_payload = get_selected_variant(
                train_start_at=window["train_start_at"],
                train_end_at=window["train_end_at"],
                candidate_symbols=candidate_symbols,
                variant_policy=config.variant_policy,
                forced_variant_label=config.forced_variant_label,
            )
            selected_variant = selected_variant_payload["variant"]
            train_symbol_reports = get_train_symbol_reports(
                train_start_at=window["train_start_at"],
                train_end_at=window["train_end_at"],
                variant=selected_variant,
                candidate_symbols=candidate_symbols,
            )
            selected_symbols = select_symbols(train_symbol_reports=train_symbol_reports, config=config)

            if not selected_symbols:
                cash_periods += 1
                slice_curve = build_slice_curve(
                    scaled_results=(),
                    initial_capital=portfolio_capital,
                    test_end_at=window["test_end_at"],
                )
                append_points(equity_curve, slice_curve)
                periods.append(
                    {
                        "train_start_at": window["train_start_at"].isoformat(),
                        "train_end_at": window["train_end_at"].isoformat(),
                        "test_start_at": window["test_start_at"].isoformat(),
                        "test_end_at": window["test_end_at"].isoformat(),
                        "selected_variant": selected_variant.label,
                        "selected_symbols": [],
                        "cash_mode": True,
                        "reason": "no_selected_symbols",
                        "period_return_pct": 0.0,
                        "max_drawdown_pct": 0.0,
                        "total_trades": 0,
                        "profit_factor": 0.0,
                        "ending_capital": round(float(portfolio_capital), 4),
                        "peak_equity": round(float(peak_equity), 4),
                        "drawdown_from_peak_pct": drawdown_from_peak_pct(
                            current_equity=portfolio_capital,
                            peak_equity=peak_equity,
                        ),
                        "guard_triggered": False,
                        "cooldown_remaining_after": cooldown_remaining,
                    }
                )
                continue

            allocation_per_symbol = portfolio_capital / Decimal(len(selected_symbols))
            scaled_results: list[tuple[BacktestResponse, Decimal, Decimal]] = []
            slice_total_trades = 0
            slice_winning_trades = 0
            slice_gross_profit = ZERO
            slice_gross_loss = ZERO
            slice_final_capital = ZERO
            for symbol in selected_symbols:
                base_result = get_symbol_test_result(
                    test_start_at=window["test_start_at"],
                    test_end_at=window["test_end_at"],
                    variant=selected_variant,
                    symbol=symbol,
                    position_size_pct=config.position_size_pct,
                )
                scale_factor = allocation_per_symbol / BASE_SYMBOL_CAPITAL
                scaled_results.append((base_result, scale_factor, allocation_per_symbol))
                slice_final_capital += base_result.final_equity * scale_factor
                slice_total_trades += base_result.metrics.total_trades
                for trade in base_result.trades:
                    pnl = trade.pnl * scale_factor
                    if pnl > ZERO:
                        slice_winning_trades += 1
                        slice_gross_profit += pnl
                    elif pnl < ZERO:
                        slice_gross_loss += abs(pnl)

            slice_curve = build_slice_curve(
                scaled_results=scaled_results,
                initial_capital=portfolio_capital,
                test_end_at=window["test_end_at"],
            )
            append_points(equity_curve, slice_curve)
            slice_summary = summarize_portfolio(
                initial_capital=portfolio_capital,
                final_capital=slice_final_capital,
                equity_curve=[
                    EquityPoint(
                        timestamp=window["test_start_at"],
                        equity=portfolio_capital,
                        cash=portfolio_capital,
                        close_price=portfolio_capital,
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
            portfolio_capital = slice_final_capital
            if portfolio_capital > peak_equity:
                peak_equity = portfolio_capital
            current_drawdown_pct = drawdown_from_peak_pct(
                current_equity=portfolio_capital,
                peak_equity=peak_equity,
            )
            guard_triggered = False
            if (
                config.drawdown_guard_pct is not None
                and current_drawdown_pct >= config.drawdown_guard_pct
                and prior_drawdown_pct < config.drawdown_guard_pct
            ):
                cooldown_remaining = config.cooldown_periods
                guard_triggers += 1
                guard_triggered = True
            prior_drawdown_pct = current_drawdown_pct
            periods.append(
                {
                    "train_start_at": window["train_start_at"].isoformat(),
                    "train_end_at": window["train_end_at"].isoformat(),
                    "test_start_at": window["test_start_at"].isoformat(),
                    "test_end_at": window["test_end_at"].isoformat(),
                    "selected_variant": selected_variant.label,
                    "selected_symbols": selected_symbols,
                    "cash_mode": False,
                    "reason": "active",
                    **slice_summary,
                    "peak_equity": round(float(peak_equity), 4),
                    "drawdown_from_peak_pct": current_drawdown_pct,
                    "guard_triggered": guard_triggered,
                    "cooldown_remaining_after": cooldown_remaining,
                }
            )

        summary = summarize_portfolio(
            initial_capital=portfolio_initial_capital,
            final_capital=portfolio_capital,
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
        summary["score"] = score_summary(summary)
        config_reports.append(
            {
                "config": config.as_dict(),
                "summary": summary,
                "periods": periods,
            }
        )

    config_reports.sort(
        key=lambda item: (
            item["summary"]["total_return_pct"],
            item["summary"]["score"],
            -item["summary"]["max_drawdown_pct"],
        ),
        reverse=True,
    )
    winner = config_reports[0]
    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "symbols": symbols,
        "window": {
            "fetch_start_at": fetch_start_at.isoformat(),
            "evaluation_start_at": evaluation_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "reports": config_reports,
        "winner": winner,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote adaptive allocator report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
