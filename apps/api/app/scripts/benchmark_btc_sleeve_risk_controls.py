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
from app.schemas.backtest import BacktestCandle, BacktestRequest, BacktestResponse, EquityPoint
from app.scripts.benchmark_regime_aware import build_regime_variants
from app.scripts.benchmark_regime_aware_adaptive_allocator import (
    BASE_SYMBOL_CAPITAL,
    add_months,
    append_points,
    build_slice_curve,
    next_rebalance_at,
    summarize_portfolio,
)
from app.scripts.benchmark_strategy_categories import CategoryVariant, fetch_candles, parse_datetime

UTC = timezone.utc
ZERO = Decimal("0")


@dataclass(frozen=True)
class BtcSleeveRiskConfig:
    label: str
    position_size_pct: Decimal
    drawdown_guard_pct: Optional[float]
    cooldown_periods: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "position_size_pct": float(self.position_size_pct),
            "drawdown_guard_pct": self.drawdown_guard_pct,
            "cooldown_periods": self.cooldown_periods,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep BTC flat-priority sleeve risk controls over longer walk-forward history."
    )
    parser.add_argument("--symbol", default="BTC-USDT")
    parser.add_argument("--evaluation-start-at", default="2023-03-15T00:00:00+00:00")
    parser.add_argument("--end-at", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--warmup-days", type=int, default=60)
    parser.add_argument("--initial-capital", default="10000")
    parser.add_argument("--fee", default="0.001")
    parser.add_argument("--slippage", default="0.0005")
    parser.add_argument("--config-labels")
    parser.add_argument("--output")
    return parser.parse_args()


def size_label(value: Decimal) -> str:
    return f"{int(value * Decimal('100'))}"


def guard_label(value: float) -> str:
    text = f"{value:.1f}".rstrip("0").rstrip(".")
    return text.replace(".", "_")


def build_configs() -> tuple[BtcSleeveRiskConfig, ...]:
    configs: list[BtcSleeveRiskConfig] = []
    position_sizes = (
        Decimal("0.05"),
        Decimal("0.10"),
        Decimal("0.15"),
        Decimal("0.20"),
        Decimal("0.25"),
        Decimal("0.30"),
        Decimal("0.35"),
        Decimal("0.40"),
        Decimal("0.50"),
        Decimal("0.75"),
        Decimal("1.00"),
    )
    guard_levels: tuple[Optional[float], ...] = (None, 4.0, 6.0, 8.0, 10.0)
    cooldown_periods = (1, 2, 3)

    for position_size_pct in position_sizes:
        configs.append(
            BtcSleeveRiskConfig(
                label=f"ps{size_label(position_size_pct)}_no_guard",
                position_size_pct=position_size_pct,
                drawdown_guard_pct=None,
                cooldown_periods=0,
            )
        )
        for drawdown_guard_pct in guard_levels:
            if drawdown_guard_pct is None:
                continue
            for cooldown in cooldown_periods:
                configs.append(
                    BtcSleeveRiskConfig(
                        label=(
                            f"ps{size_label(position_size_pct)}"
                            f"_dd{guard_label(drawdown_guard_pct)}"
                            f"_cd{cooldown}"
                        ),
                        position_size_pct=position_size_pct,
                        drawdown_guard_pct=drawdown_guard_pct,
                        cooldown_periods=cooldown,
                    )
                )
            configs.append(
                BtcSleeveRiskConfig(
                    label=f"ps{size_label(position_size_pct)}_dd{guard_label(drawdown_guard_pct)}_hard",
                    position_size_pct=position_size_pct,
                    drawdown_guard_pct=drawdown_guard_pct,
                    cooldown_periods=999,
                )
            )
    return tuple(configs)


def parse_config_labels(value: Optional[str]) -> set[str]:
    if value is None:
        return set()
    return {item.strip() for item in value.split(",") if item.strip()}


def generate_windows(evaluation_start_at: datetime, end_at: datetime) -> list[dict[str, datetime]]:
    windows: list[dict[str, datetime]] = []
    test_start_at = evaluation_start_at
    while test_start_at < end_at:
        test_end_at = min(next_rebalance_at(test_start_at, "monthly"), end_at)
        windows.append(
            {
                "train_start_at": add_months(test_start_at, -2),
                "train_end_at": test_start_at,
                "test_start_at": test_start_at,
                "test_end_at": test_end_at,
            }
        )
        if test_end_at <= test_start_at:
            break
        test_start_at = test_end_at
    return windows


def find_flat_priority_variant() -> CategoryVariant:
    for variant in build_regime_variants():
        if variant.label == "flat_priority":
            return variant
    raise ValueError("flat_priority regime variant is not available")


def score_summary(summary: dict[str, Any]) -> float:
    return round(
        float(summary["total_return_pct"]) - (float(summary["max_drawdown_pct"]) * 0.15),
        6,
    )


def make_cash_curve(capital: Decimal, timestamp: datetime) -> list[EquityPoint]:
    return [
        EquityPoint(
            timestamp=timestamp,
            equity=capital,
            cash=capital,
            close_price=capital,
            position_qty=ZERO,
        )
    ]


def drawdown_from_peak_pct(current_equity: Decimal, peak_equity: Decimal) -> float:
    if peak_equity <= ZERO:
        return 0.0
    return round(float(((peak_equity - current_equity) / peak_equity) * Decimal("100")), 4)


def run_variant_backtest(
    *,
    engine: BacktestEngine,
    symbol: str,
    candles: Sequence[BacktestCandle],
    start_at: datetime,
    end_at: datetime,
    fee: Decimal,
    slippage: Decimal,
    position_size_pct: Decimal,
    variant: CategoryVariant,
    strategy_code: str,
) -> BacktestResponse:
    candles_in_scope = [candle for candle in candles if candle.open_time < end_at]
    request = BacktestRequest(
        strategy_code=strategy_code,
        symbol=symbol,
        timeframe="4h",
        start_at=start_at,
        end_at=end_at,
        initial_capital=BASE_SYMBOL_CAPITAL,
        fee=fee,
        slippage=slippage,
        position_size_pct=position_size_pct,
        strategy_config_override=variant.overrides,
    )
    return engine.run(
        request=request,
        strategy=RegimeAwareStrategy(),
        candles=candles_in_scope,
    )


def scaled_trade_stats(
    response: BacktestResponse,
    scale_factor: Decimal,
) -> tuple[int, int, Decimal, Decimal]:
    total_trades = response.metrics.total_trades
    winning_trades = 0
    gross_profit = ZERO
    gross_loss = ZERO
    for trade in response.trades:
        pnl = trade.pnl * scale_factor
        if pnl > ZERO:
            winning_trades += 1
            gross_profit += pnl
        elif pnl < ZERO:
            gross_loss += abs(pnl)
    return total_trades, winning_trades, gross_profit, gross_loss


def main() -> None:
    args = parse_args()
    symbol = args.symbol.strip().upper()
    evaluation_start_at = parse_datetime(args.evaluation_start_at)
    end_at = parse_datetime(args.end_at)
    initial_capital = Decimal(str(args.initial_capital))
    fee = Decimal(str(args.fee))
    slippage = Decimal(str(args.slippage))
    requested_labels = parse_config_labels(args.config_labels)

    configs = build_configs()
    if requested_labels:
        configs = tuple(config for config in configs if config.label in requested_labels)
        if not configs:
            raise ValueError("No BTC sleeve risk configs matched --config-labels")

    fetch_start_at = add_months(evaluation_start_at, -2) - timedelta(days=args.warmup_days)
    windows = generate_windows(evaluation_start_at=evaluation_start_at, end_at=end_at)
    variant = find_flat_priority_variant()

    client = BinanceUSClient()
    try:
        candles = fetch_candles(
            client=client,
            symbol=symbol,
            timeframe="4h",
            start_at=fetch_start_at,
            end_at=end_at,
        )
    finally:
        client.close()

    train_engine = BacktestEngine()
    test_engine = BacktestEngine()
    train_cache: dict[tuple[str, str], BacktestResponse] = {}
    test_cache: dict[tuple[str, str, str], BacktestResponse] = {}

    def get_train_result(train_start_at: datetime, train_end_at: datetime) -> BacktestResponse:
        cache_key = (train_start_at.isoformat(), train_end_at.isoformat())
        cached = train_cache.get(cache_key)
        if cached is not None:
            return cached
        cached = run_variant_backtest(
            engine=train_engine,
            symbol=symbol,
            candles=candles,
            start_at=train_start_at,
            end_at=train_end_at,
            fee=fee,
            slippage=slippage,
            position_size_pct=Decimal("0.10"),
            variant=variant,
            strategy_code="btc_sleeve_risk_controls:train",
        )
        train_cache[cache_key] = cached
        return cached

    def get_test_result(
        test_start_at: datetime,
        test_end_at: datetime,
        position_size_pct: Decimal,
    ) -> BacktestResponse:
        cache_key = (
            test_start_at.isoformat(),
            test_end_at.isoformat(),
            str(position_size_pct),
        )
        cached = test_cache.get(cache_key)
        if cached is not None:
            return cached
        cached = run_variant_backtest(
            engine=test_engine,
            symbol=symbol,
            candles=candles,
            start_at=test_start_at,
            end_at=test_end_at,
            fee=fee,
            slippage=slippage,
            position_size_pct=position_size_pct,
            variant=variant,
            strategy_code=f"btc_sleeve_risk_controls:test:{position_size_pct}",
        )
        test_cache[cache_key] = cached
        return cached

    reports: list[dict[str, Any]] = []
    for config in configs:
        print(f"Evaluating {config.label}...")
        capital = initial_capital
        peak_equity = initial_capital
        prior_drawdown_pct = 0.0
        cooldown_remaining = 0
        positive_periods = 0
        cash_periods = 0
        guard_triggers = 0
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
            guard_triggered = False

            if cooldown_remaining > 0:
                cash_periods += 1
                cooldown_remaining -= 1
                append_points(
                    equity_curve,
                    make_cash_curve(capital=capital, timestamp=window["test_end_at"]),
                )
                current_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
                prior_drawdown_pct = current_drawdown_pct
                periods.append(
                    {
                        "train_start_at": window["train_start_at"].isoformat(),
                        "train_end_at": window["train_end_at"].isoformat(),
                        "test_start_at": window["test_start_at"].isoformat(),
                        "test_end_at": window["test_end_at"].isoformat(),
                        "selected_variant": variant.label,
                        "cash_mode": True,
                        "reason": "cooldown",
                        "train_total_trades": train_trade_count,
                        "starting_capital": round(float(capital), 4),
                        "ending_capital": round(float(capital), 4),
                        "total_return_pct": 0.0,
                        "max_drawdown_pct": 0.0,
                        "total_trades": 0,
                        "win_rate_pct": 0.0,
                        "profit_factor": 0.0,
                        "peak_equity": round(float(peak_equity), 4),
                        "drawdown_from_peak_pct": current_drawdown_pct,
                        "guard_triggered": False,
                        "cooldown_remaining_after": cooldown_remaining,
                    }
                )
                continue

            if train_trade_count < 2:
                cash_periods += 1
                append_points(
                    equity_curve,
                    make_cash_curve(capital=capital, timestamp=window["test_end_at"]),
                )
                current_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
                prior_drawdown_pct = current_drawdown_pct
                periods.append(
                    {
                        "train_start_at": window["train_start_at"].isoformat(),
                        "train_end_at": window["train_end_at"].isoformat(),
                        "test_start_at": window["test_start_at"].isoformat(),
                        "test_end_at": window["test_end_at"].isoformat(),
                        "selected_variant": variant.label,
                        "cash_mode": True,
                        "reason": "insufficient_train_trades",
                        "train_total_trades": train_trade_count,
                        "starting_capital": round(float(capital), 4),
                        "ending_capital": round(float(capital), 4),
                        "total_return_pct": 0.0,
                        "max_drawdown_pct": 0.0,
                        "total_trades": 0,
                        "win_rate_pct": 0.0,
                        "profit_factor": 0.0,
                        "peak_equity": round(float(peak_equity), 4),
                        "drawdown_from_peak_pct": current_drawdown_pct,
                        "guard_triggered": False,
                        "cooldown_remaining_after": 0,
                    }
                )
                continue

            base_result = get_test_result(
                test_start_at=window["test_start_at"],
                test_end_at=window["test_end_at"],
                position_size_pct=config.position_size_pct,
            )
            scale_factor = capital / BASE_SYMBOL_CAPITAL
            slice_curve = build_slice_curve(
                scaled_results=((base_result, scale_factor, capital),),
                initial_capital=capital,
                test_end_at=window["test_end_at"],
            )
            append_points(equity_curve, slice_curve)

            slice_final_capital = base_result.final_equity * scale_factor
            slice_total_trades, slice_winning_trades, slice_gross_profit, slice_gross_loss = scaled_trade_stats(
                response=base_result,
                scale_factor=scale_factor,
            )
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
            if capital > peak_equity:
                peak_equity = capital

            current_drawdown_pct = drawdown_from_peak_pct(current_equity=capital, peak_equity=peak_equity)
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
                    "selected_variant": variant.label,
                    "cash_mode": False,
                    "reason": "active",
                    "train_total_trades": train_trade_count,
                    **slice_summary,
                    "peak_equity": round(float(peak_equity), 4),
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
            item["summary"]["score"],
            item["summary"]["total_return_pct"],
            -item["summary"]["max_drawdown_pct"],
        ),
        reverse=True,
    )
    winner_by_return = max(
        reports,
        key=lambda item: (
            item["summary"]["total_return_pct"],
            item["summary"]["score"],
            -item["summary"]["max_drawdown_pct"],
        ),
    )
    winner_by_score = reports[0]
    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "symbol": symbol,
        "window": {
            "fetch_start_at": fetch_start_at.isoformat(),
            "evaluation_start_at": evaluation_start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        "variant": {
            "label": variant.label,
            "overrides": variant.overrides,
        },
        "reports": reports,
        "winner_by_score": winner_by_score,
        "winner_by_return": winner_by_return,
    }

    rendered = json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote BTC sleeve risk-control report to {output_path}")
    print(rendered)


if __name__ == "__main__":
    main()
