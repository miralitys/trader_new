from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.engines.performance_engine import PerformanceEngine
from app.schemas.backtest import BacktestTrade, EquityPoint


def _trade(pnl: str, entry_hour: int, exit_hour: int, *, gross_pnl: str | None = None) -> BacktestTrade:
    return BacktestTrade(
        entry_time=datetime(2026, 1, 1, entry_hour, 0, tzinfo=timezone.utc),
        exit_time=datetime(2026, 1, 1, exit_hour, 0, tzinfo=timezone.utc),
        entry_price=Decimal("100"),
        exit_price=Decimal("110"),
        qty=Decimal("1"),
        gross_pnl=Decimal(gross_pnl if gross_pnl is not None else pnl),
        pnl=Decimal(pnl),
        pnl_pct=Decimal("0"),
        fees=Decimal("0"),
        slippage=Decimal("0"),
        exit_reason="test_exit",
    )


def _equity(timestamp_hour: int, value: str) -> EquityPoint:
    return EquityPoint(
        timestamp=datetime(2026, 1, 1, timestamp_hour, 0, tzinfo=timezone.utc),
        equity=Decimal(value),
        cash=Decimal(value),
        close_price=Decimal("100"),
        position_qty=Decimal("0"),
    )


def test_performance_engine_calculates_metrics() -> None:
    engine = PerformanceEngine()

    metrics = engine.calculate_metrics(
        trades=[
            _trade("200", 0, 1),
            _trade("-50", 2, 3),
            _trade("100", 4, 5),
        ],
        equity_curve=[
            _equity(0, "1000"),
            _equity(1, "1200"),
            _equity(2, "1150"),
            _equity(3, "1300"),
        ],
        initial_capital=Decimal("1000"),
        final_equity=Decimal("1300"),
    )

    assert metrics.total_return_pct == Decimal("30")
    assert metrics.total_trades == 3
    assert metrics.win_rate_pct == Decimal("66.66666666666666666666666667")
    assert metrics.profit_factor == Decimal("6")
    assert metrics.expectancy == Decimal("83.33333333333333333333333333")
    assert metrics.gross_expectancy == Decimal("83.33333333333333333333333333")
    assert metrics.net_expectancy == Decimal("83.33333333333333333333333333")
    assert metrics.avg_winner == Decimal("150")
    assert metrics.avg_loser == Decimal("-50")
    assert metrics.max_drawdown_pct == Decimal("4.166666666666666666666666667")


def test_performance_engine_handles_no_trades() -> None:
    engine = PerformanceEngine()

    metrics = engine.calculate_metrics(
        trades=[],
        equity_curve=[_equity(0, "1000"), _equity(1, "1000")],
        initial_capital=Decimal("1000"),
        final_equity=Decimal("1000"),
    )

    assert metrics.total_return_pct == Decimal("0")
    assert metrics.total_trades == 0
    assert metrics.win_rate_pct == Decimal("0")
    assert metrics.profit_factor == Decimal("0")
    assert metrics.expectancy == Decimal("0")
    assert metrics.gross_expectancy == Decimal("0")
    assert metrics.net_expectancy == Decimal("0")
    assert metrics.avg_winner == Decimal("0")
    assert metrics.avg_loser == Decimal("0")


def test_performance_engine_tracks_gross_and_net_expectancy_separately() -> None:
    engine = PerformanceEngine()

    metrics = engine.calculate_metrics(
        trades=[
            _trade("90", 0, 1, gross_pnl="100"),
            _trade("-20", 2, 3, gross_pnl="-10"),
        ],
        equity_curve=[_equity(0, "1000"), _equity(1, "1070")],
        initial_capital=Decimal("1000"),
        final_equity=Decimal("1070"),
    )

    assert metrics.expectancy == Decimal("35")
    assert metrics.net_expectancy == Decimal("35")
    assert metrics.gross_expectancy == Decimal("45")
