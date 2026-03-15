from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.deep_flush_micro_reclaim import (
    DeepFlushMicroReclaimConfig,
    DeepFlushMicroReclaimStrategy,
)
from app.strategies.registry import get_strategy, list_strategies


def _candle(ts: datetime, open_price: str, high: str, low: str, close: str) -> BacktestCandle:
    return BacktestCandle(
        open_time=ts,
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("1"),
    )


def _history(*candles: tuple[str, str, str, str]) -> list[BacktestCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        _candle(start + timedelta(minutes=index * 5), open_price, high, low, close)
        for index, (open_price, high, low, close) in enumerate(candles)
    ]


def _config(**overrides: object) -> DeepFlushMicroReclaimConfig:
    payload: dict[str, object] = {
        "position_size_pct": 1,
        "stop_loss_pct": 0.1,
        "take_profit_pct": 0,
        "flush_lookback_bars": 6,
        "context_lookback_bars": 6,
        "min_drawdown_from_high_pct": 0.02,
        "max_rebound_from_low_pct": 0.01,
        "require_negative_context_return": True,
        "max_context_return_pct": 0,
        "flush_low_max_age_bars": 2,
        "require_green_entry_candle": True,
        "min_entry_bar_return_pct": 0.003,
        "max_entry_bar_return_pct": 0.02,
        "atr_period": 3,
        "stop_mode": "signal_low",
        "stop_lookback_bars": 3,
        "stop_atr_buffer": 0,
        "max_stop_pct": 0.1,
        "target_mode": "fixed_pct",
        "target_fixed_pct": 0.02,
        "require_cost_edge": False,
        "cost_multiplier": 1,
        "exit_ema_period": 3,
        "exit_on_ema_loss": False,
        "exit_on_stall": False,
        "min_hold_bars": 1,
        "max_bars_in_trade": 4,
        "regime_filter_enabled": False,
    }
    payload.update(overrides)
    return DeepFlushMicroReclaimConfig(**payload)


def _entry_history() -> list[BacktestCandle]:
    return _history(
        ("100", "100.5", "99.5", "100"),
        ("101", "101.5", "100.5", "101"),
        ("102", "102.5", "101.5", "102"),
        ("101", "101.2", "100", "101"),
        ("99.4", "99.6", "98.8", "99.1"),
        ("97.8", "98.0", "97.0", "97.1"),
        ("97.2", "97.5", "97.0", "97.2"),
        ("97.1", "97.8", "97.05", "97.7"),
    )


def test_deep_flush_micro_reclaim_strategy_is_registered() -> None:
    strategy = get_strategy("deep_flush_micro_reclaim")

    assert strategy.key == "deep_flush_micro_reclaim"
    assert "deep_flush_micro_reclaim" in {item.key for item in list_strategies()}


def test_deep_flush_micro_reclaim_enters_on_deep_flush_early_reclaim() -> None:
    strategy = DeepFlushMicroReclaimStrategy()
    history = _entry_history()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="5m",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "has_position": False,
                "position": None,
                "config": _config(),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "enter"
    assert signal.reason == "deep_flush_micro_reclaim_entry"
    assert Decimal(str(signal.metadata["drawdown_from_high_pct"])) < Decimal("-0.02")
    assert Decimal(str(signal.metadata["rebound_from_low_pct"])) < Decimal("0.01")
    assert Decimal(str(signal.metadata["stop_price"])) < Decimal(str(signal.metadata["current_close"]))
    assert Decimal(str(signal.metadata["take_profit_price"])) > Decimal(str(signal.metadata["current_close"]))


def test_deep_flush_micro_reclaim_rejects_late_rebound_entry() -> None:
    strategy = DeepFlushMicroReclaimStrategy()
    history = _history(
        ("100", "100.5", "99.5", "100"),
        ("101", "101.5", "100.5", "101"),
        ("102", "102.5", "101.5", "102"),
        ("101", "101.2", "100", "101"),
        ("99.4", "99.6", "98.8", "99.1"),
        ("97.8", "98.0", "97.0", "97.1"),
        ("97.2", "97.5", "97.0", "97.2"),
        ("97.8", "98.8", "97.7", "98.6"),
    )

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="5m",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "has_position": False,
                "position": None,
                "config": _config(),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "hold"
    assert signal.reason == "safety_guard_failed"
    assert signal.metadata["skip_reason_detail"] == "late_rebound_entry"


def test_deep_flush_micro_reclaim_rejects_positive_context_reset() -> None:
    strategy = DeepFlushMicroReclaimStrategy()
    history = _history(
        ("94", "94.5", "93.8", "94"),
        ("95", "95.5", "94.8", "95"),
        ("100", "100.5", "99.5", "100"),
        ("99", "99.2", "98.4", "99"),
        ("98", "98.4", "97.8", "98"),
        ("97.8", "98.0", "97.0", "97.1"),
        ("97.2", "97.5", "97.0", "97.2"),
        ("97.1", "97.8", "97.05", "97.7"),
    )

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="5m",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "has_position": False,
                "position": None,
                "config": _config(),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "hold"
    assert signal.reason == "safety_guard_failed"
    assert signal.metadata["skip_reason_detail"] == "context_not_reset"


def test_deep_flush_micro_reclaim_backtest_engine_closes_with_time_stop() -> None:
    strategy = DeepFlushMicroReclaimStrategy()
    candles = _entry_history()
    next_open_time = candles[-1].open_time
    candles.extend(
        [
            _candle(next_open_time + timedelta(minutes=5), "97.7", "98.0", "97.6", "97.8"),
            _candle(next_open_time + timedelta(minutes=10), "97.8", "97.9", "97.6", "97.75"),
        ]
    )
    request = BacktestRequest(
        strategy_code=strategy.key,
        symbol="BTC-USD",
        timeframe="5m",
        start_at=candles[0].open_time,
        end_at=candles[-1].open_time,
        exchange_code="coinbase",
        initial_capital=Decimal("1000"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        position_size_pct=Decimal("1"),
        strategy_config_override=_config(
            target_fixed_pct=0.05,
            max_bars_in_trade=2,
            exit_on_stall=False,
            exit_on_ema_loss=False,
        ).model_dump(),
    )

    result = BacktestEngine().run(request, strategy, candles)

    assert result.metrics.total_trades == 1
    assert result.trades[0].metadata["exit_reason_label"] == "time_stop"
