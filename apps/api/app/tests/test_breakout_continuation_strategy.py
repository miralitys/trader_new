from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.breakout_continuation import BreakoutContinuationConfig, BreakoutContinuationStrategy
from app.strategies.registry import get_strategy, list_strategies


def _candle(
    ts: datetime,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
) -> BacktestCandle:
    return BacktestCandle(
        open_time=ts,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=Decimal("1"),
    )


def _entry_config(**overrides: object) -> BreakoutContinuationConfig:
    payload: dict[str, object] = {
        "range_lookback_bars": 4,
        "atr_period": 3,
        "regime_filter_enabled": False,
        "require_cost_edge": False,
        "max_bars_in_trade": 3,
        "max_breakout_extension_pct": 0.01,
        "min_breakout_bar_body_pct": 0.001,
        "max_breakout_bar_body_pct": 0.02,
        "max_stop_pct": 0.05,
        "stop_buffer_atr_mult": 0.0,
    }
    payload.update(overrides)
    return BreakoutContinuationConfig(**payload)


def _entry_history() -> list[BacktestCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        _candle(start + timedelta(minutes=0), "100.0", "100.6", "99.8", "100.2"),
        _candle(start + timedelta(minutes=5), "100.2", "100.8", "100.0", "100.5"),
        _candle(start + timedelta(minutes=10), "100.5", "100.9", "100.1", "100.4"),
        _candle(start + timedelta(minutes=15), "100.4", "101.0", "100.2", "100.6"),
        _candle(start + timedelta(minutes=20), "100.6", "100.8", "100.3", "100.7"),
        _candle(start + timedelta(minutes=25), "100.7", "101.8", "100.6", "101.6"),
    ]


def _request(strategy_code: str, override: dict[str, object] | None = None) -> BacktestRequest:
    return BacktestRequest(
        strategy_code=strategy_code,
        symbol="BTC-USDT",
        timeframe="5m",
        start_at=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
        initial_capital=Decimal("1000"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        position_size_pct=Decimal("1"),
        strategy_config_override=override or {},
    )


def test_breakout_continuation_strategy_is_registered() -> None:
    strategy = get_strategy("breakout_continuation")

    assert strategy.key == "breakout_continuation"
    assert "breakout_continuation" in {item.key for item in list_strategies()}


def test_breakout_continuation_enters_on_tight_range_breakout() -> None:
    strategy = BreakoutContinuationStrategy()
    history = _entry_history()
    config = _entry_config()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USDT",
            timeframe="5m",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={"history": history, "config": config, "has_position": False},
        )
    )

    assert signal.action == "enter"
    assert signal.reason == "breakout_continuation_entry"
    assert signal.metadata["stop_mode"] == "hybrid"
    assert signal.metadata["target_mode"] == "stop_multiple"
    assert Decimal(str(signal.metadata["stop_price"])) == Decimal("101.0")
    assert Decimal(str(signal.metadata["take_profit_price"])) == Decimal("102.2")


def test_breakout_continuation_rejects_overextended_breakout() -> None:
    strategy = BreakoutContinuationStrategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "100.7", "103.0", "100.6", "102.7")
    config = _entry_config(max_breakout_extension_pct=0.005, max_breakout_bar_body_pct=0.015)

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USDT",
            timeframe="5m",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={"history": history, "config": config, "has_position": False},
        )
    )

    assert signal.action == "hold"
    assert signal.reason == "breakout_bar_too_extended"
    assert signal.metadata["reason_skipped"] == "breakout_bar_too_extended"


def test_breakout_continuation_exits_on_breakout_failure() -> None:
    strategy = BreakoutContinuationStrategy()
    history = _entry_history()
    failure_candle = _candle(history[-1].open_time + timedelta(minutes=5), "101.4", "101.5", "100.6", "100.8")
    history.append(failure_candle)
    config = _entry_config()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USDT",
            timeframe="5m",
            timestamp=failure_candle.open_time,
            mode="backtest",
            metadata={
                "history": history,
                "config": config,
                "has_position": True,
                "position": {
                    "entry_time": history[-2].open_time,
                    "entry_metadata": {"breakout_level": "101.0"},
                },
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "breakout_failure"


def test_breakout_continuation_backtest_reports_breakout_not_confirmed_diagnostics() -> None:
    engine = BacktestEngine()
    strategy = BreakoutContinuationStrategy()
    candles = _entry_history()
    candles[-1] = _candle(candles[-1].open_time, "100.7", "100.95", "100.6", "100.9")

    report = engine.run(
        request=_request(strategy.key, override=_entry_config().model_dump()),
        strategy=strategy,
        candles=candles,
    )

    assert report.metrics.total_trades == 0
    assert report.diagnostics["entry_hold_reasons"]["breakout_not_confirmed"] >= 1


def test_breakout_continuation_paper_flow_handles_entry_then_breakout_failure_exit() -> None:
    engine = PaperEngine()
    strategy = BreakoutContinuationStrategy()
    config = _entry_config(stop_mode="breakout_candle_low")
    candles = _entry_history() + [
        _candle(datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc), "101.4", "101.5", "100.65", "100.8")
    ]

    final_state, results = engine.process_candle_batch(
        strategy=strategy,
        symbol="BTC-USDT",
        timeframe="5m",
        candles=candles,
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0"),
        slippage_rate=Decimal("0"),
        strategy_config_override=config.model_dump(),
    )

    assert final_state.position is None
    assert any(result.signal_event and result.signal_event.signal_type == "enter" for result in results)
    assert results[-1].trade_event is not None
    assert results[-1].trade_event.metadata_json["exit_reason"] == "breakout_failure"
