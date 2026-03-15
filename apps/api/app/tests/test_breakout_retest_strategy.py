from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.breakout_retest import BreakoutRetestConfig, BreakoutRetestStrategy
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


def _entry_config(**overrides: object) -> BreakoutRetestConfig:
    payload: dict[str, object] = {
        "breakout_lookback": 4,
        "atr_period": 3,
        "regime_filter_enabled": False,
        "require_cost_edge": False,
        "max_bars_in_trade": 3,
        "retest_tolerance_pct": 0.004,
        "breakout_min_body_pct": 0.001,
        "breakout_max_body_pct": 0.03,
        "max_breakout_extension_pct": 0.01,
        "confirmation_min_body_pct": 0.0005,
        "max_stop_pct": 0.05,
        "stop_buffer_atr_mult": 0.0,
        "breakout_failure_buffer_pct": 0.0,
    }
    payload.update(overrides)
    return BreakoutRetestConfig(**payload)


def _entry_history() -> list[BacktestCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        _candle(start + timedelta(minutes=0), "100.0", "100.8", "99.7", "100.4"),
        _candle(start + timedelta(minutes=5), "100.4", "100.9", "100.0", "100.6"),
        _candle(start + timedelta(minutes=10), "100.6", "101.0", "100.2", "100.7"),
        _candle(start + timedelta(minutes=15), "100.7", "101.1", "100.3", "100.8"),
        _candle(start + timedelta(minutes=20), "100.8", "101.3", "100.6", "101.2"),
        _candle(start + timedelta(minutes=25), "101.2", "101.4", "100.95", "101.3"),
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


def test_breakout_retest_strategy_is_registered() -> None:
    strategy = get_strategy("breakout_retest")

    assert strategy.key == "breakout_retest"
    assert "breakout_retest" in {item.key for item in list_strategies()}


def test_breakout_retest_enters_on_breakout_retest_confirmation() -> None:
    strategy = BreakoutRetestStrategy()
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
    assert signal.reason == "breakout_retest_entry"
    assert signal.metadata["stop_mode"] == "hybrid"
    assert signal.metadata["target_mode"] == "stop_multiple"
    assert Decimal(str(signal.metadata["stop_price"])) == Decimal("101.1")
    assert Decimal(str(signal.metadata["take_profit_price"])) == Decimal("101.5")


def test_breakout_retest_rejects_breakout_bar_too_weak() -> None:
    strategy = BreakoutRetestStrategy()
    history = _entry_history()
    history[-2] = _candle(history[-2].open_time, "101.02", "101.14", "100.9", "101.11")
    config = _entry_config(breakout_min_body_pct=0.002)

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
    assert signal.reason == "breakout_bar_too_weak"
    assert signal.metadata["reason_skipped"] == "breakout_bar_too_weak"


def test_breakout_retest_rejects_when_retest_is_not_reached() -> None:
    strategy = BreakoutRetestStrategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "101.55", "101.7", "101.52", "101.62")
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

    assert signal.action == "hold"
    assert signal.reason == "retest_not_reached"
    assert signal.metadata["skip_reason_detail"] == "retest_not_reached"


def test_breakout_retest_rejects_when_retest_fails() -> None:
    strategy = BreakoutRetestStrategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "101.2", "101.3", "100.6", "100.85")
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

    assert signal.action == "hold"
    assert signal.reason == "retest_failed"
    assert signal.metadata["skip_reason_detail"] == "retest_lost_breakout_level"


def test_breakout_retest_exits_on_breakout_failure() -> None:
    strategy = BreakoutRetestStrategy()
    history = _entry_history()
    failure_candle = _candle(history[-1].open_time + timedelta(minutes=5), "101.2", "101.25", "100.7", "100.8")
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
                    "entry_metadata": {"breakout_level": "101.1"},
                },
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "breakout_failure"


def test_breakout_retest_backtest_reports_retest_not_reached_diagnostics() -> None:
    engine = BacktestEngine()
    strategy = BreakoutRetestStrategy()
    candles = _entry_history()
    candles[-1] = _candle(candles[-1].open_time, "101.55", "101.7", "101.52", "101.62")

    report = engine.run(
        request=_request(strategy.key, override=_entry_config().model_dump()),
        strategy=strategy,
        candles=candles,
    )

    assert report.metrics.total_trades == 0
    assert report.diagnostics["entry_hold_reasons"]["retest_not_reached"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["breakout_candidate_count"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["retest_candidate_count"] == 0


def test_breakout_retest_paper_flow_handles_entry_then_breakout_failure_exit() -> None:
    engine = PaperEngine()
    strategy = BreakoutRetestStrategy()
    config = _entry_config(stop_mode="breakout_bar_low")
    candles = _entry_history() + [
        _candle(datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc), "101.2", "101.25", "100.7", "100.8")
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
