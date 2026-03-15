from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.pullback_in_trend import PullbackInTrendConfig, PullbackInTrendStrategy
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


def _entry_config(**overrides: object) -> PullbackInTrendConfig:
    payload: dict[str, object] = {
        "impulse_lookback_bars": 4,
        "pullback_lookback_bars": 3,
        "pullback_ema_period": 3,
        "atr_period": 3,
        "regime_filter_enabled": False,
        "require_cost_edge": False,
        "impulse_min_return_pct": 0.01,
        "min_pullback_pct": 0.001,
        "max_pullback_pct": 0.03,
        "min_impulse_retrace_ratio": 0.15,
        "max_impulse_retrace_ratio": 0.75,
        "trigger_mode": "either",
        "trigger_min_body_pct": 0.0005,
        "max_stop_pct": 0.05,
        "stop_buffer_atr_mult": 0.0,
        "max_bars_in_trade": 1,
    }
    payload.update(overrides)
    return PullbackInTrendConfig(**payload)


def _entry_history() -> list[BacktestCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        _candle(start + timedelta(minutes=0), "100.0", "100.5", "99.8", "100.3"),
        _candle(start + timedelta(minutes=5), "100.3", "100.5", "100.1", "100.4"),
        _candle(start + timedelta(minutes=10), "100.4", "100.9", "100.2", "100.8"),
        _candle(start + timedelta(minutes=15), "100.8", "101.4", "100.7", "101.3"),
        _candle(start + timedelta(minutes=20), "101.3", "102.1", "101.2", "102.0"),
        _candle(start + timedelta(minutes=25), "102.0", "102.05", "101.7", "101.8"),
        _candle(start + timedelta(minutes=30), "101.8", "101.85", "101.4", "101.5"),
        _candle(start + timedelta(minutes=35), "101.5", "101.6", "101.45", "101.55"),
        _candle(start + timedelta(minutes=40), "101.6", "102.1", "101.55", "102.0"),
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


def test_pullback_in_trend_strategy_is_registered() -> None:
    strategy = get_strategy("pullback_in_trend")

    assert strategy.key == "pullback_in_trend"
    assert "pullback_in_trend" in {item.key for item in list_strategies()}


def test_pullback_in_trend_enters_on_controlled_pullback_trigger() -> None:
    strategy = PullbackInTrendStrategy()
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
    assert signal.reason == "pullback_in_trend_entry"
    assert signal.metadata["stop_mode"] == "pullback_low"
    assert signal.metadata["target_mode"] == "stop_multiple"
    assert Decimal(str(signal.metadata["stop_price"])) == Decimal("101.4")
    assert Decimal(str(signal.metadata["take_profit_price"])) == Decimal("102.6")


def test_pullback_in_trend_rejects_without_recent_impulse() -> None:
    strategy = PullbackInTrendStrategy()
    history = _entry_history()
    for index in range(1, 5):
        candle = history[index]
        history[index] = _candle(candle.open_time, "100.0", "100.2", "99.9", "100.05")
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
    assert signal.reason == "no_recent_impulse"


def test_pullback_in_trend_rejects_when_pullback_is_too_deep() -> None:
    strategy = PullbackInTrendStrategy()
    history = _entry_history()
    history[-3] = _candle(history[-3].open_time, "101.8", "101.85", "100.2", "100.4")
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
    assert signal.reason == "pullback_too_deep"
    assert signal.metadata["skip_reason_detail"] == "pullback_too_deep"


def test_pullback_in_trend_rejects_when_trigger_is_not_confirmed() -> None:
    strategy = PullbackInTrendStrategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "101.55", "101.7", "101.45", "101.6")
    config = _entry_config(trigger_mode="break_prev_high")

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
    assert signal.reason == "trigger_not_confirmed"
    assert signal.metadata["skip_reason_detail"] == "trigger_conditions_not_met"


def test_pullback_in_trend_exits_on_pullback_failure() -> None:
    strategy = PullbackInTrendStrategy()
    history = _entry_history()
    failure_candle = _candle(history[-1].open_time + timedelta(minutes=5), "101.9", "102.0", "101.2", "101.3")
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
                    "entry_metadata": {"pullback_low": "101.4"},
                },
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "pullback_failure"


def test_pullback_in_trend_backtest_reports_trigger_not_confirmed_diagnostics() -> None:
    engine = BacktestEngine()
    strategy = PullbackInTrendStrategy()
    candles = _entry_history()
    candles[-1] = _candle(candles[-1].open_time, "101.55", "101.7", "101.45", "101.6")

    report = engine.run(
        request=_request(strategy.key, override=_entry_config(trigger_mode="break_prev_high").model_dump()),
        strategy=strategy,
        candles=candles,
    )

    assert report.metrics.total_trades == 0
    assert report.diagnostics["entry_hold_reasons"]["trigger_not_confirmed"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["impulse_candidate_count"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["pullback_candidate_count"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["trigger_confirmed_count"] == 0


def test_pullback_in_trend_paper_flow_handles_entry_then_time_stop_exit() -> None:
    engine = PaperEngine()
    strategy = PullbackInTrendStrategy()
    config = _entry_config(target_r_multiple=3.0, max_bars_in_trade=1)
    candles = _entry_history() + [
        _candle(datetime(2026, 1, 1, 0, 45, tzinfo=timezone.utc), "101.95", "102.1", "101.8", "101.9")
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
    assert results[-1].trade_event.metadata_json["exit_reason"] == "time_stop"
