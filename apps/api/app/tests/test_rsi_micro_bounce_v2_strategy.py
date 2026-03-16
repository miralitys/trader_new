from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.registry import get_strategy, list_strategies
from app.strategies.rsi_micro_bounce_v2 import RSIMicroBounceV2Config, RSIMicroBounceV2Strategy


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


def _entry_config(**overrides: object) -> RSIMicroBounceV2Config:
    payload: dict[str, object] = {
        "rsi_period": 2,
        "rsi_oversold_threshold": 40,
        "oversold_fresh_bars": 1,
        "require_lower_band_stretch": False,
        "bb_period": 3,
        "bb_stddev": 2.0,
        "ema_period": 3,
        "context_filter_enabled": False,
        "trigger_mode": "wick_reclaim",
        "require_trigger_green": True,
        "min_wick_body_ratio": 0.25,
        "min_close_location": 0.5,
        "trigger_min_body_pct": 0.002,
        "atr_period": 3,
        "stop_mode": "event_low",
        "stop_buffer_atr_mult": 0.0,
        "max_stop_pct": 0.05,
        "target_mode": "stop_multiple",
        "target_r_multiple": 0.6,
        "max_bars_in_trade": 4,
        "fast_failure_bars": 1,
        "fast_failure_min_progress_r": 0.2,
        "require_cost_edge": False,
    }
    payload.update(overrides)
    return RSIMicroBounceV2Config(**payload)


def _entry_history() -> list[BacktestCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        _candle(start + timedelta(minutes=0), "100.0", "100.1", "99.8", "100.0"),
        _candle(start + timedelta(minutes=5), "100.0", "100.0", "99.4", "99.6"),
        _candle(start + timedelta(minutes=10), "99.6", "99.7", "98.9", "99.1"),
        _candle(start + timedelta(minutes=15), "99.1", "99.2", "98.3", "98.4"),
        _candle(start + timedelta(minutes=20), "98.4", "98.5", "97.9", "98.0"),
        _candle(start + timedelta(minutes=25), "98.05", "98.8", "97.85", "98.72"),
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


def test_rsi_micro_bounce_v2_strategy_is_registered() -> None:
    strategy = get_strategy("rsi_micro_bounce_v2")

    assert strategy.key == "rsi_micro_bounce_v2"
    assert "rsi_micro_bounce_v2" in {item.key for item in list_strategies()}


def test_rsi_micro_bounce_v2_enters_on_wick_reclaim_after_oversold() -> None:
    strategy = RSIMicroBounceV2Strategy()
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
    assert signal.reason == "rsi_micro_bounce_v2_entry"
    assert signal.metadata["trigger_mode"] == "wick_reclaim"
    assert Decimal(str(signal.metadata["stop_price"])) == Decimal("97.9")


def test_rsi_micro_bounce_v2_rejects_without_recent_oversold() -> None:
    strategy = RSIMicroBounceV2Strategy()
    history = _entry_history()
    for index in range(1, len(history)):
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
    assert signal.reason == "oversold_not_detected"


def test_rsi_micro_bounce_v2_rejects_when_stretch_is_not_large_enough() -> None:
    strategy = RSIMicroBounceV2Strategy()
    history = _entry_history()
    config = _entry_config(require_lower_band_stretch=True, stretch_min_pct=0.1, bb_stddev=4.0)

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
    assert signal.reason == "stretch_not_large_enough"


def test_rsi_micro_bounce_v2_rejects_when_trigger_is_not_confirmed() -> None:
    strategy = RSIMicroBounceV2Strategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "98.05", "98.2", "97.95", "98.06")
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
    assert signal.reason == "trigger_not_confirmed"


def test_rsi_micro_bounce_v2_rejects_when_close_is_not_strong_enough() -> None:
    strategy = RSIMicroBounceV2Strategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "98.2", "98.75", "97.85", "98.3")
    config = _entry_config(
        trigger_mode="first_uptick",
        require_trigger_green=True,
        trigger_min_body_pct=0.0005,
        min_close_location=0.7,
    )

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
    assert signal.reason == "trigger_close_not_strong_enough"


def test_rsi_micro_bounce_v2_exits_fast_when_bounce_stalls() -> None:
    strategy = RSIMicroBounceV2Strategy()
    history = _entry_history()
    follow_through = _candle(history[-1].open_time + timedelta(minutes=5), "98.70", "98.74", "98.60", "98.73")
    history.append(follow_through)
    config = _entry_config(fast_failure_bars=1, max_bars_in_trade=4)

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USDT",
            timeframe="5m",
            timestamp=follow_through.open_time,
            mode="backtest",
            metadata={
                "history": history,
                "config": config,
                "has_position": True,
                "position": {
                    "entry_time": history[-2].open_time,
                    "entry_price": "98.72",
                    "entry_metadata": {
                        "stop_price": "97.9",
                    },
                },
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "fast_failure"


def test_rsi_micro_bounce_v2_backtest_reports_oversold_diagnostics() -> None:
    engine = BacktestEngine()
    strategy = RSIMicroBounceV2Strategy()
    candles = _entry_history()
    for index in range(1, len(candles)):
        candle = candles[index]
        candles[index] = _candle(candle.open_time, "100.0", "100.2", "99.9", "100.05")

    report = engine.run(
        request=_request(strategy.key, override=_entry_config().model_dump()),
        strategy=strategy,
        candles=candles,
    )

    assert report.metrics.total_trades == 0
    assert report.diagnostics["entry_hold_reasons"]["oversold_not_detected"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["context_pass_count"] >= 1


def test_rsi_micro_bounce_v2_paper_flow_handles_entry_then_fast_failure() -> None:
    engine = PaperEngine()
    strategy = RSIMicroBounceV2Strategy()
    config = _entry_config(target_r_multiple=2.0, fast_failure_bars=1, max_bars_in_trade=4)
    candles = _entry_history() + [
        _candle(datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc), "98.70", "98.74", "98.60", "98.73")
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
    assert results[-1].trade_event.metadata_json["exit_reason"] == "fast_failure"
