from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.pullback_in_trend_v2 import PullbackInTrendV2Config, PullbackInTrendV2Strategy
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


def _entry_config(**overrides: object) -> PullbackInTrendV2Config:
    payload: dict[str, object] = {
        "impulse_lookback_bars": 4,
        "impulse_max_bars": 4,
        "impulse_min_return_pct": 0.01,
        "impulse_min_body_pct": 0.005,
        "pullback_lookback_bars": 3,
        "pullback_ema_period": 3,
        "min_pullback_pct": 0.001,
        "max_pullback_pct": 0.03,
        "min_impulse_retrace_ratio": 0.2,
        "max_impulse_retrace_ratio": 0.6,
        "trigger_mode": "reclaim_and_break_prev_high",
        "trigger_min_body_pct": 0.002,
        "close_near_high_threshold": 0.6,
        "atr_period": 3,
        "stop_buffer_atr_mult": 0.0,
        "max_stop_pct": 0.05,
        "regime_filter_enabled": False,
        "require_cost_edge": False,
        "max_bars_in_trade": 4,
        "fast_failure_bars": 1,
        "fast_failure_min_progress_r": 0.2,
    }
    payload.update(overrides)
    return PullbackInTrendV2Config(**payload)


def _entry_history() -> list[BacktestCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        _candle(start + timedelta(minutes=0), "99.8", "100.0", "99.7", "99.9"),
        _candle(start + timedelta(minutes=5), "100.0", "100.2", "99.9", "100.1"),
        _candle(start + timedelta(minutes=10), "100.1", "101.0", "100.0", "100.9"),
        _candle(start + timedelta(minutes=15), "100.9", "102.0", "100.8", "101.9"),
        _candle(start + timedelta(minutes=20), "101.9", "103.2", "101.8", "103.0"),
        _candle(start + timedelta(minutes=25), "103.0", "103.05", "102.4", "102.6"),
        _candle(start + timedelta(minutes=30), "102.6", "102.7", "102.1", "102.2"),
        _candle(start + timedelta(minutes=35), "102.2", "102.35", "102.0", "102.15"),
        _candle(start + timedelta(minutes=40), "102.15", "102.9", "102.1", "102.85"),
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


def test_pullback_in_trend_v2_strategy_is_registered() -> None:
    strategy = get_strategy("pullback_in_trend_v2")

    assert strategy.key == "pullback_in_trend_v2"
    assert "pullback_in_trend_v2" in {item.key for item in list_strategies()}


def test_pullback_in_trend_v2_enters_on_quality_pullback_trigger() -> None:
    strategy = PullbackInTrendV2Strategy()
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
    assert signal.reason == "pullback_in_trend_v2_entry"
    assert signal.metadata["trigger_mode"] == "reclaim_and_break_prev_high"
    assert Decimal(str(signal.metadata["stop_price"])) == Decimal("102.0")


def test_pullback_in_trend_v2_rejects_trend_too_extended() -> None:
    strategy = PullbackInTrendV2Strategy()
    history = [
        _candle(datetime(2026, 1, 1, index, 0, tzinfo=timezone.utc), "100", "101", "99", str(100 + index))
        for index in range(8)
    ]
    history[-1] = _candle(history[-1].open_time, "107", "120", "106", "118")
    config = _entry_config(
        regime_filter_enabled=True,
        require_cost_edge=False,
        impulse_lookback_bars=4,
        pullback_lookback_bars=2,
        pullback_ema_period=2,
        atr_period=2,
        regime_ema_period=3,
        regime_atr_period=2,
        max_atr_pct_1h=1.0,
        trend_extension_ema_short_period=3,
        trend_extension_ema_long_period=4,
        max_close_above_ema20_1h_pct=0.01,
        max_close_above_ema50_1h_pct=0.02,
    )

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USDT",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={"history": history, "config": config, "has_position": False},
        )
    )

    assert signal.action == "hold"
    assert signal.reason == "regime_blocked"
    assert signal.metadata["skip_reason_detail"] == "trend_too_extended"


def test_pullback_in_trend_v2_rejects_weak_impulse() -> None:
    strategy = PullbackInTrendV2Strategy()
    history = _entry_history()
    config = _entry_config(impulse_min_body_pct=0.04)

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
    assert signal.reason == "impulse_too_weak"


def test_pullback_in_trend_v2_rejects_pullback_that_breaks_structure() -> None:
    strategy = PullbackInTrendV2Strategy()
    history = _entry_history()
    history[-2] = _candle(history[-2].open_time, "102.2", "102.35", "99.8", "102.15")
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
    assert signal.reason == "pullback_broke_structure"
    assert signal.metadata["skip_reason_detail"] == "pullback_broke_structure"


def test_pullback_in_trend_v2_rejects_trigger_without_strong_close() -> None:
    strategy = PullbackInTrendV2Strategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "102.15", "102.9", "102.1", "102.45")
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
    assert signal.reason == "trigger_close_not_strong_enough"


def test_pullback_in_trend_v2_exits_fast_when_continuation_stalls() -> None:
    strategy = PullbackInTrendV2Strategy()
    history = _entry_history()
    follow_through = _candle(history[-1].open_time + timedelta(minutes=5), "102.82", "102.9", "102.7", "102.86")
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
                    "entry_price": "102.85",
                    "entry_metadata": {
                        "pullback_low": "102.0",
                        "stop_price": "102.0",
                    },
                },
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "fast_failure"


def test_pullback_in_trend_v2_backtest_reports_trigger_close_diagnostics() -> None:
    engine = BacktestEngine()
    strategy = PullbackInTrendV2Strategy()
    candles = _entry_history()
    candles[-1] = _candle(candles[-1].open_time, "102.15", "102.9", "102.1", "102.45")

    report = engine.run(
        request=_request(strategy.key, override=_entry_config().model_dump()),
        strategy=strategy,
        candles=candles,
    )

    assert report.metrics.total_trades == 0
    assert report.diagnostics["entry_hold_reasons"]["trigger_close_not_strong_enough"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["context_pass_count"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["impulse_candidate_count"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["pullback_candidate_count"] >= 1


def test_pullback_in_trend_v2_paper_flow_handles_entry_then_fast_failure() -> None:
    engine = PaperEngine()
    strategy = PullbackInTrendV2Strategy()
    config = _entry_config(target_r_multiple=3.0, fast_failure_bars=1, max_bars_in_trade=4)
    candles = _entry_history() + [
        _candle(datetime(2026, 1, 1, 0, 45, tzinfo=timezone.utc), "102.82", "102.9", "102.7", "102.86")
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
