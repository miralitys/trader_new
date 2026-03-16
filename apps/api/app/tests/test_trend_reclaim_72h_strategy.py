from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.registry import get_strategy, list_strategies
from app.strategies.trend_reclaim_72h import TrendReclaim72hConfig, TrendReclaim72hStrategy


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


def _entry_config(**overrides: object) -> TrendReclaim72hConfig:
    payload: dict[str, object] = {
        "required_preroll_days": 0,
        "regime_ema_period_4h": 3,
        "trend_extension_ema_period_4h": 2,
        "require_atr_band_4h": False,
        "require_not_overextended": False,
        "impulse_lookback_bars": 4,
        "impulse_max_bars": 4,
        "impulse_min_return_pct": 0.02,
        "impulse_min_body_pct": 0.01,
        "pullback_lookback_bars": 3,
        "pullback_ema_period": 3,
        "min_impulse_retrace_ratio": 0.2,
        "max_impulse_retrace_ratio": 0.5,
        "max_pullback_pct": 0.06,
        "trigger_mode": "reclaim_and_break_prev_high",
        "trigger_min_body_pct": 0.004,
        "close_near_high_threshold": 0.6,
        "atr_period": 3,
        "stop_buffer_atr_mult": 0.0,
        "max_stop_pct": 0.08,
        "require_cost_edge": False,
        "target_r_multiple": 1.8,
        "fast_failure_bars": 2,
        "fast_failure_min_progress_r": 0.2,
        "max_bars_in_trade": 6,
    }
    payload.update(overrides)
    return TrendReclaim72hConfig(**payload)


def _entry_history() -> list[BacktestCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    values = [
        ("100.0", "101.0", "99.6", "100.5"),
        ("100.5", "102.0", "100.3", "101.6"),
        ("101.6", "103.0", "101.2", "102.5"),
        ("102.5", "103.4", "102.2", "103.0"),
        ("103.0", "105.3", "102.8", "105.0"),
        ("105.0", "108.0", "104.8", "107.6"),
        ("107.6", "111.0", "107.5", "110.4"),
        ("110.4", "113.2", "110.0", "113.0"),
        ("113.0", "113.1", "111.5", "112.0"),
        ("112.0", "112.1", "111.0", "111.4"),
        ("111.4", "111.5", "110.9", "111.1"),
        ("111.1", "113.6", "111.0", "113.4"),
    ]
    return [
        _candle(start + timedelta(hours=index), *value)
        for index, value in enumerate(values)
    ]


def _request(strategy_code: str, override: dict[str, object] | None = None) -> BacktestRequest:
    return BacktestRequest(
        strategy_code=strategy_code,
        symbol="BTC-USDT",
        timeframe="1h",
        start_at=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        end_at=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        initial_capital=Decimal("1000"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        position_size_pct=Decimal("1"),
        strategy_config_override=override or {},
    )


def test_trend_reclaim_72h_strategy_is_registered() -> None:
    strategy = get_strategy("trend_reclaim_72h")

    assert strategy.key == "trend_reclaim_72h"
    assert "trend_reclaim_72h" in {item.key for item in list_strategies()}


def test_trend_reclaim_72h_enters_on_reclaim_trigger() -> None:
    strategy = TrendReclaim72hStrategy()
    history = _entry_history()
    config = _entry_config()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USDT",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={"history": history, "config": config, "has_position": False},
        )
    )

    assert signal.action == "enter"
    assert signal.reason == "trend_reclaim_72h_entry"
    assert signal.metadata["trigger_mode"] == "reclaim_and_break_prev_high"
    assert Decimal(str(signal.metadata["stop_price"])) == Decimal("110.9")


def test_trend_reclaim_72h_rejects_when_regime_is_below_4h_ema() -> None:
    strategy = TrendReclaim72hStrategy()
    history = _entry_history()
    history[0] = _candle(history[0].open_time, "130.0", "131.0", "129.0", "130.0")
    history[1] = _candle(history[1].open_time, "130.0", "130.5", "128.5", "129.0")
    history[2] = _candle(history[2].open_time, "129.0", "129.5", "127.5", "128.0")
    history[3] = _candle(history[3].open_time, "128.0", "128.5", "126.5", "127.0")
    config = _entry_config(require_atr_band_4h=False)

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
    assert signal.metadata["skip_reason_detail"] == "close_below_ema200_4h"


def test_trend_reclaim_72h_rejects_trigger_without_strong_close() -> None:
    strategy = TrendReclaim72hStrategy()
    history = _entry_history()
    history[-1] = _candle(history[-1].open_time, "111.1", "113.6", "111.0", "112.0")
    config = _entry_config()

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
    assert signal.reason == "trigger_close_not_strong_enough"


def test_trend_reclaim_72h_exits_fast_when_swing_stalls() -> None:
    strategy = TrendReclaim72hStrategy()
    history = _entry_history()
    history.append(_candle(history[-1].open_time + timedelta(hours=1), "113.3", "113.5", "112.8", "113.2"))
    history.append(_candle(history[-1].open_time + timedelta(hours=1), "113.2", "113.3", "112.7", "113.0"))
    config = _entry_config(fast_failure_bars=2, max_bars_in_trade=10)

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USDT",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "config": config,
                "has_position": True,
                "position": {
                    "entry_time": history[-3].open_time,
                    "entry_price": "113.4",
                    "entry_metadata": {"stop_price": "110.9"},
                },
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "fast_failure"


def test_trend_reclaim_72h_backtest_reports_runtime_window_and_trigger_diagnostics() -> None:
    engine = BacktestEngine()
    strategy = TrendReclaim72hStrategy()
    candles = _entry_history()
    candles[-1] = _candle(candles[-1].open_time, "111.1", "113.6", "111.0", "112.0")
    request = _request(strategy.key, override=_entry_config(required_preroll_days=0).model_dump()).model_copy(
        update={"start_at": datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc)}
    )

    report = engine.run(
        request=request,
        strategy=strategy,
        candles=candles,
        preroll_days=2,
    )

    assert report.metrics.total_trades == 0
    assert report.diagnostics["runtime_window"]["preroll_days"] == 2
    assert report.diagnostics["runtime_window"]["effective_trading_start_at"] == "2026-01-01T04:00:00+00:00"
    assert report.diagnostics["entry_hold_reasons"]["trigger_close_not_strong_enough"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["context_pass_count"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["impulse_candidate_count"] >= 1
    assert report.diagnostics["strategy_specific_counters"]["pullback_candidate_count"] >= 1


def test_trend_reclaim_72h_paper_flow_handles_entry_then_fast_failure() -> None:
    engine = PaperEngine()
    strategy = TrendReclaim72hStrategy()
    config = _entry_config(target_r_multiple=3.0, fast_failure_bars=2, max_bars_in_trade=8)
    candles = _entry_history() + [
        _candle(datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc), "113.3", "113.5", "112.8", "113.2"),
        _candle(datetime(2026, 1, 1, 13, 0, tzinfo=timezone.utc), "113.2", "113.3", "112.7", "113.0"),
    ]

    final_state, results = engine.process_candle_batch(
        strategy=strategy,
        symbol="BTC-USDT",
        timeframe="1h",
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
