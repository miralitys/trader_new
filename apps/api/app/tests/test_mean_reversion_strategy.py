from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.mean_reversion_hard_stop import (
    MeanReversionHardStopConfig,
    MeanReversionHardStopStrategy,
)


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
        _candle(start + timedelta(hours=index), open_price, high, low, close)
        for index, (open_price, high, low, close) in enumerate(candles)
    ]


def _config(**overrides: object) -> MeanReversionHardStopConfig:
    payload: dict[str, object] = {
        "lookback_period": 5,
        "bb_stddev_mult": 1.5,
        "oversold_lookback_bars": 1,
        "rsi_period": 3,
        "rsi_oversold_threshold": 20,
        "rsi_reclaim_threshold": 30,
        "atr_period": 3,
        "min_band_overshoot_atr": 0,
        "min_recovery_atr": 0.1,
        "exit_deviation_pct": 0,
        "min_bounce_pct": 0.01,
        "hard_stop_pct": 0.1,
        "stop_loss_pct": 0.1,
        "take_profit_pct": 0,
        "position_size_pct": 1,
        "exit_ema_period": 5,
        "stop_atr_buffer": 0,
        "stop_lookback_bars": 3,
        "min_hold_bars": 1,
        "max_bars_in_trade": 4,
        "regime_filter_enabled": True,
        "regime_ema_period": 2,
        "min_slope": -0.01,
        "regime_atr_period": 2,
        "atr_pct_max": 0.2,
        "use_htf_rsi_filter": False,
        "downside_volatility_filter_enabled": False,
    }
    payload.update(overrides)
    return MeanReversionHardStopConfig(**payload)


def _entry_history() -> list[BacktestCandle]:
    return _history(
        ("100", "101", "99", "100"),
        ("102", "103", "101", "102"),
        ("104", "105", "103", "104"),
        ("103", "104", "102", "103"),
        ("101", "102", "100", "101"),
        ("95", "96", "94", "95"),
        ("97", "98", "96.6", "97.8"),
    )


def _wide_stop_history() -> list[BacktestCandle]:
    return _history(
        ("100", "101", "99", "100"),
        ("102", "103", "101", "102"),
        ("104", "105", "103", "104"),
        ("103", "104", "102", "103"),
        ("101", "102", "100", "101"),
        ("95", "96", "94", "95"),
        ("96", "99", "95", "98"),
    )


def test_mean_reversion_strategy_enters_after_deep_oversold_bounce() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _entry_history()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "has_position": False,
                "position": None,
                "config": _config(),
                "fee_rate": Decimal("0.001"),
                "slippage_rate": Decimal("0.0005"),
            },
        )
    )

    assert signal.action == "enter"
    assert signal.reason == "oversold_reclaim_entry"
    assert signal.metadata["setup_type"] == "bb_and_rsi"
    assert Decimal(signal.metadata["stop_price"]) < Decimal(signal.metadata["current_close"])
    assert Decimal(signal.metadata["take_profit_price"]) > Decimal(signal.metadata["current_close"])
    assert signal.metadata["oversold_detection_mode"] == "rsi"
    assert signal.metadata["stop_mode_used"] == "signal_low"
    assert Decimal(signal.metadata["cost_multiplier"]) == Decimal("2.5")
    assert Decimal(signal.metadata["max_stop_pct"]) == Decimal("0.015")


def test_mean_reversion_strategy_enforces_runtime_guardrails_on_config() -> None:
    config = _config(
        bb_reentry_required=False,
        stop_mode="lookback_low",
        oversold_detection_mode="both",
        max_stop_pct=0.3,
        cost_multiplier=9,
    )

    assert config.bb_reentry_required is True
    assert config.stop_mode == "signal_low"
    assert config.oversold_detection_mode == "rsi"
    assert config.max_stop_pct == 0.015
    assert config.cost_multiplier == 2.5


def test_mean_reversion_strategy_allows_signal_relaxation_in_research_mode() -> None:
    config = _config(
        research_overrides_enabled=True,
        bb_reentry_required=False,
        oversold_detection_mode="either",
    )

    assert config.bb_reentry_required is False
    assert config.oversold_detection_mode == "either"


def test_mean_reversion_strategy_regime_flags_preserve_current_defaults() -> None:
    strategy = MeanReversionHardStopStrategy()
    config = strategy.parse_config()

    assert config.require_close_above_ema200_1h is True
    assert config.require_positive_slope_1h is True
    assert config.require_atr_band_1h is True
    assert config.require_htf_rsi is True
    assert config.require_downside_volatility_filter is True
    assert config.use_htf_rsi_filter is True
    assert config.downside_volatility_filter_enabled is True


def test_mean_reversion_strategy_reports_required_history_for_runtime_warmup() -> None:
    strategy = MeanReversionHardStopStrategy()
    config = strategy.parse_config()

    assert strategy.required_history_bars("5m", config) == 2424
    assert strategy.required_history_bars("15m", config) == 808
    assert strategy.required_history_bars("1h", config) == 202


def test_mean_reversion_strategy_can_disable_close_above_ema_subrule() -> None:
    strategy = MeanReversionHardStopStrategy()
    snapshot = {
        "one_hour_bars": 4,
        "regime_close_1h": "99",
        "regime_ema_1h": "100",
        "regime_previous_ema_1h": "99.5",
        "regime_atr_pct_1h": "0.01",
        "regime_rsi_1h": "55",
        "regime_closes_tail": ("100", "101", "101", "101"),
    }

    blocked, reason, _ = strategy._passes_regime_filter_from_snapshot(snapshot, _config(min_slope=0))
    relaxed, relaxed_reason, _ = strategy._passes_regime_filter_from_snapshot(
        snapshot,
        _config(min_slope=0, require_close_above_ema200_1h=False),
    )

    assert blocked is False
    assert reason == "close_below_ema200_1h"
    assert relaxed is True
    assert relaxed_reason == "regime_passed"


def test_mean_reversion_strategy_can_disable_positive_slope_subrule() -> None:
    strategy = MeanReversionHardStopStrategy()
    snapshot = {
        "one_hour_bars": 4,
        "regime_close_1h": "101",
        "regime_ema_1h": "100",
        "regime_previous_ema_1h": "100.2",
        "regime_atr_pct_1h": "0.01",
        "regime_rsi_1h": "55",
        "regime_closes_tail": ("100", "100.5", "100.8", "101"),
    }

    blocked, reason, _ = strategy._passes_regime_filter_from_snapshot(snapshot, _config(min_slope=0))
    relaxed, relaxed_reason, _ = strategy._passes_regime_filter_from_snapshot(
        snapshot,
        _config(min_slope=0, require_positive_slope_1h=False),
    )

    assert blocked is False
    assert reason == "ema200_slope_below_threshold"
    assert relaxed is True
    assert relaxed_reason == "regime_passed"


def test_mean_reversion_strategy_skips_trades_that_do_not_clear_costs() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _entry_history()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "has_position": False,
                "position": None,
                "config": _config(),
                "fee_rate": Decimal("0.01"),
                "slippage_rate": Decimal("0.01"),
            },
        )
    )

    assert signal.action == "hold"
    assert signal.reason == "insufficient_tp_vs_cost"


def test_mean_reversion_strategy_blocks_negative_regime() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _history(
        ("120", "121", "119", "120"),
        ("118", "119", "117", "118"),
        ("116", "117", "115", "116"),
        ("114", "115", "113", "114"),
        ("112", "113", "111", "112"),
        ("106", "107", "105", "106"),
        ("107", "110", "106", "109"),
    )

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "has_position": False,
                "position": None,
                "config": _config(use_htf_rsi_filter=True, htf_rsi_period=3, htf_rsi_min=50),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "hold"
    assert signal.reason == "regime_blocked"
    assert signal.metadata["skip_reason_detail"] == "htf_rsi_below_min"


def test_mean_reversion_strategy_skips_trade_when_stop_is_too_wide() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _wide_stop_history()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="1h",
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
    assert signal.reason == "max_stop_exceeded"
    assert signal.metadata["selected_stop_mode"] == "signal_low"


def test_mean_reversion_strategy_can_target_stop_multiple_in_research_mode() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _entry_history()

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="backtest",
            metadata={
                "history": history,
                "has_position": False,
                "position": None,
                "config": _config(
                    research_overrides_enabled=True,
                    regime_filter_enabled=False,
                    target_source="stop_multiple",
                    target_r_multiple=0.5,
                ),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "enter"
    entry_price = Decimal(signal.metadata["current_close"])
    stop_price = Decimal(signal.metadata["stop_price"])
    take_profit_price = Decimal(signal.metadata["take_profit_price"])
    expected_take_profit = entry_price + ((entry_price - stop_price) * Decimal("0.5"))

    assert take_profit_price == expected_take_profit


def test_mean_reversion_strategy_exits_when_price_reverts_to_mean_target() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _entry_history()
    history.append(_candle(history[-1].open_time + timedelta(hours=1), "100", "102", "99", "101"))
    entry_time = history[-2].open_time

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="paper",
            metadata={
                "history": history,
                "has_position": True,
                "position": {"entry_price": Decimal("98"), "entry_time": entry_time},
                "config": _config(),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "tp"


def test_mean_reversion_strategy_exits_on_ema_failure_after_min_hold() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _entry_history()
    history.append(_candle(history[-1].open_time + timedelta(hours=1), "99", "100", "98", "99"))
    history.append(_candle(history[-1].open_time + timedelta(hours=1), "98", "99", "97", "98"))

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="1h",
            timestamp=history[-1].open_time,
            mode="paper",
            metadata={
                "history": history,
                "has_position": True,
                "position": {"entry_price": Decimal("98"), "entry_time": history[6].open_time},
                "config": _config(exit_ema_period=3, regime_filter_enabled=False),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "ema_failure"


def test_mean_reversion_strategy_backtest_hits_dynamic_take_profit() -> None:
    strategy = MeanReversionHardStopStrategy()
    engine = BacktestEngine()
    candles = _entry_history()
    candles.append(_candle(candles[-1].open_time + timedelta(hours=1), "100", "102", "99", "101"))

    report = engine.run(
        request=BacktestRequest(
            strategy_code=strategy.key,
            symbol="BTC-USD",
            timeframe="1h",
            start_at=candles[0].open_time,
            end_at=candles[-1].open_time + timedelta(hours=1),
            initial_capital=Decimal("1000"),
            fee=Decimal("0"),
            slippage=Decimal("0"),
            position_size_pct=Decimal("1"),
            strategy_config_override=_config().model_dump(),
        ),
        strategy=strategy,
        candles=candles,
    )

    assert report.metrics.total_trades == 1
    assert report.trades[0].entry_price == Decimal("97.8")
    assert report.trades[0].exit_price == Decimal("100.160")
    assert report.trades[0].exit_reason == "take_profit"
    assert report.trades[0].metadata["entry"]["setup_type"] == "bb_and_rsi"
    assert report.trades[0].metadata["entry"]["oversold_detection_mode"] == "rsi"
    assert report.trades[0].metadata["exit_reason_label"] == "tp"
    assert report.final_equity == Decimal("1024.130879345603271983640081")


def test_mean_reversion_strategy_paper_engine_generates_trade_event() -> None:
    strategy = MeanReversionHardStopStrategy()
    engine = PaperEngine()
    candles = _entry_history()
    candles.append(_candle(candles[-1].open_time + timedelta(hours=1), "100", "102", "99", "101"))

    final_state, results = engine.process_candle_batch(
        strategy=strategy,
        symbol="BTC-USD",
        timeframe="1h",
        candles=candles,
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0"),
        slippage_rate=Decimal("0"),
        strategy_config_override=_config().model_dump(),
    )

    assert final_state.position is None
    assert final_state.cash == Decimal("1024.130879345603271983640081")
    assert any(result.signal_event is not None and result.signal_event.signal_type == "enter" for result in results)
    assert results[-1].trade_event is not None
    assert results[-1].trade_event.exit_price == Decimal("100.160")
    assert results[-1].trade_event.pnl == Decimal("24.1308793456032719836400813")
    assert results[-1].trade_event.metadata_json["entry"]["setup_type"] == "bb_and_rsi"
    assert results[-1].trade_event.metadata_json["entry"]["oversold_detection_mode"] == "rsi"
    assert results[-1].trade_event.metadata_json["exit_reason_label"] == "tp"
