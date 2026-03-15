from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.registry import get_strategy, list_strategies
from app.strategies.rsi_micro_bounce import RSIMicroBounceConfig, RSIMicroBounceStrategy


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


def _config(**overrides: object) -> RSIMicroBounceConfig:
    payload: dict[str, object] = {
        "position_size_pct": 1,
        "rsi_period": 3,
        "rsi_oversold_threshold": 25,
        "oversold_lookback_bars": 2,
        "atr_period": 3,
        "stop_atr_buffer": 0,
        "stop_lookback_bars": 3,
        "max_stop_pct": 0.1,
        "cost_multiplier": 1,
        "soft_context_filter_enabled": False,
        "max_bars_in_trade": 3,
    }
    payload.update(overrides)
    return RSIMicroBounceConfig(**payload)


def _entry_history() -> list[BacktestCandle]:
    return _history(
        ("105", "106", "104", "105"),
        ("103", "104", "102", "103"),
        ("101", "102", "100", "101"),
        ("100", "101", "99", "100"),
        ("97", "97.5", "96", "97"),
        ("94", "94.5", "93", "94"),
        ("92.6", "93", "91.8", "92"),
        ("92.2", "94.8", "91.6", "94.4"),
    )


def test_rsi_micro_bounce_strategy_is_registered() -> None:
    strategy = get_strategy("rsi_micro_bounce")

    assert strategy.key == "rsi_micro_bounce"
    assert "rsi_micro_bounce" in {item.key for item in list_strategies()}


def test_rsi_micro_bounce_default_config_matches_research_baseline() -> None:
    strategy = RSIMicroBounceStrategy()
    config = strategy.parse_config()

    assert config.timeframes == ["5m"]
    assert config.rsi_period == 7
    assert config.rsi_oversold_threshold == 20
    assert config.entry_mode == "first_uptick"
    assert config.stop_mode == "signal_low"
    assert config.target_mode == "stop_multiple"
    assert config.target_r_multiple == 0.5
    assert config.max_bars_in_trade == 6


def test_rsi_micro_bounce_enters_on_first_uptick_after_oversold() -> None:
    strategy = RSIMicroBounceStrategy()
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
    assert signal.reason == "rsi_micro_bounce_entry"
    assert signal.metadata["entry_mode"] == "first_uptick"
    assert signal.metadata["setup_type"] == "rsi_only"
    assert Decimal(str(signal.metadata["stop_price"])) < Decimal(str(signal.metadata["current_close"]))
    assert Decimal(str(signal.metadata["take_profit_price"])) > Decimal(str(signal.metadata["current_close"]))


def test_rsi_micro_bounce_supports_wick_rejection_entry_mode() -> None:
    strategy = RSIMicroBounceStrategy()
    history = _history(
        ("105", "106", "104", "105"),
        ("103", "104", "102", "103"),
        ("101", "102", "100", "101"),
        ("100", "100.5", "99", "100"),
        ("97", "97.5", "96", "97"),
        ("94", "94.5", "93", "94"),
        ("92.6", "93", "91.8", "92"),
        ("92.2", "93.2", "90.8", "92.9"),
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
                "config": _config(entry_mode="wick_rejection"),
                "fee_rate": Decimal("0"),
                "slippage_rate": Decimal("0"),
            },
        )
    )

    assert signal.action == "enter"
    assert signal.metadata["entry_mode"] == "wick_rejection"


def test_rsi_micro_bounce_backtest_engine_closes_with_time_stop() -> None:
    strategy = RSIMicroBounceStrategy()
    candles = _entry_history()
    next_open_time = candles[-1].open_time
    candles.extend(
        [
            _candle(next_open_time + timedelta(minutes=5), "94.4", "94.6", "94.0", "94.2"),
            _candle(next_open_time + timedelta(minutes=10), "94.2", "94.3", "93.9", "94.0"),
            _candle(next_open_time + timedelta(minutes=15), "94.0", "94.1", "93.8", "93.95"),
        ]
    )
    request = BacktestRequest(
        strategy_code=strategy.key,
        symbol="BTC-USD",
        timeframe="5m",
        start_at=candles[0].open_time,
        end_at=candles[-1].open_time,
        initial_capital=Decimal("1000"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        position_size_pct=Decimal("1"),
        strategy_config_override=_config(max_bars_in_trade=2).model_dump(),
    )

    result = BacktestEngine().run(request, strategy, candles)

    assert result.metrics.total_trades == 1
    assert result.trades[0].metadata["exit_reason_label"] == "time_stop"


def test_rsi_micro_bounce_paper_engine_emits_skipped_hold_signal_for_cost_filter() -> None:
    strategy = RSIMicroBounceStrategy()
    history = _entry_history()
    candle = history[-1]

    result = PaperEngine().process_candle(
        strategy=strategy,
        symbol="BTC-USD",
        timeframe="5m",
        candle=candle,
        history=history,
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0.01"),
        slippage_rate=Decimal("0.01"),
        strategy_config_override=_config(target_r_multiple=0.2, cost_multiplier=2).model_dump(),
    )

    assert result.signal_event is not None
    assert result.signal_event.signal_type == "hold"
    assert result.signal_event.payload_json["metadata"]["reason_skipped"] == "insufficient_tp_vs_cost"
