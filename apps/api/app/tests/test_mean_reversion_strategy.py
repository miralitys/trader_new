from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.engines.backtest_engine import BacktestEngine
from app.engines.paper_engine import PaperEngine, PaperRuntimeState
from app.schemas.backtest import BacktestCandle, BacktestRequest
from app.strategies.base import StrategyContext
from app.strategies.mean_reversion_hard_stop import (
    MeanReversionHardStopConfig,
    MeanReversionHardStopStrategy,
)


def _candle(ts: datetime, price: str) -> BacktestCandle:
    decimal_price = Decimal(price)
    return BacktestCandle(
        open_time=ts,
        open=decimal_price,
        high=decimal_price,
        low=decimal_price,
        close=decimal_price,
        volume=Decimal("1"),
    )


def _history(*prices: str) -> list[BacktestCandle]:
    return [
        _candle(datetime(2026, 1, 1, 0, index * 5, tzinfo=timezone.utc), price)
        for index, price in enumerate(prices)
    ]


def _config() -> MeanReversionHardStopConfig:
    return MeanReversionHardStopConfig(
        lookback_period=3,
        entry_deviation_pct=0.02,
        exit_deviation_pct=0,
        min_bounce_pct=0.005,
        hard_stop_pct=0.25,
        take_profit_pct=0,
        position_size_pct=1,
    )


def test_mean_reversion_strategy_enters_after_oversold_bounce() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _history("100", "100", "95", "96")

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
            },
        )
    )

    assert signal.action == "enter"
    assert signal.reason == "oversold_bounce_entry"
    assert signal.confidence > 0


def test_mean_reversion_strategy_exits_when_price_reverts_to_mean() -> None:
    strategy = MeanReversionHardStopStrategy()
    history = _history("100", "98", "95", "99")

    signal = strategy.generate_signal(
        StrategyContext(
            symbol="BTC-USD",
            timeframe="5m",
            timestamp=history[-1].open_time,
            mode="paper",
            metadata={
                "history": history,
                "has_position": True,
                "position": {"entry_price": Decimal("96")},
                "config": _config(),
            },
        )
    )

    assert signal.action == "exit"
    assert signal.reason == "mean_reversion_complete"


def test_mean_reversion_strategy_backtest_generates_trade() -> None:
    strategy = MeanReversionHardStopStrategy()
    engine = BacktestEngine()
    candles = _history("100", "100", "95", "96", "99")

    report = engine.run(
        request=BacktestRequest(
            strategy_code=strategy.key,
            symbol="BTC-USD",
            timeframe="5m",
            start_at=candles[0].open_time,
            end_at=candles[-1].open_time,
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
    assert report.trades[0].entry_price == Decimal("96")
    assert report.trades[0].exit_price == Decimal("99")
    assert report.final_equity == Decimal("1031.250000000000000000000000")


def test_mean_reversion_strategy_paper_engine_generates_trade_event() -> None:
    strategy = MeanReversionHardStopStrategy()
    engine = PaperEngine()
    candles = _history("100", "100", "95", "96", "99")

    final_state, results = engine.process_candle_batch(
        strategy=strategy,
        symbol="BTC-USD",
        timeframe="5m",
        candles=candles,
        state=PaperRuntimeState(cash=Decimal("1000"), position=None),
        fee_rate=Decimal("0"),
        slippage_rate=Decimal("0"),
        strategy_config_override=_config().model_dump(),
    )

    assert final_state.position is None
    assert final_state.cash == Decimal("1031.250000000000000000000000")
    assert any(result.signal_event is not None and result.signal_event.signal_type == "enter" for result in results)
    assert results[-1].trade_event is not None
    assert results[-1].trade_event.pnl == Decimal("31.250000000000000000000000")
