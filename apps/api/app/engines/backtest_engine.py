from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable, Iterator, Optional, Sequence

from app.engines.base import EngineBase
from app.engines.performance_engine import PerformanceEngine
from app.engines.risk_engine import EntryPlan, ExitPlan, RiskEngine
from app.schemas.backtest import (
    BacktestCandle,
    BacktestRequest,
    BacktestResponse,
    BacktestTrade,
    EquityPoint,
)
from app.strategies.base import BaseStrategy, StrategyContext
from app.utils.time import utc_now

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
HUNDRED = Decimal("100")


@dataclass
class OpenPosition:
    entry_time: datetime
    entry_bar_index: int
    entry_price: Decimal
    qty: Decimal
    entry_fee: Decimal
    entry_slippage: Decimal
    capital_committed: Decimal
    stop_price: Optional[Decimal]
    take_profit_price: Optional[Decimal]
    entry_metadata: dict[str, object]


class HistoryWindow(Sequence[BacktestCandle]):
    def __init__(self, source: Sequence[BacktestCandle], start: int, stop: int) -> None:
        self._source = source
        self._start = max(0, start)
        self._stop = max(self._start, stop)

    def __len__(self) -> int:
        return self._stop - self._start

    def __getitem__(self, item: int | slice) -> BacktestCandle | list[BacktestCandle]:
        if isinstance(item, slice):
            start, stop, step = item.indices(len(self))
            return self._source[self._start + start : self._start + stop : step]

        index = item
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError("history window index out of range")
        return self._source[self._start + index]

    def __iter__(self) -> Iterator[BacktestCandle]:
        for index in range(self._start, self._stop):
            yield self._source[index]


class BacktestEngine(EngineBase):
    engine_name = "backtest_engine"
    purpose = "Historical simulation, reproducible runs, and result persistence."

    def __init__(
        self,
        performance_engine: Optional[PerformanceEngine] = None,
        risk_engine: Optional[RiskEngine] = None,
    ) -> None:
        self.performance_engine = performance_engine or PerformanceEngine()
        self.risk_engine = risk_engine or RiskEngine()

    def prepare_run(self, strategy_key: str, dataset_name: str) -> dict[str, str | bool]:
        payload = self.describe()
        payload.update(
            {
                "strategy_key": strategy_key,
                "dataset_name": dataset_name,
                "run_state": "queued",
            }
        )
        return payload

    def run(
        self,
        request: BacktestRequest,
        strategy: BaseStrategy,
        candles: Sequence[BacktestCandle],
        started_at: Optional[datetime] = None,
        completed_at: Optional[Callable[[], datetime] | datetime] = None,
        progress_interval_bars: int = 0,
        progress_callback: Optional[Callable[[int, int, datetime], None]] = None,
    ) -> BacktestResponse:
        if not strategy.long_only or not strategy.spot_only:
            raise ValueError("BacktestEngine currently supports LONG-only SPOT-only strategies only")

        ordered_candles = sorted(candles, key=lambda candle: candle.open_time)
        if not ordered_candles:
            raise ValueError("BacktestEngine requires at least one candle")
        ordered_closes = [Decimal(str(candle.close)) for candle in ordered_candles]

        config_payload = strategy.default_config()
        config_payload.update(request.strategy_config_override)
        config_payload["position_size_pct"] = float(request.position_size_pct)
        strategy_config = strategy.parse_config(config_payload)
        risk_plan = self.risk_engine.build_risk_plan(strategy_config)
        history_window_bars = self._history_window_bars(
            strategy=strategy,
            timeframe=request.timeframe,
            strategy_config=strategy_config,
            total_bars=len(ordered_candles),
        )

        fee_rate = Decimal(str(request.fee))
        slippage_rate = Decimal(str(request.slippage))
        cash = Decimal(str(request.initial_capital))
        position: Optional[OpenPosition] = None
        equity_curve: list[EquityPoint] = []
        closed_trades: list[BacktestTrade] = []
        one_hour_history: list[BacktestCandle] = []
        one_hour_closes: list[Decimal] = []
        total_bars = len(ordered_candles)
        use_mean_reversion_runtime_cache = strategy.key == "mean_reversion_hard_stop"
        rsi_period = int(getattr(strategy_config, "rsi_period", 0) or 0)
        rsi_avg_gain: Optional[Decimal] = None
        rsi_avg_loss: Optional[Decimal] = None
        rsi_seed_gains: list[Decimal] = []
        rsi_seed_losses: list[Decimal] = []
        rsi_tail = deque(maxlen=max(2, int(getattr(strategy_config, "oversold_lookback_bars", 0) or 0) + 2))
        exit_ema_value: Optional[Decimal] = None
        exit_ema_period = int(getattr(strategy_config, "exit_ema_period", 0) or 0)
        regime_ema_period = int(getattr(strategy_config, "regime_ema_period", 0) or 0)
        regime_atr_period = int(getattr(strategy_config, "regime_atr_period", 0) or 0)
        htf_rsi_period = int(getattr(strategy_config, "htf_rsi_period", 0) or 0)
        downside_lookback = int(getattr(strategy_config, "downside_volatility_lookback", 0) or 0)
        regime_tail_size = max(0, (downside_lookback * 2) + 1)
        completed_one_hour_close: Optional[Decimal] = None
        completed_one_hour_ema: Optional[Decimal] = None
        completed_one_hour_avg_gain: Optional[Decimal] = None
        completed_one_hour_avg_loss: Optional[Decimal] = None
        one_hour_seed_gains: list[Decimal] = []
        one_hour_seed_losses: list[Decimal] = []
        completed_one_hour_true_ranges = deque(maxlen=max(1, regime_atr_period))

        for bar_index, candle in enumerate(ordered_candles):
            previous_one_hour_bucket = one_hour_history[-1] if one_hour_history else None
            self._append_one_hour_candle(
                one_hour_history=one_hour_history,
                one_hour_closes=one_hour_closes,
                candle=candle,
                timeframe=request.timeframe,
            )
            if use_mean_reversion_runtime_cache and previous_one_hour_bucket is not None and (
                previous_one_hour_bucket.open_time != one_hour_history[-1].open_time
            ):
                completed_close = Decimal(str(previous_one_hour_bucket.close))
                previous_completed_close = completed_one_hour_close
                completed_one_hour_ema = self._next_ema_value(
                    previous_ema=completed_one_hour_ema,
                    current_value=completed_close,
                    period=regime_ema_period,
                )
                _, completed_one_hour_avg_gain, completed_one_hour_avg_loss = self._next_rsi_value(
                    period=htf_rsi_period,
                    current_close=completed_close,
                    previous_close=previous_completed_close,
                    seed_gains=one_hour_seed_gains,
                    seed_losses=one_hour_seed_losses,
                    avg_gain=completed_one_hour_avg_gain,
                    avg_loss=completed_one_hour_avg_loss,
                )
                if previous_completed_close is not None and regime_atr_period > 0:
                    completed_one_hour_true_ranges.append(
                        self._true_range(previous_completed_close, previous_one_hour_bucket)
                    )
                completed_one_hour_close = completed_close
            if use_mean_reversion_runtime_cache:
                current_close = ordered_closes[bar_index]
                previous_close = ordered_closes[bar_index - 1] if bar_index > 0 else None
                next_rsi, rsi_avg_gain, rsi_avg_loss = self._next_rsi_value(
                    period=rsi_period,
                    current_close=current_close,
                    previous_close=previous_close,
                    seed_gains=rsi_seed_gains,
                    seed_losses=rsi_seed_losses,
                    avg_gain=rsi_avg_gain,
                    avg_loss=rsi_avg_loss,
                )
                rsi_tail.append(next_rsi)
                exit_ema_value = self._next_ema_value(
                    previous_ema=exit_ema_value,
                    current_value=current_close,
                    period=exit_ema_period,
                )
            if position is not None:
                exit_plan = self.risk_engine.evaluate_intrabar_exit(
                    candle=candle,
                    qty=position.qty,
                    stop_price=position.stop_price,
                    take_profit_price=position.take_profit_price,
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                )
                if exit_plan is not None:
                    cash, trade = self._close_position(
                        position=position,
                        exit_plan=exit_plan,
                        cash=cash,
                        exit_time=candle.open_time,
                    )
                    closed_trades.append(trade)
                    position = None

            history_start = max(0, bar_index + 1 - history_window_bars)
            history_window = HistoryWindow(ordered_candles, history_start, bar_index + 1)
            metadata = {
                "bar_index": bar_index,
                "bars_seen": bar_index + 1,
                "current_candle": candle,
                "history": history_window,
                "closes": HistoryWindow(ordered_closes, history_start, bar_index + 1),
                "one_hour_history": HistoryWindow(one_hour_history, 0, len(one_hour_history)),
                "one_hour_closes": HistoryWindow(one_hour_closes, 0, len(one_hour_closes)),
                "has_position": position is not None,
                "position": self._position_snapshot(position),
                "config": strategy_config,
                "cash": cash,
                "fee_rate": fee_rate,
                "slippage_rate": slippage_rate,
            }
            if use_mean_reversion_runtime_cache:
                current_one_hour_close = one_hour_closes[-1] if one_hour_closes else None
                current_one_hour_ema = (
                    self._next_ema_value(completed_one_hour_ema, current_one_hour_close, regime_ema_period)
                    if current_one_hour_close is not None
                    else None
                )
                current_one_hour_rsi: Optional[Decimal] = None
                if current_one_hour_close is not None:
                    current_one_hour_rsi, _, _ = self._next_rsi_value(
                        period=htf_rsi_period,
                        current_close=current_one_hour_close,
                        previous_close=completed_one_hour_close,
                        seed_gains=list(one_hour_seed_gains),
                        seed_losses=list(one_hour_seed_losses),
                        avg_gain=completed_one_hour_avg_gain,
                        avg_loss=completed_one_hour_avg_loss,
                    )
                current_one_hour_atr = self._current_atr_value(
                    previous_completed_close=completed_one_hour_close,
                    current_candle=one_hour_history[-1] if one_hour_history else None,
                    completed_true_ranges=completed_one_hour_true_ranges,
                    period=regime_atr_period,
                )
                metadata["rsi_series_tail"] = tuple(rsi_tail)
                metadata["exit_ema_value"] = exit_ema_value
                metadata["regime_snapshot"] = {
                    "one_hour_bars": len(one_hour_history),
                    "regime_close_1h": current_one_hour_close,
                    "regime_ema_1h": current_one_hour_ema,
                    "regime_previous_ema_1h": completed_one_hour_ema,
                    "regime_atr_pct_1h": (
                        current_one_hour_atr / current_one_hour_close
                        if current_one_hour_atr is not None and current_one_hour_close is not None and current_one_hour_close > ZERO
                        else None
                    ),
                    "regime_rsi_1h": current_one_hour_rsi,
                    "regime_closes_tail": tuple(one_hour_closes[-regime_tail_size:]) if regime_tail_size > 0 else tuple(),
                }
            signal = strategy.generate_signal(
                StrategyContext(
                    symbol=request.symbol,
                    timeframe=request.timeframe,
                    timestamp=candle.open_time,
                    mode="backtest",
                    metadata=metadata,
                )
            )

            if position is not None and signal.action == "exit":
                exit_plan = self.risk_engine.build_market_exit(
                    reference_price=candle.close,
                    qty=position.qty,
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                    reason=signal.reason or "signal_exit",
                )
                cash, trade = self._close_position(
                    position=position,
                    exit_plan=exit_plan,
                    cash=cash,
                    exit_time=candle.open_time,
                )
                closed_trades.append(trade)
                position = None
            elif position is None and signal.action == "enter":
                stop_price = self._metadata_decimal(signal.metadata, "stop_price")
                take_profit_price = self._metadata_decimal(signal.metadata, "take_profit_price")
                entry_plan = self.risk_engine.calculate_entry_plan(
                    available_cash=cash,
                    reference_price=candle.close,
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                    risk_plan=risk_plan,
                    override_stop_price=stop_price,
                    override_take_profit_price=take_profit_price,
                )
                if entry_plan is not None:
                    position, cash = self._open_position(
                        candle_time=candle.open_time,
                        entry_bar_index=bar_index,
                        entry_plan=entry_plan,
                        cash=cash,
                        entry_metadata=signal.metadata,
                    )

            equity_curve.append(
                EquityPoint(
                    timestamp=candle.open_time,
                    equity=self._mark_to_market(cash=cash, position=position, close_price=candle.close),
                    cash=cash,
                    close_price=candle.close,
                    position_qty=position.qty if position is not None else ZERO,
                )
            )
            if progress_callback is not None and progress_interval_bars > 0:
                processed_bars = bar_index + 1
                if processed_bars % progress_interval_bars == 0 or processed_bars == total_bars:
                    progress_callback(processed_bars, total_bars, candle.open_time)

        if position is not None:
            final_candle = ordered_candles[-1]
            exit_plan = self.risk_engine.build_market_exit(
                reference_price=final_candle.close,
                qty=position.qty,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                reason="end_of_data",
            )
            cash, trade = self._close_position(
                position=position,
                exit_plan=exit_plan,
                cash=cash,
                exit_time=final_candle.open_time,
            )
            closed_trades.append(trade)
            equity_curve[-1] = EquityPoint(
                timestamp=final_candle.open_time,
                equity=cash,
                cash=cash,
                close_price=final_candle.close,
                position_qty=ZERO,
            )

        final_equity = equity_curve[-1].equity if equity_curve else cash
        metrics = self.performance_engine.calculate_metrics(
            trades=closed_trades,
            equity_curve=equity_curve,
            initial_capital=Decimal(str(request.initial_capital)),
            final_equity=final_equity,
        )

        completed_timestamp = (
            completed_at()
            if callable(completed_at)
            else completed_at
            if completed_at is not None
            else utc_now()
        )
        return BacktestResponse(
            strategy_code=request.strategy_code,
            symbol=request.symbol,
            timeframe=request.timeframe,
            exchange_code=request.exchange_code,
            status="completed",
            initial_capital=Decimal(str(request.initial_capital)),
            final_equity=final_equity,
            started_at=started_at or utc_now(),
            completed_at=completed_timestamp,
            params=request.model_dump(mode="json"),
            metrics=metrics,
            equity_curve=equity_curve,
            trades=closed_trades,
        )

    def _open_position(
        self,
        candle_time: datetime,
        entry_bar_index: int,
        entry_plan: EntryPlan,
        cash: Decimal,
        entry_metadata: dict[str, object],
    ) -> tuple[OpenPosition, Decimal]:
        position = OpenPosition(
            entry_time=candle_time,
            entry_bar_index=entry_bar_index,
            entry_price=entry_plan.fill_price,
            qty=entry_plan.qty,
            entry_fee=entry_plan.fee_paid,
            entry_slippage=entry_plan.slippage_paid,
            capital_committed=entry_plan.capital_committed,
            stop_price=entry_plan.stop_price,
            take_profit_price=entry_plan.take_profit_price,
            entry_metadata=dict(entry_metadata),
        )
        return position, cash - entry_plan.capital_committed

    def _close_position(
        self,
        position: OpenPosition,
        exit_plan: ExitPlan,
        cash: Decimal,
        exit_time: datetime,
    ) -> tuple[Decimal, BacktestTrade]:
        proceeds = position.qty * exit_plan.fill_price
        updated_cash = cash + proceeds - exit_plan.fee_paid
        gross_pnl = (exit_plan.fill_price - position.entry_price) * position.qty
        total_fees = position.entry_fee + exit_plan.fee_paid
        total_slippage = position.entry_slippage + exit_plan.slippage_paid
        pnl = proceeds - exit_plan.fee_paid - position.capital_committed
        pnl_pct = ZERO
        if position.capital_committed > ZERO:
            pnl_pct = (pnl / position.capital_committed) * Decimal("100")

        trade = BacktestTrade(
            entry_time=position.entry_time,
            exit_time=exit_time,
            entry_price=position.entry_price,
            exit_price=exit_plan.fill_price,
            qty=position.qty,
            gross_pnl=gross_pnl,
            pnl=pnl,
            pnl_pct=pnl_pct,
            fees=total_fees,
            slippage=total_slippage,
            exit_reason=exit_plan.reason,
            metadata={
                "entry": position.entry_metadata,
                "exit_reason_label": self._normalized_exit_reason(exit_plan.reason),
            },
        )
        return updated_cash, trade

    def _mark_to_market(
        self,
        cash: Decimal,
        position: Optional[OpenPosition],
        close_price: Decimal,
    ) -> Decimal:
        if position is None:
            return cash
        return cash + (position.qty * close_price)

    def _position_snapshot(
        self,
        position: Optional[OpenPosition],
    ) -> Optional[dict[str, Decimal | datetime | int | dict[str, object] | None]]:
        if position is None:
            return None
        return {
            "entry_time": position.entry_time,
            "entry_bar_index": position.entry_bar_index,
            "entry_price": position.entry_price,
            "qty": position.qty,
            "stop_price": position.stop_price,
            "take_profit_price": position.take_profit_price,
            "entry_metadata": position.entry_metadata,
        }

    def _metadata_decimal(self, metadata: dict[str, object], key: str) -> Optional[Decimal]:
        value = metadata.get(key)
        if value is None:
            return None
        return Decimal(str(value))

    def _normalized_exit_reason(self, reason: str) -> str:
        mapping = {
            "take_profit": "tp",
            "stop_loss": "stop",
        }
        return mapping.get(reason, reason)

    def _history_window_bars(
        self,
        strategy: BaseStrategy,
        timeframe: str,
        strategy_config: object,
        total_bars: int,
    ) -> int:
        if strategy.key == "mean_reversion_hard_stop":
            oversold_lookback = int(getattr(strategy_config, "oversold_lookback_bars", 0) or 0)
            lookback_period = int(getattr(strategy_config, "lookback_period", 0) or 0)
            rsi_period = int(getattr(strategy_config, "rsi_period", 0) or 0)
            atr_period = int(getattr(strategy_config, "atr_period", 0) or 0)
            stop_lookback_bars = int(getattr(strategy_config, "stop_lookback_bars", 0) or 0)
            buffered_window = max(
                lookback_period + oversold_lookback + 1,
                rsi_period + oversold_lookback + 1,
                atr_period + oversold_lookback + 1,
                stop_lookback_bars,
                2,
            )
            return min(total_bars, buffered_window)

        required_history = total_bars
        required_history_fn = getattr(strategy, "required_history_bars", None)
        if callable(required_history_fn):
            required_history = int(required_history_fn(timeframe, strategy_config))

        max_bars_in_trade = int(getattr(strategy_config, "max_bars_in_trade", 0) or 0)
        buffered_window = max(required_history, max_bars_in_trade + 2, 2)
        return min(total_bars, buffered_window)

    def _append_one_hour_candle(
        self,
        one_hour_history: list[BacktestCandle],
        one_hour_closes: list[Decimal],
        candle: BacktestCandle,
        timeframe: str,
    ) -> None:
        if timeframe == "1h":
            one_hour_history.append(candle)
            one_hour_closes.append(Decimal(str(candle.close)))
            return

        bucket_time = candle.open_time.replace(minute=0, second=0, microsecond=0)
        open_price = Decimal(str(candle.open))
        high_price = Decimal(str(candle.high))
        low_price = Decimal(str(candle.low))
        close_price = Decimal(str(candle.close))
        volume = Decimal(str(candle.volume))
        if not one_hour_history or one_hour_history[-1].open_time != bucket_time:
            one_hour_history.append(
                BacktestCandle(
                    open_time=bucket_time,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )
            )
            one_hour_closes.append(close_price)
            return

        previous = one_hour_history[-1]
        one_hour_history[-1] = BacktestCandle(
            open_time=previous.open_time,
            open=previous.open,
            high=max(previous.high, high_price),
            low=min(previous.low, low_price),
            close=close_price,
            volume=previous.volume + volume,
        )
        one_hour_closes[-1] = close_price

    def _next_rsi_value(
        self,
        period: int,
        current_close: Decimal,
        previous_close: Optional[Decimal],
        seed_gains: list[Decimal],
        seed_losses: list[Decimal],
        avg_gain: Optional[Decimal],
        avg_loss: Optional[Decimal],
    ) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        if period <= 0 or previous_close is None:
            return None, avg_gain, avg_loss

        delta = current_close - previous_close
        gain = max(delta, ZERO)
        loss = abs(min(delta, ZERO))
        if len(seed_gains) < period:
            seed_gains.append(gain)
            seed_losses.append(loss)
            if len(seed_gains) < period:
                return None, avg_gain, avg_loss
            avg_gain = sum(seed_gains, ZERO) / Decimal(period)
            avg_loss = sum(seed_losses, ZERO) / Decimal(period)
            return self._rsi_from_average_moves(avg_gain, avg_loss), avg_gain, avg_loss

        next_avg_gain = ((avg_gain or ZERO) * Decimal(period - 1) + gain) / Decimal(period)
        next_avg_loss = ((avg_loss or ZERO) * Decimal(period - 1) + loss) / Decimal(period)
        return self._rsi_from_average_moves(next_avg_gain, next_avg_loss), next_avg_gain, next_avg_loss

    def _next_ema_value(
        self,
        previous_ema: Optional[Decimal],
        current_value: Decimal,
        period: int,
    ) -> Decimal:
        if period <= 0 or previous_ema is None:
            return current_value
        alpha = TWO / (Decimal(period) + ONE)
        return previous_ema + (current_value - previous_ema) * alpha

    def _rsi_from_average_moves(self, avg_gain: Decimal, avg_loss: Decimal) -> Decimal:
        if avg_gain <= ZERO and avg_loss <= ZERO:
            return Decimal("50")
        if avg_loss <= ZERO:
            return HUNDRED
        relative_strength = avg_gain / avg_loss
        return HUNDRED - (HUNDRED / (ONE + relative_strength))

    def _true_range(self, previous_close: Decimal, candle: BacktestCandle) -> Decimal:
        high = Decimal(str(candle.high))
        low = Decimal(str(candle.low))
        return max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )

    def _current_atr_value(
        self,
        previous_completed_close: Optional[Decimal],
        current_candle: Optional[BacktestCandle],
        completed_true_ranges: deque[Decimal],
        period: int,
    ) -> Optional[Decimal]:
        if period <= 0 or previous_completed_close is None or current_candle is None:
            return None
        current_true_range = self._true_range(previous_completed_close, current_candle)
        relevant_true_ranges = list(completed_true_ranges)
        if len(relevant_true_ranges) + 1 < period:
            return None
        atr_window = (relevant_true_ranges + [current_true_range])[-period:]
        return sum(atr_window, ZERO) / Decimal(len(atr_window))
