from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable, Optional, Sequence

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


@dataclass
class OpenPosition:
    entry_time: datetime
    entry_price: Decimal
    qty: Decimal
    entry_fee: Decimal
    entry_slippage: Decimal
    capital_committed: Decimal
    stop_price: Optional[Decimal]
    take_profit_price: Optional[Decimal]
    entry_metadata: dict[str, object]


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
    ) -> BacktestResponse:
        if not strategy.long_only or not strategy.spot_only:
            raise ValueError("BacktestEngine currently supports LONG-only SPOT-only strategies only")

        ordered_candles = sorted(candles, key=lambda candle: candle.open_time)
        if not ordered_candles:
            raise ValueError("BacktestEngine requires at least one candle")

        config_payload = strategy.default_config()
        config_payload.update(request.strategy_config_override)
        config_payload["position_size_pct"] = float(request.position_size_pct)
        strategy_config = strategy.parse_config(config_payload)
        risk_plan = self.risk_engine.build_risk_plan(strategy_config)

        fee_rate = Decimal(str(request.fee))
        slippage_rate = Decimal(str(request.slippage))
        cash = Decimal(str(request.initial_capital))
        position: Optional[OpenPosition] = None
        equity_curve: list[EquityPoint] = []
        closed_trades: list[BacktestTrade] = []

        for bar_index, candle in enumerate(ordered_candles):
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

            signal = strategy.generate_signal(
                StrategyContext(
                    symbol=request.symbol,
                    timeframe=request.timeframe,
                    timestamp=candle.open_time,
                    mode="backtest",
                    metadata={
                        "bar_index": bar_index,
                        "current_candle": candle,
                        "history": ordered_candles[: bar_index + 1],
                        "has_position": position is not None,
                        "position": self._position_snapshot(position),
                        "config": strategy_config,
                        "cash": cash,
                        "fee_rate": fee_rate,
                        "slippage_rate": slippage_rate,
                    },
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
        entry_plan: EntryPlan,
        cash: Decimal,
        entry_metadata: dict[str, object],
    ) -> tuple[OpenPosition, Decimal]:
        position = OpenPosition(
            entry_time=candle_time,
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

    def _position_snapshot(self, position: Optional[OpenPosition]) -> Optional[dict[str, Decimal | datetime]]:
        if position is None:
            return None
        return {
            "entry_time": position.entry_time,
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
