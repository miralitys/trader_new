from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Sequence

from app.engines.base import EngineBase
from app.engines.risk_engine import EntryPlan, ExitPlan, RiskEngine
from app.schemas.backtest import BacktestCandle, BacktestTrade
from app.strategies.base import BaseStrategy, StrategyContext

ZERO = Decimal("0")


@dataclass(frozen=True)
class PaperPositionState:
    entry_time: datetime
    entry_price: Decimal
    qty: Decimal
    entry_fee: Decimal
    entry_slippage: Decimal
    capital_committed: Decimal
    stop_price: Optional[Decimal]
    take_profit_price: Optional[Decimal]


@dataclass(frozen=True)
class PaperRuntimeState:
    cash: Decimal
    position: Optional[PaperPositionState]


@dataclass(frozen=True)
class PaperSignalEvent:
    signal_type: str
    signal_strength: float
    payload_json: dict[str, object]
    candle_time: datetime


@dataclass(frozen=True)
class PaperOrderEvent:
    qty: Decimal
    price: Decimal
    linked_to_signal: bool


@dataclass(frozen=True)
class PaperTradeEvent:
    entry_price: Decimal
    exit_price: Decimal
    qty: Decimal
    pnl: Decimal
    pnl_pct: Decimal
    fees: Decimal
    slippage: Decimal
    opened_at: datetime
    closed_at: datetime
    metadata_json: dict[str, object]


@dataclass(frozen=True)
class PaperCandleResult:
    state: PaperRuntimeState
    signal_event: Optional[PaperSignalEvent]
    orders: list[PaperOrderEvent]
    trade_event: Optional[PaperTradeEvent]


class PaperEngine(EngineBase):
    engine_name = "paper_engine"
    purpose = "Simulated execution against streaming or recently ingested market data."

    def __init__(self, risk_engine: Optional[RiskEngine] = None) -> None:
        self.risk_engine = risk_engine or RiskEngine()

    def process_cycle(self, strategy_key: str) -> dict[str, str | bool]:
        payload = self.describe()
        payload.update({"strategy_key": strategy_key, "cycle_state": "idle"})
        return payload

    def process_candle(
        self,
        strategy: BaseStrategy,
        symbol: str,
        timeframe: str,
        candle: BacktestCandle,
        history: Sequence[BacktestCandle],
        state: PaperRuntimeState,
        fee_rate: Decimal,
        slippage_rate: Decimal,
        strategy_config_override: dict[str, Any],
        runtime_metadata: Optional[dict[str, Any]] = None,
    ) -> PaperCandleResult:
        if not strategy.long_only or not strategy.spot_only:
            raise ValueError("PaperEngine currently supports LONG-only SPOT-only strategies only")

        config_payload = strategy.default_config()
        config_payload.update(strategy_config_override)
        strategy_config = strategy.parse_config(config_payload)
        risk_plan = self.risk_engine.build_risk_plan(strategy_config)

        current_state = state
        orders: list[PaperOrderEvent] = []
        trade_event: Optional[PaperTradeEvent] = None

        if current_state.position is not None:
            exit_plan = self.risk_engine.evaluate_intrabar_exit(
                candle=candle,
                qty=current_state.position.qty,
                stop_price=current_state.position.stop_price,
                take_profit_price=current_state.position.take_profit_price,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            if exit_plan is not None:
                current_state, trade_event = self._close_position(
                    state=current_state,
                    position=current_state.position,
                    exit_plan=exit_plan,
                    exit_time=candle.open_time,
                )
                orders.append(
                    PaperOrderEvent(
                        qty=trade_event.qty,
                        price=trade_event.exit_price,
                        linked_to_signal=False,
                    )
                )

        signal = strategy.generate_signal(
            StrategyContext(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=candle.open_time,
                mode="paper",
                metadata={
                    "current_candle": candle,
                    "history": list(history),
                    "has_position": current_state.position is not None,
                    "position": self._position_snapshot(current_state.position),
                    "cash": current_state.cash,
                    "config": strategy_config,
                    **(runtime_metadata or {}),
                },
            )
        )

        signal_event: Optional[PaperSignalEvent] = None
        if signal.action != "hold":
            signal_type = "enter" if signal.action == "enter" else "exit"
            signal_event = PaperSignalEvent(
                signal_type=signal_type,
                signal_strength=signal.confidence,
                payload_json={
                    "reason": signal.reason,
                    "metadata": signal.metadata,
                    "action": signal.action,
                    "side": signal.side,
                },
                candle_time=candle.open_time,
            )

        if current_state.position is None and signal.action == "enter":
            validation = self.risk_engine.validate_order(strategy.key, symbol)
            if validation.get("approved"):
                entry_plan = self.risk_engine.calculate_entry_plan(
                    available_cash=current_state.cash,
                    reference_price=Decimal(str(candle.close)),
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                    risk_plan=risk_plan,
                )
                if entry_plan is not None:
                    current_state = self._open_position(
                        state=current_state,
                        candle_time=candle.open_time,
                        entry_plan=entry_plan,
                    )
                    orders.append(
                        PaperOrderEvent(
                            qty=entry_plan.qty,
                            price=entry_plan.fill_price,
                            linked_to_signal=True,
                        )
                    )
        elif current_state.position is not None and signal.action == "exit":
            exit_plan = self.risk_engine.build_market_exit(
                reference_price=Decimal(str(candle.close)),
                qty=current_state.position.qty,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                reason=signal.reason or "signal_exit",
            )
            current_state, trade_event = self._close_position(
                state=current_state,
                position=current_state.position,
                exit_plan=exit_plan,
                exit_time=candle.open_time,
            )
            orders.append(
                PaperOrderEvent(
                    qty=trade_event.qty,
                    price=trade_event.exit_price,
                    linked_to_signal=True,
                )
            )

        return PaperCandleResult(
            state=current_state,
            signal_event=signal_event,
            orders=orders,
            trade_event=trade_event,
        )

    def process_candle_batch(
        self,
        strategy: BaseStrategy,
        symbol: str,
        timeframe: str,
        candles: Sequence[BacktestCandle],
        state: PaperRuntimeState,
        fee_rate: Decimal,
        slippage_rate: Decimal,
        strategy_config_override: dict[str, Any],
        runtime_metadata: Optional[dict[str, Any]] = None,
    ) -> tuple[PaperRuntimeState, list[PaperCandleResult]]:
        history: list[BacktestCandle] = []
        current_state = state
        results: list[PaperCandleResult] = []
        for candle in candles:
            history.append(candle)
            result = self.process_candle(
                strategy=strategy,
                symbol=symbol,
                timeframe=timeframe,
                candle=candle,
                history=history,
                state=current_state,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                strategy_config_override=strategy_config_override,
                runtime_metadata=runtime_metadata,
            )
            current_state = result.state
            results.append(result)
        return current_state, results

    def _open_position(
        self,
        state: PaperRuntimeState,
        candle_time: datetime,
        entry_plan: EntryPlan,
    ) -> PaperRuntimeState:
        return PaperRuntimeState(
            cash=state.cash - entry_plan.capital_committed,
            position=PaperPositionState(
                entry_time=candle_time,
                entry_price=entry_plan.fill_price,
                qty=entry_plan.qty,
                entry_fee=entry_plan.fee_paid,
                entry_slippage=entry_plan.slippage_paid,
                capital_committed=entry_plan.capital_committed,
                stop_price=entry_plan.stop_price,
                take_profit_price=entry_plan.take_profit_price,
            ),
        )

    def _close_position(
        self,
        state: PaperRuntimeState,
        position: PaperPositionState,
        exit_plan: ExitPlan,
        exit_time: datetime,
    ) -> tuple[PaperRuntimeState, PaperTradeEvent]:
        proceeds = position.qty * exit_plan.fill_price
        updated_cash = state.cash + proceeds - exit_plan.fee_paid
        total_fees = position.entry_fee + exit_plan.fee_paid
        total_slippage = position.entry_slippage + exit_plan.slippage_paid
        pnl = proceeds - exit_plan.fee_paid - position.capital_committed
        pnl_pct = ZERO
        if position.capital_committed > ZERO:
            pnl_pct = (pnl / position.capital_committed) * Decimal("100")

        next_state = PaperRuntimeState(cash=updated_cash, position=None)
        trade_event = PaperTradeEvent(
            entry_price=position.entry_price,
            exit_price=exit_plan.fill_price,
            qty=position.qty,
            pnl=pnl,
            pnl_pct=pnl_pct,
            fees=total_fees,
            slippage=total_slippage,
            opened_at=position.entry_time,
            closed_at=exit_time,
            metadata_json={"exit_reason": exit_plan.reason},
        )
        return next_state, trade_event

    def _position_snapshot(
        self,
        position: Optional[PaperPositionState],
    ) -> Optional[dict[str, Decimal | datetime]]:
        if position is None:
            return None
        return {
            "entry_time": position.entry_time,
            "entry_price": position.entry_price,
            "qty": position.qty,
            "stop_price": position.stop_price,
            "take_profit_price": position.take_profit_price,
        }
