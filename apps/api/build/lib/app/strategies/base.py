from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Type

from app.domain.enums import SignalAction
from app.strategies.types import BaseStrategyConfig, CandleInput, ExecutionResult, RiskDecision
from app.strategies.types import SignalDecision, StrategyContext


class BaseStrategy(ABC):
    key: str
    name: str
    description: str
    config_model: Type[BaseStrategyConfig] = BaseStrategyConfig

    def parse_config(self, config: dict) -> BaseStrategyConfig:
        return self.config_model(**config)

    def default_config(self) -> dict:
        return self.config_model().model_dump()

    def warmup_candles(self, config: BaseStrategyConfig) -> int:
        return config.warmup_candles

    @abstractmethod
    def generate_signal(self, candles: list[CandleInput], context: StrategyContext) -> SignalDecision:
        raise NotImplementedError

    def evaluate_risk(self, signal: SignalDecision, context: StrategyContext) -> RiskDecision:
        if signal.action == SignalAction.HOLD:
            return RiskDecision(approved=False, reason="no_action")

        if signal.action == SignalAction.ENTER and context.has_open_position:
            return RiskDecision(approved=False, reason="position_already_open")

        if signal.action == SignalAction.EXIT and not context.has_open_position:
            return RiskDecision(approved=False, reason="no_open_position")

        if signal.action == SignalAction.ENTER:
            position_value = context.cash * context.config.position_size_pct
            if position_value <= 0:
                return RiskDecision(approved=False, reason="position_value_zero")
            return RiskDecision(approved=True, position_value=position_value, reason="approved")

        notional = (context.position.quantity * candles_last_close(context)) if context.position else 0.0
        return RiskDecision(approved=True, position_value=notional, reason="approved")

    def simulate_execution(
        self,
        signal: SignalDecision,
        candle: CandleInput,
        context: StrategyContext,
        risk: RiskDecision,
    ) -> ExecutionResult:
        if signal.action == SignalAction.HOLD:
            return ExecutionResult(signal.action, candle.close, 0.0, 0.0, 0.0, 0.0)

        slippage_multiplier = context.config.slippage_bps / 10000
        fee_multiplier = context.config.fee_bps / 10000

        if signal.action == SignalAction.ENTER:
            execution_price = candle.close * (1 + slippage_multiplier)
            quantity = risk.position_value / execution_price if execution_price else 0.0
            notional = execution_price * quantity
            fee = notional * fee_multiplier
            return ExecutionResult(
                action=signal.action,
                price=execution_price,
                quantity=quantity,
                fee=fee,
                slippage=notional * slippage_multiplier,
                notional=notional,
                notes={"reason": signal.reason},
            )

        quantity = context.position.quantity if context.position else 0.0
        execution_price = candle.close * (1 - slippage_multiplier)
        notional = execution_price * quantity
        fee = notional * fee_multiplier
        return ExecutionResult(
            action=signal.action,
            price=execution_price,
            quantity=quantity,
            fee=fee,
            slippage=notional * slippage_multiplier,
            notional=notional,
            notes={"reason": signal.reason},
        )


def candles_last_close(context: StrategyContext) -> float:
    if context.position:
        return context.position.entry_price
    return 0.0
