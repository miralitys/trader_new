from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from app.engines.base import EngineBase
from app.models.enums import Side
from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategyConfig

ZERO = Decimal("0")


@dataclass(frozen=True)
class RiskPlan:
    position_size_pct: Decimal
    stop_loss_pct: Decimal
    take_profit_pct: Decimal


@dataclass(frozen=True)
class EntryPlan:
    side: str
    qty: Decimal
    fill_price: Decimal
    fee_paid: Decimal
    slippage_paid: Decimal
    notional_value: Decimal
    capital_committed: Decimal
    stop_price: Optional[Decimal]
    take_profit_price: Optional[Decimal]


@dataclass(frozen=True)
class EntryDecision:
    plan: Optional[EntryPlan]
    reject_reason: Optional[str] = None


@dataclass(frozen=True)
class ExitPlan:
    side: str
    fill_price: Decimal
    fee_paid: Decimal
    slippage_paid: Decimal
    reason: str


class RiskEngine(EngineBase):
    engine_name = "risk_engine"
    purpose = "Position sizing, stop rules, exposure policies, and order validation."

    def validate_order(self, strategy_key: str, symbol: str, side: str = Side.LONG.value) -> dict[str, str | bool]:
        payload = self.describe()
        payload.update(
            {
                "strategy_key": strategy_key,
                "symbol": symbol,
                "side": side,
                "approved": True,
                "reason": "validated_for_simulated_directional_backtest",
            }
        )
        return payload

    def build_risk_plan(self, strategy_config: BaseStrategyConfig) -> RiskPlan:
        return RiskPlan(
            position_size_pct=Decimal(str(strategy_config.position_size_pct)),
            stop_loss_pct=Decimal(str(strategy_config.stop_loss_pct)),
            take_profit_pct=Decimal(str(strategy_config.take_profit_pct)),
        )

    def calculate_entry_plan(
        self,
        available_cash: Decimal,
        reference_price: Decimal,
        fee_rate: Decimal,
        slippage_rate: Decimal,
        risk_plan: RiskPlan,
        side: str = Side.LONG.value,
        override_stop_price: Optional[Decimal] = None,
        override_take_profit_price: Optional[Decimal] = None,
    ) -> Optional[EntryPlan]:
        return self.calculate_entry_decision(
            available_cash=available_cash,
            reference_price=reference_price,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            risk_plan=risk_plan,
            side=side,
            override_stop_price=override_stop_price,
            override_take_profit_price=override_take_profit_price,
        ).plan

    def calculate_entry_decision(
        self,
        available_cash: Decimal,
        reference_price: Decimal,
        fee_rate: Decimal,
        slippage_rate: Decimal,
        risk_plan: RiskPlan,
        side: str = Side.LONG.value,
        override_stop_price: Optional[Decimal] = None,
        override_take_profit_price: Optional[Decimal] = None,
    ) -> EntryDecision:
        if available_cash <= ZERO or reference_price <= ZERO:
            return EntryDecision(plan=None, reject_reason="risk_rejected")

        capital_budget = available_cash * risk_plan.position_size_pct
        if capital_budget <= ZERO:
            return EntryDecision(plan=None, reject_reason="position_size_zero")

        normalized_side = side if side in {Side.LONG.value, Side.SHORT.value} else Side.LONG.value
        fill_price = (
            reference_price * (Decimal("1") + slippage_rate)
            if normalized_side == Side.LONG.value
            else reference_price * (Decimal("1") - slippage_rate)
        )
        gross_qty = capital_budget / (fill_price * (Decimal("1") + fee_rate))
        if gross_qty <= ZERO:
            return EntryDecision(plan=None, reject_reason="position_size_zero")

        fee_paid = gross_qty * fill_price * fee_rate
        notional_value = gross_qty * fill_price
        capital_committed = notional_value + fee_paid
        slippage_paid = abs(fill_price - reference_price) * gross_qty
        stop_price = None
        take_profit_price = None

        if override_stop_price is not None:
            if normalized_side == Side.LONG.value:
                if override_stop_price <= ZERO or override_stop_price >= fill_price:
                    return EntryDecision(plan=None, reject_reason="missing_stop")
            else:
                if override_stop_price <= fill_price:
                    return EntryDecision(plan=None, reject_reason="missing_stop")
            stop_price = override_stop_price
        elif risk_plan.stop_loss_pct > ZERO:
            stop_price = (
                fill_price * (Decimal("1") - risk_plan.stop_loss_pct)
                if normalized_side == Side.LONG.value
                else fill_price * (Decimal("1") + risk_plan.stop_loss_pct)
            )
        if override_take_profit_price is not None:
            if normalized_side == Side.LONG.value:
                if override_take_profit_price <= fill_price:
                    return EntryDecision(plan=None, reject_reason="missing_take_profit")
            else:
                if override_take_profit_price >= fill_price or override_take_profit_price <= ZERO:
                    return EntryDecision(plan=None, reject_reason="missing_take_profit")
            take_profit_price = override_take_profit_price
        elif risk_plan.take_profit_pct > ZERO:
            take_profit_price = (
                fill_price * (Decimal("1") + risk_plan.take_profit_pct)
                if normalized_side == Side.LONG.value
                else fill_price * (Decimal("1") - risk_plan.take_profit_pct)
            )

        return EntryDecision(
            plan=EntryPlan(
                side=normalized_side,
                qty=gross_qty,
                fill_price=fill_price,
                fee_paid=fee_paid,
                slippage_paid=slippage_paid,
                notional_value=notional_value,
                capital_committed=capital_committed,
                stop_price=stop_price,
                take_profit_price=take_profit_price,
            ),
        )

    def evaluate_intrabar_exit(
        self,
        candle: BacktestCandle,
        side: str,
        qty: Decimal,
        stop_price: Optional[Decimal],
        take_profit_price: Optional[Decimal],
        fee_rate: Decimal,
        slippage_rate: Decimal,
    ) -> Optional[ExitPlan]:
        normalized_side = side if side in {Side.LONG.value, Side.SHORT.value} else Side.LONG.value
        if normalized_side == Side.LONG.value:
            if stop_price is not None and candle.low <= stop_price:
                return self._build_exit_plan(
                    reference_price=stop_price,
                    qty=qty,
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                    reason="stop_loss",
                    side=normalized_side,
                )
            if take_profit_price is not None and candle.high >= take_profit_price:
                return self._build_exit_plan(
                    reference_price=take_profit_price,
                    qty=qty,
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                    reason="take_profit",
                    side=normalized_side,
                )
            return None

        if stop_price is not None and candle.high >= stop_price:
            return self._build_exit_plan(
                reference_price=stop_price,
                qty=qty,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                reason="stop_loss",
                side=normalized_side,
            )
        if take_profit_price is not None and candle.low <= take_profit_price:
            return self._build_exit_plan(
                reference_price=take_profit_price,
                qty=qty,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                reason="take_profit",
                side=normalized_side,
            )
        return None

    def build_market_exit(
        self,
        reference_price: Decimal,
        side: str,
        qty: Decimal,
        fee_rate: Decimal,
        slippage_rate: Decimal,
        reason: str,
    ) -> ExitPlan:
        return self._build_exit_plan(
            reference_price=reference_price,
            qty=qty,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            reason=reason,
            side=side,
        )

    def _build_exit_plan(
        self,
        reference_price: Decimal,
        qty: Decimal,
        fee_rate: Decimal,
        slippage_rate: Decimal,
        reason: str,
        side: str,
    ) -> ExitPlan:
        normalized_side = side if side in {Side.LONG.value, Side.SHORT.value} else Side.LONG.value
        fill_price = (
            reference_price * (Decimal("1") - slippage_rate)
            if normalized_side == Side.LONG.value
            else reference_price * (Decimal("1") + slippage_rate)
        )
        fee_paid = qty * fill_price * fee_rate
        slippage_paid = abs(reference_price - fill_price) * qty
        return ExitPlan(
            side=normalized_side,
            fill_price=fill_price,
            fee_paid=fee_paid,
            slippage_paid=slippage_paid,
            reason=reason,
        )
