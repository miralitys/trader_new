from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from app.engines.base import EngineBase
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
    qty: Decimal
    fill_price: Decimal
    fee_paid: Decimal
    slippage_paid: Decimal
    capital_committed: Decimal
    stop_price: Optional[Decimal]
    take_profit_price: Optional[Decimal]


@dataclass(frozen=True)
class ExitPlan:
    fill_price: Decimal
    fee_paid: Decimal
    slippage_paid: Decimal
    reason: str


class RiskEngine(EngineBase):
    engine_name = "risk_engine"
    purpose = "Position sizing, stop rules, exposure policies, and order validation."

    def validate_order(self, strategy_key: str, symbol: str) -> dict[str, str | bool]:
        payload = self.describe()
        payload.update(
            {
                "strategy_key": strategy_key,
                "symbol": symbol,
                "approved": True,
                "reason": "validated_for_long_only_spot_backtest",
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
        override_stop_price: Optional[Decimal] = None,
        override_take_profit_price: Optional[Decimal] = None,
    ) -> Optional[EntryPlan]:
        if available_cash <= ZERO or reference_price <= ZERO:
            return None

        capital_budget = available_cash * risk_plan.position_size_pct
        if capital_budget <= ZERO:
            return None

        fill_price = reference_price * (Decimal("1") + slippage_rate)
        gross_qty = capital_budget / (fill_price * (Decimal("1") + fee_rate))
        if gross_qty <= ZERO:
            return None

        fee_paid = gross_qty * fill_price * fee_rate
        cost = gross_qty * fill_price
        capital_committed = cost + fee_paid
        slippage_paid = (fill_price - reference_price) * gross_qty
        stop_price = None
        take_profit_price = None

        if override_stop_price is not None:
            if override_stop_price <= ZERO or override_stop_price >= fill_price:
                return None
            stop_price = override_stop_price
        elif risk_plan.stop_loss_pct > ZERO:
            stop_price = fill_price * (Decimal("1") - risk_plan.stop_loss_pct)
        if override_take_profit_price is not None:
            if override_take_profit_price <= fill_price:
                return None
            take_profit_price = override_take_profit_price
        elif risk_plan.take_profit_pct > ZERO:
            take_profit_price = fill_price * (Decimal("1") + risk_plan.take_profit_pct)

        return EntryPlan(
            qty=gross_qty,
            fill_price=fill_price,
            fee_paid=fee_paid,
            slippage_paid=slippage_paid,
            capital_committed=capital_committed,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
        )

    def evaluate_intrabar_exit(
        self,
        candle: BacktestCandle,
        qty: Decimal,
        stop_price: Optional[Decimal],
        take_profit_price: Optional[Decimal],
        fee_rate: Decimal,
        slippage_rate: Decimal,
    ) -> Optional[ExitPlan]:
        if stop_price is not None and candle.low <= stop_price:
            return self._build_exit_plan(
                reference_price=stop_price,
                qty=qty,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                reason="stop_loss",
            )

        if take_profit_price is not None and candle.high >= take_profit_price:
            return self._build_exit_plan(
                reference_price=take_profit_price,
                qty=qty,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                reason="take_profit",
            )
        return None

    def build_market_exit(
        self,
        reference_price: Decimal,
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
        )

    def _build_exit_plan(
        self,
        reference_price: Decimal,
        qty: Decimal,
        fee_rate: Decimal,
        slippage_rate: Decimal,
        reason: str,
    ) -> ExitPlan:
        fill_price = reference_price * (Decimal("1") - slippage_rate)
        fee_paid = qty * fill_price * fee_rate
        slippage_paid = (reference_price - fill_price) * qty
        return ExitPlan(
            fill_price=fill_price,
            fee_paid=fee_paid,
            slippage_paid=slippage_paid,
            reason=reason,
        )
