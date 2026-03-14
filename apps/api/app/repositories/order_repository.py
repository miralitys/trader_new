from __future__ import annotations

from decimal import Decimal
from typing import Optional

from app.models import Order
from app.models.enums import OrderStatus, OrderType, Side
from app.repositories.base import BaseRepository


class OrderRepository(BaseRepository):
    def create_filled_order(
        self,
        strategy_run_id: int,
        symbol: str,
        qty: Decimal,
        price: Decimal,
        linked_signal_id: Optional[int] = None,
    ) -> Order:
        order = Order(
            strategy_run_id=strategy_run_id,
            symbol=symbol,
            side=Side.LONG,
            order_type=OrderType.SIMULATED,
            qty=qty,
            price=price,
            status=OrderStatus.FILLED,
            linked_signal_id=linked_signal_id,
        )
        self.session.add(order)
        self.session.flush()
        return order
