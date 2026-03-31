from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import func, select

from app.models import Position
from app.models.enums import PositionStatus, Side
from app.repositories.base import BaseRepository


class PositionRepository(BaseRepository):
    def get_open_position(self, strategy_run_id: int, symbol: str) -> Position | None:
        stmt = select(Position).where(
            Position.strategy_run_id == strategy_run_id,
            Position.symbol == symbol,
            Position.status == PositionStatus.OPEN,
        )
        return self.session.scalar(stmt)

    def list_open_positions(self, strategy_run_id: int) -> list[Position]:
        stmt = select(Position).where(
            Position.strategy_run_id == strategy_run_id,
            Position.status == PositionStatus.OPEN,
        )
        return list(self.session.scalars(stmt))

    def list_positions(
        self,
        strategy_run_id: Optional[int] = None,
        symbol: Optional[str] = None,
        status: Optional[PositionStatus] = None,
        limit: int = 100,
    ) -> list[Position]:
        stmt = select(Position).order_by(Position.opened_at.desc(), Position.id.desc()).limit(limit)
        if strategy_run_id is not None:
            stmt = stmt.where(Position.strategy_run_id == strategy_run_id)
        if symbol:
            stmt = stmt.where(Position.symbol == symbol)
        if status is not None:
            stmt = stmt.where(Position.status == status)
        return list(self.session.scalars(stmt))

    def count_open_positions(self, strategy_run_id: int) -> int:
        stmt = select(func.count(Position.id)).where(
            Position.strategy_run_id == strategy_run_id,
            Position.status == PositionStatus.OPEN,
        )
        return int(self.session.scalar(stmt) or 0)

    def count_all_open_positions(self) -> int:
        stmt = select(func.count(Position.id)).where(Position.status == PositionStatus.OPEN)
        return int(self.session.scalar(stmt) or 0)

    def open_position(
        self,
        strategy_run_id: int,
        symbol: str,
        qty: Decimal,
        avg_entry_price: Decimal,
        stop_price: Decimal | None,
        take_profit_price: Decimal | None,
        opened_at: datetime,
        side: Side = Side.LONG,
    ) -> Position:
        position = Position(
            strategy_run_id=strategy_run_id,
            symbol=symbol,
            side=side,
            qty=qty,
            avg_entry_price=avg_entry_price,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            status=PositionStatus.OPEN,
            opened_at=opened_at,
        )
        self.session.add(position)
        self.session.flush()
        return position

    def close_position(self, position: Position, closed_at: datetime) -> Position:
        position.status = PositionStatus.CLOSED
        position.closed_at = closed_at
        self.session.add(position)
        self.session.flush()
        return position
