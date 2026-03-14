from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import select

from app.models import Trade
from app.repositories.base import BaseRepository


class TradeRepository(BaseRepository):
    def create_trade(
        self,
        strategy_run_id: int,
        symbol: str,
        entry_price: Decimal,
        exit_price: Decimal,
        qty: Decimal,
        pnl: Decimal,
        pnl_pct: Decimal,
        fees: Decimal,
        slippage: Decimal,
        opened_at: datetime,
        closed_at: datetime,
        metadata_json: dict[str, object],
    ) -> Trade:
        trade = Trade(
            strategy_run_id=strategy_run_id,
            symbol=symbol,
            entry_price=entry_price,
            exit_price=exit_price,
            qty=qty,
            pnl=pnl,
            pnl_pct=pnl_pct,
            fees=fees,
            slippage=slippage,
            opened_at=opened_at,
            closed_at=closed_at,
            metadata_json=metadata_json,
        )
        self.session.add(trade)
        self.session.flush()
        return trade

    def list_trades(
        self,
        strategy_run_id: Optional[int] = None,
        symbol: Optional[str] = None,
        limit: int = 100,
    ) -> list[Trade]:
        stmt = select(Trade).order_by(Trade.closed_at.desc(), Trade.id.desc()).limit(limit)
        if strategy_run_id is not None:
            stmt = stmt.where(Trade.strategy_run_id == strategy_run_id)
        if symbol:
            stmt = stmt.where(Trade.symbol == symbol)
        return list(self.session.scalars(stmt))
