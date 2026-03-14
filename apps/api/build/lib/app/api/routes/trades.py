from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.domain.schemas.strategy import TradeRead
from app.repositories.trading import TradingRepository

router = APIRouter(prefix="/trades")


@router.get("", response_model=list[TradeRead])
def list_trades(
    strategy_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[TradeRead]:
    return TradingRepository(db).list_trades(strategy_id=strategy_id, limit=500)
