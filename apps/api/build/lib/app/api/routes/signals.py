from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.domain.schemas.strategy import SignalRead
from app.repositories.trading import TradingRepository

router = APIRouter(prefix="/signals")


@router.get("", response_model=list[SignalRead])
def list_signals(
    strategy_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[SignalRead]:
    return TradingRepository(db).list_signals(strategy_id=strategy_id, limit=200)
