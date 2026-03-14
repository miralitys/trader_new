from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.domain.schemas.strategy import PositionRead
from app.repositories.trading import TradingRepository

router = APIRouter(prefix="/positions")


@router.get("", response_model=list[PositionRead])
def list_positions(
    strategy_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[PositionRead]:
    return TradingRepository(db).list_positions(strategy_id=strategy_id, status=status, limit=500)
