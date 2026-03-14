from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.domain.schemas.log import LogRead
from app.repositories.trading import TradingRepository

router = APIRouter(prefix="/logs")


@router.get("", response_model=list[LogRead])
def list_logs(
    strategy_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[LogRead]:
    return TradingRepository(db).list_logs(strategy_id=strategy_id, limit=300)
