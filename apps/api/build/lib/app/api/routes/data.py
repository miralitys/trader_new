from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.domain.schemas.data import DataStatusRead, DataSyncRequest
from app.services.market_data import MarketDataService

router = APIRouter(prefix="/data")


@router.post("/sync")
def sync_data(payload: DataSyncRequest, db: Session = Depends(get_db)) -> dict:
    try:
        jobs = MarketDataService(db).schedule_sync_jobs(
            symbols=payload.symbols,
            timeframes=payload.timeframes,
            start=payload.start,
            end=payload.end,
            full_resync=payload.full_resync,
        )
        return {"jobs": jobs}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/status", response_model=DataStatusRead)
def data_status(db: Session = Depends(get_db)) -> DataStatusRead:
    return MarketDataService(db).get_status()
