from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_market_data_service, get_query_service
from app.api.errors import BadRequestError
from app.schemas.api import CandleResponse, DataSyncRequest, DataSyncResponse, SyncJobResponse
from app.services.market_data_service import MarketDataService
from app.services.query_service import QueryService

router = APIRouter(tags=["data"])


@router.post("/data/sync", response_model=DataSyncResponse, status_code=status.HTTP_201_CREATED, summary="Run data sync")
def run_data_sync(
    request: DataSyncRequest,
    service: MarketDataService = Depends(get_market_data_service),
) -> DataSyncResponse:
    try:
        if request.mode == "initial":
            result = service.initial_historical_sync(
                exchange_code=request.exchange_code,
                symbol=request.symbol,
                timeframe=request.timeframe,
                start_at=request.start_at,
                end_at=request.end_at,
            )
        elif request.mode == "incremental":
            result = service.incremental_sync(
                exchange_code=request.exchange_code,
                symbol=request.symbol,
                timeframe=request.timeframe,
                end_at=request.end_at,
            )
        else:
            result = service.manual_sync(
                exchange_code=request.exchange_code,
                symbol=request.symbol,
                timeframe=request.timeframe,
                start_at=request.start_at,
                end_at=request.end_at,
            )
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc

    return DataSyncResponse(
        job_id=result.job_id,
        exchange=result.exchange,
        symbol=result.symbol,
        timeframe=result.timeframe,
        start_at=result.start_at,
        end_at=result.end_at,
        fetched_rows=result.fetched_rows,
        normalized_rows=result.normalized_rows,
        inserted_rows=result.inserted_rows,
        status=result.status,
    )


@router.get("/data/status", response_model=list[SyncJobResponse], summary="List sync job status")
def list_sync_jobs(
    status: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    timeframe: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[SyncJobResponse]:
    return service.list_sync_jobs(
        status=status,
        symbol=symbol,
        timeframe=timeframe,
        limit=limit,
    )


@router.get("/candles", response_model=list[CandleResponse], summary="Query stored candles")
def list_candles(
    symbol: str = Query(...),
    timeframe: str = Query(...),
    start_at: datetime = Query(...),
    end_at: datetime = Query(...),
    exchange_code: str = Query(default="binance_us"),
    limit: Optional[int] = Query(default=None, ge=1, le=5000),
    service: QueryService = Depends(get_query_service),
) -> list[CandleResponse]:
    return service.list_candles(
        exchange_code=exchange_code,
        symbol=symbol,
        timeframe=timeframe,
        start_at=start_at,
        end_at=end_at,
        limit=limit,
    )
