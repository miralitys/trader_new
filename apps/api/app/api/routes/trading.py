from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_query_service
from app.schemas.api import PositionResponse, SignalResponse, TradeResponse
from app.services.query_service import QueryService

router = APIRouter(tags=["trading"])


@router.get("/signals", response_model=list[SignalResponse], summary="List strategy signals")
def list_signals(
    strategy_run_id: Optional[int] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    timeframe: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[SignalResponse]:
    return service.list_signals(
        strategy_run_id=strategy_run_id,
        symbol=symbol,
        timeframe=timeframe,
        limit=limit,
    )


@router.get("/trades", response_model=list[TradeResponse], summary="List simulated trades")
def list_trades(
    strategy_run_id: Optional[int] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[TradeResponse]:
    return service.list_trades(
        strategy_run_id=strategy_run_id,
        symbol=symbol,
        limit=limit,
    )


@router.get("/positions", response_model=list[PositionResponse], summary="List simulated positions")
def list_positions(
    strategy_run_id: Optional[int] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[PositionResponse]:
    return service.list_positions(
        strategy_run_id=strategy_run_id,
        symbol=symbol,
        status=status,
        limit=limit,
    )
