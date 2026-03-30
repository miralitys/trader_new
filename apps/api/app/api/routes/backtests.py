from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_backtest_runner_service, get_query_service
from app.schemas.api import BacktestListItemResponse
from app.schemas.backtest import (
    BacktestDeleteRequest,
    BacktestDeleteResponse,
    BacktestRequest,
    BacktestResponse,
    BacktestStopRequest,
)
from app.services.backtest_runner_service import BacktestRunnerService
from app.services.query_service import QueryService

router = APIRouter(prefix="/backtests", tags=["backtests"])


@router.post("/run", response_model=BacktestResponse, status_code=status.HTTP_201_CREATED, summary="Run backtest")
def run_backtest(
    request: BacktestRequest,
    service: BacktestRunnerService = Depends(get_backtest_runner_service),
) -> BacktestResponse:
    return service.run_backtest(request)


@router.get("", response_model=list[BacktestListItemResponse], status_code=status.HTTP_200_OK, summary="List backtests")
def list_backtests(
    strategy_code: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[BacktestListItemResponse]:
    return service.list_backtests(
        strategy_code=strategy_code,
        status=status_filter,
        limit=limit,
    )


@router.get("/{run_id}", response_model=BacktestResponse, status_code=status.HTTP_200_OK, summary="Get backtest detail")
def get_backtest(run_id: int, service: QueryService = Depends(get_query_service)) -> BacktestResponse:
    return service.get_backtest(run_id)


@router.post("/{run_id}/stop", response_model=BacktestResponse, status_code=status.HTTP_200_OK, summary="Stop backtest")
def stop_backtest(
    run_id: int,
    request: BacktestStopRequest,
    service: BacktestRunnerService = Depends(get_backtest_runner_service),
) -> BacktestResponse:
    return service.stop_backtest(run_id, reason=request.reason)


@router.post("/delete", response_model=BacktestDeleteResponse, status_code=status.HTTP_200_OK, summary="Delete backtests")
def delete_backtests(
    request: BacktestDeleteRequest,
    service: BacktestRunnerService = Depends(get_backtest_runner_service),
) -> BacktestDeleteResponse:
    return service.delete_backtests(request.run_ids)
