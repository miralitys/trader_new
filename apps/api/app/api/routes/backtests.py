from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_backtest_runner_service, get_query_service
from app.api.errors import BadRequestError, NotFoundError
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
    try:
        return service.run_backtest(request)
    except KeyError as exc:
        raise NotFoundError(f"Strategy {request.strategy_code} was not found") from exc
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc


@router.get("", response_model=list[BacktestListItemResponse], summary="List backtest runs")
def list_backtests(
    strategy_code: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[BacktestListItemResponse]:
    return service.list_backtests(
        strategy_code=strategy_code,
        status=status,
        limit=limit,
    )


@router.get("/{run_id}", response_model=BacktestResponse, summary="Get backtest details")
def get_backtest(
    run_id: int,
    service: QueryService = Depends(get_query_service),
) -> BacktestResponse:
    return service.get_backtest(run_id)


@router.post("/{run_id}/stop", response_model=BacktestResponse, summary="Stop backtest run")
def stop_backtest(
    run_id: int,
    request: BacktestStopRequest | None = None,
    service: BacktestRunnerService = Depends(get_backtest_runner_service),
) -> BacktestResponse:
    try:
        return service.stop_backtest(run_id, request.reason if request is not None else "manual_stop")
    except KeyError as exc:
        raise NotFoundError(str(exc)) from exc
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc


@router.post("/delete", response_model=BacktestDeleteResponse, summary="Delete backtest runs")
def delete_backtests(
    request: BacktestDeleteRequest,
    service: BacktestRunnerService = Depends(get_backtest_runner_service),
) -> BacktestDeleteResponse:
    try:
        return service.delete_backtests(request.run_ids)
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc
