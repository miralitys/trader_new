from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_query_service, get_strategy_service
from app.schemas.api import (
    StrategyConfigResponse,
    StrategyConfigUpdateRequest,
    StrategyDetailResponse,
    StrategyPaperStartRequest,
    StrategyPaperStopRequest,
    StrategyRunDetailResponse,
    StrategyRunSummaryResponse,
    StrategySummaryResponse,
)
from app.schemas.paper import PaperRunResponse
from app.services.query_service import QueryService
from app.services.strategy_service import StrategyService

router = APIRouter(prefix="/strategies", tags=["strategies"])


@router.get("", response_model=list[StrategySummaryResponse], status_code=status.HTTP_200_OK, summary="List registered strategies")
def list_strategies(service: StrategyService = Depends(get_strategy_service)) -> list[StrategySummaryResponse]:
    return service.list_strategies()


@router.get("/runs/list", response_model=list[StrategyRunSummaryResponse], status_code=status.HTTP_200_OK, summary="List strategy runs")
def list_strategy_runs(
    strategy_code: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    mode: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[StrategyRunSummaryResponse]:
    return service.list_strategy_runs(
        strategy_code=strategy_code,
        status=status_filter,
        mode=mode,
        limit=limit,
    )


@router.get("/runs/{run_id}", response_model=StrategyRunDetailResponse, status_code=status.HTTP_200_OK, summary="Get strategy run detail")
def get_strategy_run(run_id: int, service: QueryService = Depends(get_query_service)) -> StrategyRunDetailResponse:
    return service.get_strategy_run(run_id)


@router.get("/{code}", response_model=StrategyDetailResponse, status_code=status.HTTP_200_OK, summary="Get strategy detail")
def get_strategy(code: str, service: StrategyService = Depends(get_strategy_service)) -> StrategyDetailResponse:
    return service.get_strategy(code)


@router.get("/{code}/config", response_model=StrategyConfigResponse, status_code=status.HTTP_200_OK, summary="Get strategy config")
def get_strategy_config(code: str, service: StrategyService = Depends(get_strategy_service)) -> StrategyConfigResponse:
    return service.get_strategy_config(code)


@router.put("/{code}/config", response_model=StrategyConfigResponse, status_code=status.HTTP_200_OK, summary="Update strategy config")
def update_strategy_config(
    code: str,
    request: StrategyConfigUpdateRequest,
    service: StrategyService = Depends(get_strategy_service),
) -> StrategyConfigResponse:
    return service.update_strategy_config(code, request)


@router.post("/{code}/paper/start", response_model=PaperRunResponse, status_code=status.HTTP_202_ACCEPTED, summary="Start strategy paper run")
def start_strategy_paper_run(
    code: str,
    request: StrategyPaperStartRequest,
    service: StrategyService = Depends(get_strategy_service),
) -> PaperRunResponse:
    return service.start_paper_run(code, request)


@router.post("/{code}/paper/stop", response_model=PaperRunResponse, status_code=status.HTTP_200_OK, summary="Stop strategy paper run")
def stop_strategy_paper_run(
    code: str,
    request: StrategyPaperStopRequest,
    service: StrategyService = Depends(get_strategy_service),
) -> PaperRunResponse:
    return service.stop_paper_run(code, reason=request.reason)
