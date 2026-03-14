from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, status

from app.api.dependencies import get_strategy_service
from app.schemas.api import (
    StrategyConfigResponse,
    StrategyConfigUpdateRequest,
    StrategyDetailResponse,
    StrategyPaperStartRequest,
    StrategyPaperStopRequest,
    StrategySummaryResponse,
)
from app.schemas.paper import PaperRunResponse
from app.services.strategy_service import StrategyService

router = APIRouter(prefix="/strategies", tags=["strategies"])


@router.get("", response_model=list[StrategySummaryResponse], summary="List registered strategies")
def list_strategies(
    service: StrategyService = Depends(get_strategy_service),
) -> list[StrategySummaryResponse]:
    return service.list_strategies()


@router.get("/{code}", response_model=StrategyDetailResponse, summary="Get strategy details")
def get_strategy(
    code: str,
    service: StrategyService = Depends(get_strategy_service),
) -> StrategyDetailResponse:
    return service.get_strategy(code)


@router.get(
    "/{code}/config",
    response_model=StrategyConfigResponse,
    summary="Get effective strategy config",
)
def get_strategy_config(
    code: str,
    service: StrategyService = Depends(get_strategy_service),
) -> StrategyConfigResponse:
    return service.get_strategy_config(code)


@router.put(
    "/{code}/config",
    response_model=StrategyConfigResponse,
    summary="Create or update stored strategy config",
)
def update_strategy_config(
    code: str,
    request: StrategyConfigUpdateRequest,
    service: StrategyService = Depends(get_strategy_service),
) -> StrategyConfigResponse:
    return service.update_strategy_config(code, request)


@router.post(
    "/{code}/start-paper",
    response_model=PaperRunResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Start paper trading for a strategy",
)
def start_paper_run(
    code: str,
    request: StrategyPaperStartRequest,
    service: StrategyService = Depends(get_strategy_service),
) -> PaperRunResponse:
    return service.start_paper_run(code, request)


@router.post(
    "/{code}/stop-paper",
    response_model=PaperRunResponse,
    summary="Stop active paper trading for a strategy",
)
def stop_paper_run(
    code: str,
    request: Optional[StrategyPaperStopRequest] = None,
    service: StrategyService = Depends(get_strategy_service),
) -> PaperRunResponse:
    return service.stop_paper_run(code, request.reason if request is not None else "manual_stop")
