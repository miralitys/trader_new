from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_query_service
from app.schemas.api import StrategyRunDetailResponse, StrategyRunSummaryResponse
from app.services.query_service import QueryService

router = APIRouter(prefix="/strategy-runs", tags=["strategy-runs"])


@router.get("", response_model=list[StrategyRunSummaryResponse], summary="List strategy runs")
def list_strategy_runs(
    strategy_code: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    mode: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[StrategyRunSummaryResponse]:
    return service.list_strategy_runs(
        strategy_code=strategy_code,
        status=status,
        mode=mode,
        limit=limit,
    )


@router.get("/{run_id}", response_model=StrategyRunDetailResponse, summary="Get strategy run details")
def get_strategy_run(
    run_id: int,
    service: QueryService = Depends(get_query_service),
) -> StrategyRunDetailResponse:
    return service.get_strategy_run(run_id)
