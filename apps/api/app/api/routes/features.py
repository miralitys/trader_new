from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_feature_layer_service
from app.schemas.api import (
    FeatureCoverageResponse,
    FeatureFailedRunsClearResponse,
    FeatureRunRequest,
    FeatureRunResponse,
    FeatureWorkspaceResetResponse,
)
from app.services.feature_layer_service import FeatureLayerService
from app.utils.symbols import supported_symbol_codes

router = APIRouter(prefix="/features", tags=["features"])


@router.post("/run", response_model=FeatureRunResponse, status_code=status.HTTP_201_CREATED, summary="Queue feature generation")
def run_feature_layer(
    request: FeatureRunRequest,
    service: FeatureLayerService = Depends(get_feature_layer_service),
) -> FeatureRunResponse:
    return service.create_run(request)


@router.get("/runs", response_model=list[FeatureRunResponse], summary="List feature generation history")
def list_feature_runs(
    symbol: Optional[str] = Query(default=None),
    timeframe: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    service: FeatureLayerService = Depends(get_feature_layer_service),
) -> list[FeatureRunResponse]:
    return service.list_runs(symbol=symbol, timeframe=timeframe, limit=limit)


@router.get("/coverage", response_model=list[FeatureCoverageResponse], summary="List feature coverage by symbol/timeframe")
def list_feature_coverages(
    exchange_code: str = Query(default="binance_us"),
    symbol: Optional[str] = Query(default=None),
    service: FeatureLayerService = Depends(get_feature_layer_service),
) -> list[FeatureCoverageResponse]:
    return service.get_symbol_timeframe_coverages(
        exchange_code=exchange_code,
        symbols=[symbol] if symbol else list(supported_symbol_codes()),
        timeframes=["1m", "5m", "15m", "1h", "4h"],
    )


@router.post("/reset", response_model=FeatureWorkspaceResetResponse, summary="Reset feature workspace")
def reset_feature_workspace(
    service: FeatureLayerService = Depends(get_feature_layer_service),
) -> FeatureWorkspaceResetResponse:
    return FeatureWorkspaceResetResponse(**service.reset_workspace())


@router.post("/clear-failed", response_model=FeatureFailedRunsClearResponse, summary="Clear failed feature runs")
def clear_failed_feature_runs(
    service: FeatureLayerService = Depends(get_feature_layer_service),
) -> FeatureFailedRunsClearResponse:
    return FeatureFailedRunsClearResponse(**service.clear_failed_runs())
