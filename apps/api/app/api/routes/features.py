from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_feature_layer_service
from app.schemas.api import FeatureCoverageResponse, FeatureRunRequest, FeatureRunResponse
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
    service: FeatureLayerService = Depends(get_feature_layer_service),
) -> list[FeatureCoverageResponse]:
    return service.get_symbol_timeframe_coverages(
        exchange_code=exchange_code,
        symbols=list(supported_symbol_codes()),
        timeframes=["1m", "5m", "15m", "1h", "4h"],
    )
