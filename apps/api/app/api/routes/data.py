from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import (
    get_data_validation_service,
    get_market_data_service,
    get_query_service,
    get_validation_run_service,
)
from app.api.errors import BadRequestError
from app.schemas.api import (
    CandleCoverageResponse,
    CandleResponse,
    DataSyncRequest,
    DataSyncResponse,
    DataValidationReportResponse,
    DataValidationRequest,
    DataValidationSummaryResponse,
    DataValidationOverviewResponse,
    DataValidationResultResponse,
    DataValidationSymbolSummaryResponse,
    DataValidationTimeframeSummaryResponse,
    DataValidationOneMinuteSummaryResponse,
    SyncJobResponse,
    ValidationFailedRunsClearResponse,
    ValidationRunResponse,
)
from app.services.data_validation_service import DataValidationService, build_validation_report_payload
from app.services.market_data_service import MarketDataService
from app.services.query_service import QueryService
from app.services.validation_run_service import ValidationRunService

router = APIRouter(tags=["data"])


def _value(container: object, key: str):
    if isinstance(container, dict):
        return container[key]
    return getattr(container, key)


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
        coverage=(
            CandleCoverageResponse(
                exchange_code=result.coverage.exchange_code,
                symbol=result.coverage.symbol_code,
                timeframe=result.coverage.timeframe,
                requested_start_at=result.coverage.requested_start_at,
                requested_end_at=result.coverage.requested_end_at,
                loaded_start_at=result.coverage.actual_start_at,
                loaded_end_at=result.coverage.actual_end_at,
                candle_count=result.coverage.candle_count,
                expected_candle_count=result.coverage.expected_candle_count,
                missing_candle_count=result.coverage.missing_candle_count,
                completion_pct=result.coverage.completion_pct,
            )
            if result.coverage is not None
            else None
        ),
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


@router.get("/candles/coverage", response_model=CandleCoverageResponse, summary="Query stored candle coverage")
def get_candle_coverage(
    symbol: str = Query(...),
    timeframe: str = Query(...),
    start_at: datetime = Query(...),
    end_at: datetime = Query(...),
    exchange_code: str = Query(default="binance_us"),
    service: QueryService = Depends(get_query_service),
) -> CandleCoverageResponse:
    return service.get_candle_coverage(
        exchange_code=exchange_code,
        symbol=symbol,
        timeframe=timeframe,
        start_at=start_at,
        end_at=end_at,
    )


@router.post(
    "/data/validation-report",
    response_model=DataValidationReportResponse,
    status_code=status.HTTP_200_OK,
    summary="Run offline data validation report",
)
def run_data_validation(
    request: DataValidationRequest,
    service: DataValidationService = Depends(get_data_validation_service),
) -> DataValidationReportResponse:
    report = service.validate(
        exchange_code=request.exchange_code,
        symbols=request.symbols,
        timeframes=request.timeframes,
        lookback_days=request.lookback_days,
        perform_resync=request.perform_resync,
        resync_days=request.resync_days,
        sample_limit=request.sample_limit,
    )
    payload = build_validation_report_payload(report)
    return DataValidationReportResponse(
        summary=DataValidationSummaryResponse(**payload["summary"]),
        results=[DataValidationResultResponse(**row) for row in payload["results"]],
    )


@router.post(
    "/data/validation-report/start",
    response_model=ValidationRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start offline data validation report in background",
)
def start_data_validation(
    request: DataValidationRequest,
    service: ValidationRunService = Depends(get_validation_run_service),
) -> ValidationRunResponse:
    run = service.create_run(request)
    return run


@router.get(
    "/data/validation-report/runs",
    response_model=list[ValidationRunResponse],
    status_code=status.HTTP_200_OK,
    summary="List offline data validation runs",
)
def list_data_validation_runs(
    limit: int = Query(default=20, ge=1, le=200),
    service: ValidationRunService = Depends(get_validation_run_service),
) -> list[ValidationRunResponse]:
    return service.list_runs(limit=limit)


@router.get(
    "/data/validation-report/runs/{run_id}",
    response_model=ValidationRunResponse,
    status_code=status.HTTP_200_OK,
    summary="Get one offline data validation run",
)
def get_data_validation_run(
    run_id: int,
    service: ValidationRunService = Depends(get_validation_run_service),
) -> ValidationRunResponse:
    run = service.get_run(run_id)
    if run is None:
        raise BadRequestError(f"Validation run {run_id} was not found")
    return run


@router.post(
    "/data/validation-report/clear-failed",
    response_model=ValidationFailedRunsClearResponse,
    status_code=status.HTTP_200_OK,
    summary="Clear failed validation runs",
)
def clear_failed_data_validation_runs(
    service: ValidationRunService = Depends(get_validation_run_service),
) -> ValidationFailedRunsClearResponse:
    return ValidationFailedRunsClearResponse(**service.clear_failed_runs())
