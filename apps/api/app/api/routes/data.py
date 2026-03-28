from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_data_validation_service, get_market_data_service, get_query_service
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
)
from app.services.data_validation_service import DataValidationService, serialize_validation_report
from app.services.market_data_service import MarketDataService
from app.services.query_service import QueryService

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
    payload = serialize_validation_report(report)
    results = payload["results"]

    completion_by_timeframe: dict[str, list[float]] = {}
    symbol_rollup: dict[str, dict[str, float | int | str]] = {}
    timeframe_rollup: dict[str, dict[str, float | int | str]] = {}
    best_completion_by_symbol: dict[str, float] = {}
    one_minute_rows: list[dict[str, object]] = []

    pass_count = warning_count = fail_count = 0
    duplicate_rows_total = invalid_timestamps_total = internal_gap_total = 0

    for row in results:
        verdict = _value(row, "verdict")
        if verdict == "PASS":
            pass_count += 1
        elif verdict == "PASS WITH WARNINGS":
            warning_count += 1
        else:
            fail_count += 1

        symbol = _value(row, "symbol")
        timeframe = _value(row, "timeframe")
        validation_window = _value(row, "validation_window")
        gaps = _value(row, "gaps")
        duplicates = _value(row, "duplicates")
        timestamp_alignment = _value(row, "timestamp_alignment")

        completion_pct = float(_value(validation_window, "completion_pct"))
        gap_count = int(_value(gaps, "missing_candle_count"))
        duplicate_count = int(_value(duplicates, "duplicate_count"))
        invalid_timestamp_count = int(_value(timestamp_alignment, "invalid_timestamp_count"))

        duplicate_rows_total += duplicate_count
        invalid_timestamps_total += invalid_timestamp_count
        internal_gap_total += gap_count

        completion_by_timeframe.setdefault(timeframe, []).append(completion_pct)
        best_completion_by_symbol[symbol] = max(best_completion_by_symbol.get(symbol, 0.0), completion_pct)

        symbol_entry = symbol_rollup.setdefault(
            symbol,
            {
                "symbol": symbol,
                "worst_completion_pct": 100.0,
                "total_gap_count": 0,
                "total_duplicate_count": 0,
                "invalid_timestamp_count": 0,
                "failing_series_count": 0,
            },
        )
        symbol_entry["worst_completion_pct"] = min(float(symbol_entry["worst_completion_pct"]), completion_pct)
        symbol_entry["total_gap_count"] = int(symbol_entry["total_gap_count"]) + gap_count
        symbol_entry["total_duplicate_count"] = int(symbol_entry["total_duplicate_count"]) + duplicate_count
        symbol_entry["invalid_timestamp_count"] = int(symbol_entry["invalid_timestamp_count"]) + invalid_timestamp_count
        if verdict != "PASS":
            symbol_entry["failing_series_count"] = int(symbol_entry["failing_series_count"]) + 1

        timeframe_entry = timeframe_rollup.setdefault(
            timeframe,
            {
                "timeframe": timeframe,
                "completion_values": [],
                "total_gap_count": 0,
                "failing_series_count": 0,
            },
        )
        timeframe_entry["completion_values"].append(completion_pct)
        timeframe_entry["total_gap_count"] = int(timeframe_entry["total_gap_count"]) + gap_count
        if verdict != "PASS":
            timeframe_entry["failing_series_count"] = int(timeframe_entry["failing_series_count"]) + 1

        if timeframe == "1m":
            one_minute_rows.append(
                {
                    "symbol": symbol,
                    "completion_pct": completion_pct,
                    "gap_count": gap_count,
                }
            )

    worst_symbols = sorted(
        symbol_rollup.values(),
        key=lambda item: (
            float(item["worst_completion_pct"]),
            -int(item["total_gap_count"]),
            -int(item["failing_series_count"]),
        ),
    )[:10]

    worst_timeframes = []
    for timeframe, item in timeframe_rollup.items():
        completion_values = item.pop("completion_values")
        avg_completion = sum(completion_values) / max(1, len(completion_values))
        worst_timeframes.append(
            {
                "timeframe": timeframe,
                "avg_completion_pct": avg_completion,
                "total_gap_count": item["total_gap_count"],
                "failing_series_count": item["failing_series_count"],
            }
        )
    worst_timeframes = sorted(
        worst_timeframes,
        key=lambda item: (float(item["avg_completion_pct"]), -int(item["total_gap_count"])),
    )

    one_minute_laggards = sorted(
        (
            {
                "symbol": row["symbol"],
                "completion_pct": row["completion_pct"],
                "gap_vs_best_timeframe_pct": max(best_completion_by_symbol.get(row["symbol"], 0.0) - float(row["completion_pct"]), 0.0),
                "gap_count": row["gap_count"],
            }
            for row in one_minute_rows
        ),
        key=lambda item: (float(item["completion_pct"]), -float(item["gap_vs_best_timeframe_pct"]), -int(item["gap_count"])),
    )[:10]

    summary = DataValidationSummaryResponse(
        generated_at=payload["generated_at"],
        exchange_code=payload["exchange_code"],
        lookback_days=payload["lookback_days"],
        verdict=payload["verdict"],
        overview=DataValidationOverviewResponse(
            total_series=len(results),
            pass_count=pass_count,
            warning_count=warning_count,
            fail_count=fail_count,
            duplicate_rows_total=duplicate_rows_total,
            invalid_timestamps_total=invalid_timestamps_total,
            internal_gap_total=internal_gap_total,
        ),
        worst_symbols=[DataValidationSymbolSummaryResponse(**item) for item in worst_symbols],
        worst_timeframes=[DataValidationTimeframeSummaryResponse(**item) for item in worst_timeframes],
        one_minute_laggards=[DataValidationOneMinuteSummaryResponse(**item) for item in one_minute_laggards],
        completion_by_timeframe={
            timeframe: round(sum(values) / max(1, len(values)), 4)
            for timeframe, values in completion_by_timeframe.items()
        },
    )

    return DataValidationReportResponse(
        summary=summary,
        results=[DataValidationResultResponse(**row) for row in results],
    )
