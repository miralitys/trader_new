from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import get_pattern_scan_run_service
from app.api.errors import BadRequestError
from app.schemas.research import PatternScanRequest, PatternScanRunResponse
from app.services.pattern_scan_run_service import PatternScanRunService

router = APIRouter(prefix="/patterns", tags=["patterns"])


@router.post("/scan/start", response_model=PatternScanRunResponse, status_code=status.HTTP_202_ACCEPTED, summary="Queue pattern scan")
def start_pattern_scan(
    request: PatternScanRequest,
    service: PatternScanRunService = Depends(get_pattern_scan_run_service),
) -> PatternScanRunResponse:
    return service.create_run(request)


@router.get("/scans", response_model=list[PatternScanRunResponse], status_code=status.HTTP_200_OK, summary="List pattern scans")
def list_pattern_scans(
    limit: int = Query(default=20, ge=1, le=200),
    service: PatternScanRunService = Depends(get_pattern_scan_run_service),
) -> list[PatternScanRunResponse]:
    return service.list_runs(limit=limit)


@router.get("/scans/{run_id}", response_model=PatternScanRunResponse, status_code=status.HTTP_200_OK, summary="Get one pattern scan")
def get_pattern_scan(
    run_id: int,
    service: PatternScanRunService = Depends(get_pattern_scan_run_service),
) -> PatternScanRunResponse:
    run = service.get_run(run_id)
    if run is None:
        raise BadRequestError(f"Pattern scan run {run_id} was not found")
    return run
