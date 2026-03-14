from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.dependencies import get_query_service
from app.schemas.api import DashboardSummaryResponse
from app.services.query_service import QueryService

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/summary", response_model=DashboardSummaryResponse, summary="Get dashboard summary")
def get_dashboard_summary(
    service: QueryService = Depends(get_query_service),
) -> DashboardSummaryResponse:
    return service.get_dashboard_summary()
