from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.dependencies import get_health_service
from app.schemas.health import HealthResponse
from app.services.health import HealthService

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse, summary="Health check")
def health_check(service: HealthService = Depends(get_health_service)) -> HealthResponse:
    return service.get_status()
