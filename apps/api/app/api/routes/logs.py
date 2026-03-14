from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_query_service
from app.schemas.api import AppLogResponse
from app.services.query_service import QueryService

router = APIRouter(tags=["logs"])


@router.get("/logs", response_model=list[AppLogResponse], summary="List application logs")
def list_logs(
    scope: Optional[str] = Query(default=None),
    level: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    service: QueryService = Depends(get_query_service),
) -> list[AppLogResponse]:
    return service.list_logs(scope=scope, level=level, limit=limit)
