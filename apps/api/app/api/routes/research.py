from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_pattern_research_service
from app.schemas.research import ResearchSummaryResponse
from app.services.pattern_research_service import PatternResearchService

router = APIRouter(prefix="/research", tags=["research"])


@router.get("/summary", response_model=ResearchSummaryResponse, summary="Get research summary")
def get_research_summary(
    exchange_code: str = Query(default="binance_us"),
    symbols: str | None = Query(default=None, description="Comma-separated symbols"),
    timeframes: str | None = Query(default=None, description="Comma-separated timeframes"),
    lookback_days: int = Query(default=730, ge=7, le=730),
    forward_bars: int = Query(default=12, ge=1, le=96),
    fee_pct: Decimal = Query(default=Decimal("0.001"), ge=0),
    slippage_pct: Decimal = Query(default=Decimal("0.0005"), ge=0),
    max_bars_per_series: int = Query(default=5000, ge=250, le=50000),
    service: PatternResearchService = Depends(get_pattern_research_service),
) -> ResearchSummaryResponse:
    return service.get_summary(
        exchange_code=exchange_code,
        symbols=[item.strip().upper() for item in symbols.split(",") if item.strip()] if symbols else None,
        timeframes=[item.strip() for item in timeframes.split(",") if item.strip()] if timeframes else None,
        lookback_days=lookback_days,
        forward_bars=forward_bars,
        fee_pct=fee_pct,
        slippage_pct=slippage_pct,
        max_bars_per_series=max_bars_per_series,
    )
