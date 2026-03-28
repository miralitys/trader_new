from __future__ import annotations

from collections.abc import Generator

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.db.session import get_db_session
from app.services.health import HealthService
from app.services.market_data_service import MarketDataService
from app.services.pattern_research_service import PatternResearchService
from app.services.query_service import QueryService


def get_settings_dependency() -> Settings:
    return get_settings()


def get_db_dependency() -> Generator[Session, None, None]:
    yield from get_db_session()


def get_health_service() -> HealthService:
    return HealthService(settings=get_settings())


def get_market_data_service() -> Generator[MarketDataService, None, None]:
    service = MarketDataService(settings=get_settings())
    try:
        yield service
    finally:
        service.close()


def get_query_service(db: Session = Depends(get_db_dependency)) -> QueryService:
    return QueryService(session=db)


def get_pattern_research_service(db: Session = Depends(get_db_dependency)) -> PatternResearchService:
    return PatternResearchService(session=db)


__all__ = [
    "get_db_dependency",
    "get_health_service",
    "get_market_data_service",
    "get_pattern_research_service",
    "get_query_service",
    "get_settings_dependency",
]
