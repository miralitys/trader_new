from __future__ import annotations

from collections.abc import Generator

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.db.session import get_db_session
from app.services.data_validation_service import DataValidationService
from app.services.feature_layer_service import FeatureLayerService
from app.services.health import HealthService
from app.services.market_data_service import MarketDataService
from app.services.backtest_runner_service import BacktestRunnerService
from app.services.pattern_scan_run_service import PatternScanRunService
from app.services.pattern_research_service import PatternResearchService
from app.services.query_service import QueryService
from app.services.strategy_service import StrategyService
from app.services.validation_run_service import ValidationRunService


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


def get_strategy_service(db: Session = Depends(get_db_dependency)) -> StrategyService:
    return StrategyService(session=db)


def get_backtest_runner_service() -> BacktestRunnerService:
    return BacktestRunnerService()


def get_pattern_research_service(db: Session = Depends(get_db_dependency)) -> PatternResearchService:
    return PatternResearchService(session=db)


def get_pattern_scan_run_service(db: Session = Depends(get_db_dependency)) -> PatternScanRunService:
    return PatternScanRunService(session=db)


def get_data_validation_service(db: Session = Depends(get_db_dependency)) -> Generator[DataValidationService, None, None]:
    service = DataValidationService(session=db)
    try:
        yield service
    finally:
        service.close()


def get_feature_layer_service(db: Session = Depends(get_db_dependency)) -> FeatureLayerService:
    return FeatureLayerService(session=db)


def get_validation_run_service(db: Session = Depends(get_db_dependency)) -> ValidationRunService:
    return ValidationRunService(session=db)


__all__ = [
    "get_data_validation_service",
    "get_db_dependency",
    "get_feature_layer_service",
    "get_health_service",
    "get_market_data_service",
    "get_backtest_runner_service",
    "get_pattern_scan_run_service",
    "get_pattern_research_service",
    "get_query_service",
    "get_settings_dependency",
    "get_strategy_service",
    "get_validation_run_service",
]
