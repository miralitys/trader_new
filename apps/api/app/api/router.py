from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.backtests import router as backtests_router
from app.api.routes.data import router as data_router
from app.api.routes.features import router as features_router
from app.api.routes.health import router as health_router
from app.api.routes.logs import router as logs_router
from app.api.routes.patterns import router as patterns_router
from app.api.routes.research import router as research_router
from app.api.routes.strategies import router as strategies_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(data_router)
api_router.include_router(features_router)
api_router.include_router(logs_router)
api_router.include_router(patterns_router)
api_router.include_router(research_router)
api_router.include_router(strategies_router)
api_router.include_router(backtests_router)
