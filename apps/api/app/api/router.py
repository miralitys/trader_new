from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.backtests import router as backtests_router
from app.api.routes.dashboard import router as dashboard_router
from app.api.routes.data import router as data_router
from app.api.routes.health import router as health_router
from app.api.routes.logs import router as logs_router
from app.api.routes.strategies import router as strategies_router
from app.api.routes.strategy_runs import router as strategy_runs_router
from app.api.routes.trading import router as trading_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(strategies_router)
api_router.include_router(strategy_runs_router)
api_router.include_router(backtests_router)
api_router.include_router(data_router)
api_router.include_router(trading_router)
api_router.include_router(logs_router)
api_router.include_router(dashboard_router)
