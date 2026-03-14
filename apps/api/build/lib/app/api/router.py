from __future__ import annotations

from fastapi import APIRouter

from app.api.routes import backtests, dashboard, data, logs, positions, signals, strategies, trades

api_router = APIRouter(prefix="/api")
api_router.include_router(dashboard.router, tags=["dashboard"])
api_router.include_router(strategies.router, tags=["strategies"])
api_router.include_router(backtests.router, tags=["backtests"])
api_router.include_router(data.router, tags=["data"])
api_router.include_router(signals.router, tags=["signals"])
api_router.include_router(trades.router, tags=["trades"])
api_router.include_router(positions.router, tags=["positions"])
api_router.include_router(logs.router, tags=["logs"])
