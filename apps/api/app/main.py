from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.errors import APIError, api_error_handler
from app.api.router import api_router
from app.core.config import get_settings
from app.core.logging import configure_logging

configure_logging()

settings = get_settings()


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=(
            "Backend API for Binance.US-first market data ingestion, "
            "pattern research, and historical dataset validation."
        ),
        debug=settings.debug,
    )
    app.add_exception_handler(APIError, api_error_handler)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router, prefix=settings.api_v1_prefix)
    return app


app = create_app()
