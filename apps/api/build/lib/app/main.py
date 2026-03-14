from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.logging import configure_logging

configure_logging()

app = FastAPI(
    title="Trader MVP API",
    version="0.1.0",
    description="Algorithmic crypto trading MVP with Coinbase market data, backtests, and paper trading.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
