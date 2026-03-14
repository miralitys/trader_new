from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    app_name: str
    environment: str
    version: str
    timestamp: datetime
    services: dict[str, str] = Field(default_factory=dict)
