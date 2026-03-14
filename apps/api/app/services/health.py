from __future__ import annotations

from app.core.config import Settings
from app.schemas.health import HealthResponse
from app.utils.time import utc_now


class HealthService:
    """Small service used by the scaffold API and startup checks."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def get_status(self) -> HealthResponse:
        return HealthResponse(
            app_name=self.settings.app_name,
            environment=self.settings.environment,
            version=self.settings.app_version,
            timestamp=utc_now(),
            services={
                "api": "ready",
                "database": "configured",
                "redis": "configured",
                "coinbase": "configured",
            },
        )
