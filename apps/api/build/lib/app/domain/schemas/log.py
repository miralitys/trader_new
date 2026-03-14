from __future__ import annotations

from datetime import datetime
from typing import Any

from app.domain.schemas.common import ORMModel


class LogRead(ORMModel):
    id: int
    category: str
    level: str
    strategy_id: int | None = None
    symbol_id: int | None = None
    message: str
    context: dict[str, Any]
    created_at: datetime
