from __future__ import annotations

from typing import Optional

from sqlalchemy import select

from app.models import AppLog
from app.models.enums import AppLogLevel
from app.repositories.base import BaseRepository


class LogRepository(BaseRepository):
    def list_logs(
        self,
        scope: Optional[str] = None,
        level: Optional[str] = None,
        limit: int = 100,
    ) -> list[AppLog]:
        stmt = select(AppLog).order_by(AppLog.created_at.desc(), AppLog.id.desc()).limit(limit)
        if scope:
            stmt = stmt.where(AppLog.scope == scope)
        if level:
            stmt = stmt.where(AppLog.level == AppLogLevel(level))
        return list(self.session.scalars(stmt))
