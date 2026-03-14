from __future__ import annotations

from sqlalchemy.orm import Session

from app.repositories.trading import TradingRepository


class LogService:
    def __init__(self, db: Session) -> None:
        self.repo = TradingRepository(db)

    def info(self, category: str, message: str, **context: object) -> None:
        self.repo.create_log(category=category, level="info", message=message, context=context)

    def warning(self, category: str, message: str, **context: object) -> None:
        self.repo.create_log(category=category, level="warning", message=message, context=context)

    def error(self, category: str, message: str, **context: object) -> None:
        self.repo.create_log(category=category, level="error", message=message, context=context)
