from __future__ import annotations

from typing import Optional

from sqlalchemy import select

from app.models import StrategyConfig
from app.repositories.base import BaseRepository


class StrategyConfigRepository(BaseRepository):
    def get_active_by_strategy_id(self, strategy_id: int) -> Optional[StrategyConfig]:
        stmt = (
            select(StrategyConfig)
            .where(
                StrategyConfig.strategy_id == strategy_id,
                StrategyConfig.is_active.is_(True),
            )
            .order_by(StrategyConfig.updated_at.desc(), StrategyConfig.id.desc())
        )
        return self.session.scalar(stmt)

    def upsert_active(self, strategy_id: int, config_json: dict[str, object]) -> StrategyConfig:
        existing = self.get_active_by_strategy_id(strategy_id)
        if existing is None:
            existing = StrategyConfig(
                strategy_id=strategy_id,
                config_json=config_json,
                is_active=True,
            )
        else:
            existing.config_json = config_json
            existing.is_active = True

        self.session.add(existing)
        self.session.flush()
        return existing
