from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select

from app.models import Signal
from app.models.enums import SignalType
from app.repositories.base import BaseRepository


class SignalRepository(BaseRepository):
    def create_signal(
        self,
        strategy_run_id: int,
        symbol: str,
        timeframe: str,
        signal_type: SignalType,
        signal_strength: float,
        payload_json: dict[str, object],
        candle_time: datetime,
    ) -> Signal:
        signal = Signal(
            strategy_run_id=strategy_run_id,
            symbol=symbol,
            timeframe=timeframe,
            signal_type=signal_type,
            signal_strength=signal_strength,
            payload_json=payload_json,
            candle_time=candle_time,
        )
        self.session.add(signal)
        self.session.flush()
        return signal

    def list_signals(
        self,
        strategy_run_id: Optional[int] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 100,
    ) -> list[Signal]:
        stmt = select(Signal).order_by(Signal.created_at.desc(), Signal.id.desc()).limit(limit)
        if strategy_run_id is not None:
            stmt = stmt.where(Signal.strategy_run_id == strategy_run_id)
        if symbol:
            stmt = stmt.where(Signal.symbol == symbol)
        if timeframe:
            stmt = stmt.where(Signal.timeframe == timeframe)
        return list(self.session.scalars(stmt))
