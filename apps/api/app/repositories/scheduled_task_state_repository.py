from __future__ import annotations

from typing import Optional

from sqlalchemy import select

from app.models import ScheduledTaskState
from app.repositories.base import BaseRepository


class ScheduledTaskStateRepository(BaseRepository):
    def get_by_task_key(self, task_key: str) -> Optional[ScheduledTaskState]:
        return self.session.scalar(select(ScheduledTaskState).where(ScheduledTaskState.task_key == task_key))

    def get_or_create(
        self,
        *,
        task_key: str,
        timezone_name: str,
        schedule_local_hour: int,
        schedule_local_minute: int,
        lookback_days: int,
    ) -> ScheduledTaskState:
        state = self.get_by_task_key(task_key)
        if state is None:
            state = ScheduledTaskState(
                task_key=task_key,
                timezone_name=timezone_name,
                schedule_local_hour=schedule_local_hour,
                schedule_local_minute=schedule_local_minute,
                lookback_days=lookback_days,
            )
            self.session.add(state)
            self.session.flush()
            return state

        state.timezone_name = timezone_name
        state.schedule_local_hour = schedule_local_hour
        state.schedule_local_minute = schedule_local_minute
        state.lookback_days = lookback_days
        self.session.add(state)
        self.session.flush()
        return state
