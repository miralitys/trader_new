from __future__ import annotations

from sqlalchemy.orm import Session


class BaseRepository:
    """Thin repository base to keep DB access behind explicit boundaries."""

    def __init__(self, session: Session) -> None:
        self.session = session
