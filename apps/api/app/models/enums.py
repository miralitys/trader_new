from __future__ import annotations

from enum import Enum

from sqlalchemy import Enum as SAEnum


def enum_values(enum_class: type[Enum]) -> list[str]:
    return [member.value for member in enum_class]


def pg_enum(enum_class: type[Enum], name: str) -> SAEnum:
    return SAEnum(enum_class, name=name, values_callable=enum_values)


class StrategyRunMode(str, Enum):
    PAPER = "paper"
    BACKTEST = "backtest"
    LIVE_PREP = "live_prep"


class StrategyRunStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"
    COMPLETED = "completed"


class SignalType(str, Enum):
    ENTER = "enter"
    EXIT = "exit"
    HOLD = "hold"


class Side(str, Enum):
    LONG = "long"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    SIMULATED = "simulated"


class OrderStatus(str, Enum):
    NEW = "new"
    OPEN = "open"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class PositionStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


class BacktestStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class SyncJobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class AppLogLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
