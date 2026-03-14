from __future__ import annotations

from enum import Enum


class StringEnum(str, Enum):
    pass


class StrategyStatus(StringEnum):
    STOPPED = "stopped"
    RUNNING = "running"
    BACKTESTING = "backtesting"
    PAPER_TRADING = "paper_trading"
    SYNCING_DATA = "syncing_data"


class RunMode(StringEnum):
    HISTORICAL_BACKTEST = "historical_backtest"
    PAPER_TRADING = "paper_trading"
    LIVE_PREP = "live_prep"


class OrderStatus(StringEnum):
    CREATED = "created"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class PositionStatus(StringEnum):
    OPEN = "open"
    CLOSED = "closed"


class SignalAction(StringEnum):
    ENTER = "enter"
    EXIT = "exit"
    HOLD = "hold"


class SignalSide(StringEnum):
    LONG = "long"


class BacktestStatus(StringEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class SyncJobStatus(StringEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class LogLevel(StringEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
