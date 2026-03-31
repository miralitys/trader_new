from __future__ import annotations

from pydantic import BaseModel


class StrategyDescriptor(BaseModel):
    key: str
    name: str
    description: str
    spot_only: bool = True
    long_only: bool = True
    primary_side: str = "long"
    status: str = "scaffold"
