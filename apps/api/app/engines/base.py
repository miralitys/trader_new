from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class EngineDescriptor:
    name: str
    purpose: str
    status: str = "placeholder"
    ready_for_execution: bool = False


class EngineBase:
    engine_name = "base_engine"
    purpose = "base scaffold"

    def describe(self) -> dict[str, str | bool]:
        return asdict(
            EngineDescriptor(
                name=self.engine_name,
                purpose=self.purpose,
            )
        )
