from __future__ import annotations

from typing import Optional

from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.api.errors import BadRequestError, ConflictError, NotFoundError
from app.repositories.strategy_config_repository import StrategyConfigRepository
from app.repositories.strategy_run_repository import StrategyRunRepository
from app.schemas.api import (
    StrategyConfigResponse,
    StrategyConfigUpdateRequest,
    StrategyDetailResponse,
    StrategyPaperStartRequest,
    StrategySummaryResponse,
)
from app.schemas.paper import PaperRunResponse, PaperRunStartRequest
from app.services.paper_execution_service import PaperExecutionService
from app.strategies.base import BaseStrategy
from app.strategies.registry import get_strategy, list_strategies

INTERFACE_VISIBLE_STRATEGY_CODES = frozenset(
    {
        "breakout_continuation",
        "breakout_retest",
        "pullback_to_trend",
        "trend_retrace_70",
    }
)


class StrategyService:
    def __init__(
        self,
        session: Session,
        paper_execution_service: Optional[PaperExecutionService] = None,
    ) -> None:
        self.session = session
        self.strategy_run_repository = StrategyRunRepository(session)
        self.strategy_config_repository = StrategyConfigRepository(session)
        self.paper_execution_service = paper_execution_service or PaperExecutionService()

    def list_strategies(self) -> list[StrategySummaryResponse]:
        payload: list[StrategySummaryResponse] = []
        for strategy in list_strategies():
            if strategy.key not in INTERFACE_VISIBLE_STRATEGY_CODES:
                continue
            strategy_row = self.strategy_run_repository.get_strategy_by_code(strategy.key)
            active_run = None
            has_saved_config = False
            if strategy_row is not None:
                active_run = self.strategy_run_repository.get_active_paper_run_for_strategy(strategy_row.id)
                has_saved_config = self.strategy_config_repository.get_active_by_strategy_id(strategy_row.id) is not None

            payload.append(
                StrategySummaryResponse(
                    code=strategy.key,
                    name=strategy.name,
                    description=strategy.description,
                    spot_only=strategy.spot_only,
                    long_only=strategy.long_only,
                    has_saved_config=has_saved_config,
                    active_paper_run_id=active_run.id if active_run is not None else None,
                    active_paper_status=active_run.status.value if active_run is not None else None,
                )
            )
        return payload

    def visible_strategy_codes(self) -> set[str]:
        return set(INTERFACE_VISIBLE_STRATEGY_CODES)

    def get_strategy(self, code: str) -> StrategyDetailResponse:
        strategy = self._resolve_strategy(code)
        strategy_row = self.strategy_run_repository.get_strategy_by_code(strategy.key)
        active_run = None
        config_record = None
        if strategy_row is not None:
            active_run = self.strategy_run_repository.get_active_paper_run_for_strategy(strategy_row.id)
            config_record = self.strategy_config_repository.get_active_by_strategy_id(strategy_row.id)

        effective_config = self._build_effective_config(strategy, config_record.config_json if config_record else None)
        return StrategyDetailResponse(
            code=strategy.key,
            name=strategy.name,
            description=strategy.description,
            spot_only=strategy.spot_only,
            long_only=strategy.long_only,
            has_saved_config=config_record is not None,
            active_paper_run_id=active_run.id if active_run is not None else None,
            active_paper_status=active_run.status.value if active_run is not None else None,
            default_config=strategy.default_config(),
            effective_config=effective_config,
            config_schema=strategy.config_model.model_json_schema(),
            config_source="database" if config_record is not None else "default",
        )

    def get_strategy_config(self, code: str) -> StrategyConfigResponse:
        strategy = self._resolve_strategy(code)
        strategy_row = self.strategy_run_repository.get_strategy_by_code(strategy.key)
        config_record = None
        if strategy_row is not None:
            config_record = self.strategy_config_repository.get_active_by_strategy_id(strategy_row.id)

        effective_config = self._build_effective_config(strategy, config_record.config_json if config_record else None)
        return StrategyConfigResponse(
            strategy_code=strategy.key,
            source="database" if config_record is not None else "default",
            config=effective_config,
            default_config=strategy.default_config(),
            config_schema=strategy.config_model.model_json_schema(),
            updated_at=config_record.updated_at if config_record is not None else None,
        )

    def update_strategy_config(
        self,
        code: str,
        request: StrategyConfigUpdateRequest,
    ) -> StrategyConfigResponse:
        strategy = self._resolve_strategy(code)
        strategy_row = self.strategy_run_repository.ensure_strategy(
            code=strategy.key,
            name=strategy.name,
            description=strategy.description,
        )
        validated_config = self._build_effective_config(strategy, request.config)
        config_record = self.strategy_config_repository.upsert_active(
            strategy_id=strategy_row.id,
            config_json=validated_config,
        )
        self.session.commit()
        return StrategyConfigResponse(
            strategy_code=strategy.key,
            source="database",
            config=validated_config,
            default_config=strategy.default_config(),
            config_schema=strategy.config_model.model_json_schema(),
            updated_at=config_record.updated_at,
        )

    def start_paper_run(self, code: str, request: StrategyPaperStartRequest) -> PaperRunResponse:
        strategy = self._resolve_strategy(code)
        strategy_row = self.strategy_run_repository.ensure_strategy(
            code=strategy.key,
            name=strategy.name,
            description=strategy.description,
        )
        stored_config = self.strategy_config_repository.get_active_by_strategy_id(strategy_row.id)
        merged_config = self._build_effective_config(
            strategy,
            {
                **(stored_config.config_json if stored_config is not None else {}),
                **request.strategy_config_override,
            },
        )

        try:
            return self.paper_execution_service.start_run(
                PaperRunStartRequest(
                    strategy_code=strategy.key,
                    symbols=request.symbols,
                    timeframes=request.timeframes,
                    exchange_code=request.exchange_code,
                    initial_balance=request.initial_balance,
                    currency=request.currency,
                    fee=request.fee,
                    slippage=request.slippage,
                    strategy_config_override=merged_config,
                    metadata=request.metadata,
                )
            )
        except ValueError as exc:
            detail = str(exc)
            if "Active paper run already exists" in detail:
                raise ConflictError(detail) from exc
            raise BadRequestError(detail) from exc
        except KeyError as exc:
            raise NotFoundError(f"Strategy {code} was not found") from exc

    def stop_paper_run(self, code: str, reason: str) -> PaperRunResponse:
        strategy = self._resolve_strategy(code)
        strategy_row = self.strategy_run_repository.get_strategy_by_code(strategy.key)
        if strategy_row is None:
            raise NotFoundError(f"No persisted paper run exists for strategy {code}")

        active_run = self.strategy_run_repository.get_active_paper_run_for_strategy(strategy_row.id)
        if active_run is None:
            raise NotFoundError(f"No active paper run exists for strategy {code}")

        try:
            return self.paper_execution_service.stop_run(active_run.id, reason=reason)
        except ValueError as exc:
            raise BadRequestError(str(exc)) from exc

    def _resolve_strategy(self, code: str) -> BaseStrategy:
        try:
            return get_strategy(code)
        except KeyError as exc:
            raise NotFoundError(f"Strategy {code} was not found") from exc

    def _build_effective_config(
        self,
        strategy: BaseStrategy,
        overrides: Optional[dict[str, object]],
    ) -> dict[str, object]:
        payload = strategy.default_config()
        if overrides:
            payload.update(overrides)
        try:
            return strategy.parse_config(payload).model_dump()
        except ValidationError as exc:
            raise BadRequestError(str(exc)) from exc
