from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.domain.schemas.strategy import StrategyConfigUpdate, StrategyDetail, StrategyListItem
from app.domain.schemas.strategy import StrategyModeChangeRequest
from app.services.strategy_service import StrategyService

router = APIRouter(prefix="/strategies")


@router.get("", response_model=list[StrategyListItem])
def list_strategies(db: Session = Depends(get_db)) -> list[StrategyListItem]:
    return StrategyService(db).list_strategies()


@router.get("/{strategy_id}", response_model=StrategyDetail)
def get_strategy(strategy_id: int, db: Session = Depends(get_db)) -> StrategyDetail:
    try:
        return StrategyService(db).get_strategy_detail(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{strategy_id}/start")
def start_strategy(
    strategy_id: int,
    payload: StrategyModeChangeRequest,
    db: Session = Depends(get_db),
) -> dict:
    try:
        return StrategyService(db).start_strategy(strategy_id, payload.mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{strategy_id}/stop")
def stop_strategy(strategy_id: int, db: Session = Depends(get_db)) -> dict:
    try:
        return StrategyService(db).stop_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{strategy_id}/config")
def get_config(strategy_id: int, db: Session = Depends(get_db)) -> dict:
    try:
        return StrategyService(db).get_config(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.put("/{strategy_id}/config")
def update_config(
    strategy_id: int,
    payload: StrategyConfigUpdate,
    db: Session = Depends(get_db),
) -> dict:
    try:
        return StrategyService(db).update_config(strategy_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
