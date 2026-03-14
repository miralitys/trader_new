from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.domain.schemas.backtest import BacktestRunRead, BacktestRunRequest
from app.services.backtest_service import BacktestService

router = APIRouter(prefix="/backtests")


@router.post("/run")
def run_backtest(payload: BacktestRunRequest, db: Session = Depends(get_db)) -> dict:
    try:
        return BacktestService(db).create_run(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("", response_model=list[BacktestRunRead])
def list_backtests(db: Session = Depends(get_db)) -> list[BacktestRunRead]:
    return BacktestService(db).list_runs()


@router.get("/{run_id}")
def get_backtest(run_id: int, db: Session = Depends(get_db)) -> dict:
    try:
        return BacktestService(db).get_run_result(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{run_id}/export")
def export_backtest(
    run_id: int,
    format_name: str = Query(default="json", alias="format"),
    db: Session = Depends(get_db),
):
    try:
        payload = BacktestService(db).export(run_id, format_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if format_name == "csv":
        return PlainTextResponse(payload, media_type="text/csv")
    return payload
