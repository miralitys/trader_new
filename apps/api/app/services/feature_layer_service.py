from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import sqrt
from statistics import pstdev
from typing import Optional

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.db.session import session_scope
from app.integrations.binance_us import BinanceUSTimeframe
from app.models import FeatureRun
from app.repositories.candle_repository import CandleRepository
from app.repositories.feature_repository import FeatureRepository
from app.schemas.api import FeatureCoverageResponse, FeatureRunRequest, FeatureRunResponse
from app.utils.exchanges import normalize_exchange_code
from app.utils.symbols import normalize_supported_symbol
from app.utils.time import ensure_utc, utc_now

logger = get_logger(__name__)

FEATURE_CHUNK_DAYS = {
    "1m": 7,
    "5m": 30,
    "15m": 90,
    "1h": 180,
    "4h": 365,
}


@dataclass(frozen=True)
class _FeatureRow:
    open_time: object
    ret_1: float | None
    ret_3: float | None
    ret_12: float | None
    ret_48: float | None
    range_pct: float | None
    atr_pct: float | None
    realized_vol_20: float | None
    body_pct: float | None
    upper_wick_pct: float | None
    lower_wick_pct: float | None
    distance_to_high_20_pct: float | None
    distance_to_low_20_pct: float | None
    ema20_dist_pct: float | None
    ema50_dist_pct: float | None
    ema200_dist_pct: float | None
    ema20_slope_pct: float | None
    ema50_slope_pct: float | None
    ema200_slope_pct: float | None
    relative_volume_20: float | None
    volume_zscore_20: float | None
    compression_ratio_12: float | None
    expansion_ratio_12: float | None

    def to_payload(self) -> dict[str, object]:
        return self.__dict__.copy()


@dataclass
class _FeatureRunAggregate:
    source_candle_count: int = 0
    feature_rows_upserted: int = 0
    computed_start_at: datetime | None = None
    computed_end_at: datetime | None = None

    def update(
        self,
        *,
        source_candle_count: int,
        feature_rows_upserted: int,
        computed_start_at: datetime | None,
        computed_end_at: datetime | None,
    ) -> None:
        self.source_candle_count += source_candle_count
        self.feature_rows_upserted += feature_rows_upserted
        if self.computed_start_at is None and computed_start_at is not None:
            self.computed_start_at = computed_start_at
        if computed_end_at is not None:
            self.computed_end_at = computed_end_at


class FeatureLayerService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.candle_repository = CandleRepository(session)
        self.feature_repository = FeatureRepository(session)

    def create_run(self, request: FeatureRunRequest) -> FeatureRunResponse:
        normalized_exchange = normalize_exchange_code(request.exchange_code)
        normalized_symbol = normalize_supported_symbol(request.symbol)
        timeframe_value = BinanceUSTimeframe.from_code(request.timeframe)

        end_at = utc_now()
        start_at = end_at - timedelta(days=request.lookback_days)

        run = self.feature_repository.create_run(
            exchange=normalized_exchange,
            symbol=normalized_symbol,
            timeframe=timeframe_value.value,
            lookback_days=request.lookback_days,
            start_at=start_at,
            end_at=end_at,
        )
        self.session.commit()
        return self._build_run_response(run)

    def process_next_queued_run(self) -> bool:
        with session_scope() as session:
            repository = FeatureRepository(session)
            run = repository.get_next_queued_run()
            run_id = run.id if run is not None else None

        if run_id is None:
            return False

        return self.execute_run(run_id)

    def execute_run(self, run_id: int) -> bool:
        with session_scope() as session:
            repository = FeatureRepository(session)
            run = repository.get_by_id(run_id)
            if run is None:
                return False
            if run.status.value == "completed":
                return False

            repository.mark_running(run)
            logger.info(
                "Feature run started",
                extra={
                    "run_id": run.id,
                    "symbol": run.symbol,
                    "timeframe": run.timeframe,
                    "lookback_days": run.lookback_days,
                },
            )

        source_candles = []
        aggregate = _FeatureRunAggregate()

        try:
            with session_scope() as session:
                repository = FeatureRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return False
                run_exchange = run.exchange
                run_symbol = run.symbol
                run_timeframe = run.timeframe
                run_start_at = run.start_at
                run_end_at = run.end_at

            if run_start_at is None or run_end_at is None:
                raise ValueError("Feature run is missing start/end bounds")

            processing_windows = self._build_processing_windows(
                start_at=run_start_at,
                end_at=run_end_at,
                timeframe=run_timeframe,
            )

            timeframe_value = BinanceUSTimeframe.from_code(run_timeframe)
            warmup_bars = 240

            for chunk_index, (chunk_start_at, chunk_end_at) in enumerate(processing_windows, start=1):
                chunk_source_count = 0
                chunk_feature_rows_upserted = 0
                chunk_computed_start_at = None
                chunk_computed_end_at = None

                with session_scope() as session:
                    repository = FeatureRepository(session)
                    run = repository.get_by_id(run_id)
                    if run is None:
                        return False
                    if run.status.value != "running":
                        logger.info(
                            "Feature run aborted before chunk execution",
                            extra={"run_id": run_id, "chunk_index": chunk_index},
                        )
                        return False

                    candle_repository = CandleRepository(session)
                    exchange = candle_repository.get_exchange(run_exchange)
                    if exchange is None:
                        raise ValueError(f"Exchange {run_exchange} is not loaded")

                    symbol_row = candle_repository.get_symbol(exchange.id, run_symbol)
                    if symbol_row is None:
                        raise ValueError(f"Symbol {run_symbol} is not loaded")

                    warmup_start_at = chunk_start_at - timeframe_value.interval * warmup_bars
                    source_candles = candle_repository.list_candles(
                        exchange_code=run_exchange,
                        symbol_code=run_symbol,
                        timeframe=run_timeframe,
                        start_at=warmup_start_at,
                        end_at=chunk_end_at,
                    )

                    feature_rows = self._compute_feature_rows(source_candles=source_candles, start_at=chunk_start_at)
                    chunk_source_count = sum(1 for candle in source_candles if candle.open_time >= chunk_start_at)

                    if feature_rows:
                        chunk_feature_rows_upserted = repository.upsert_features(
                            exchange_id=exchange.id,
                            symbol_id=symbol_row.id,
                            timeframe=run_timeframe,
                            rows=[row.to_payload() for row in feature_rows],
                        )
                        chunk_computed_start_at = feature_rows[0].open_time
                        chunk_computed_end_at = feature_rows[-1].open_time

                    aggregate.update(
                        source_candle_count=chunk_source_count,
                        feature_rows_upserted=chunk_feature_rows_upserted,
                        computed_start_at=chunk_computed_start_at,
                        computed_end_at=chunk_computed_end_at,
                    )

                    run.source_candle_count = aggregate.source_candle_count
                    run.feature_rows_upserted = aggregate.feature_rows_upserted
                    run.computed_start_at = aggregate.computed_start_at
                    run.computed_end_at = aggregate.computed_end_at
                    run.error_text = None
                    session.add(run)
                    session.flush()

                logger.info(
                    "Feature run chunk completed",
                    extra={
                        "run_id": run_id,
                        "symbol": run_symbol,
                        "timeframe": run_timeframe,
                        "chunk_index": chunk_index,
                        "chunk_total": len(processing_windows),
                        "chunk_start_at": chunk_start_at.isoformat(),
                        "chunk_end_at": chunk_end_at.isoformat(),
                        "chunk_source_candle_count": chunk_source_count,
                        "chunk_feature_rows_upserted": chunk_feature_rows_upserted,
                        "feature_rows_upserted_total": aggregate.feature_rows_upserted,
                    },
                )

            with session_scope() as session:
                repository = FeatureRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return False

                repository.mark_completed(
                    run,
                    source_candle_count=aggregate.source_candle_count,
                    feature_rows_upserted=aggregate.feature_rows_upserted,
                    computed_start_at=aggregate.computed_start_at,
                    computed_end_at=aggregate.computed_end_at,
                )
                logger.info(
                    "Feature run completed",
                    extra={
                        "run_id": run.id,
                        "symbol": run.symbol,
                        "timeframe": run.timeframe,
                        "source_candle_count": aggregate.source_candle_count,
                        "feature_rows_upserted": aggregate.feature_rows_upserted,
                        "chunk_total": len(processing_windows),
                    },
                )
                return True
        except Exception as exc:
            with session_scope() as session:
                repository = FeatureRepository(session)
                run = repository.get_by_id(run_id)
                if run is None:
                    return False
                repository.mark_failed(
                    run,
                    error_text=str(exc),
                    source_candle_count=aggregate.source_candle_count,
                    feature_rows_upserted=aggregate.feature_rows_upserted,
                    computed_start_at=aggregate.computed_start_at,
                    computed_end_at=aggregate.computed_end_at,
                )
                logger.exception("Feature run failed", extra={"run_id": run.id, "symbol": run.symbol, "timeframe": run.timeframe})
            return False

    def mark_stale_running_runs(self, *, stale_after_seconds: int) -> int:
        if stale_after_seconds <= 0:
            return 0

        stale_before = utc_now().replace(microsecond=0) - timedelta(seconds=stale_after_seconds)

        with session_scope() as session:
            repository = FeatureRepository(session)
            stale_runs = repository.list_stale_running_runs(stale_before=stale_before)
            for run in stale_runs:
                repository.mark_failed(
                    run,
                    error_text=(
                        "Feature run became stale before completion. "
                        "The background process likely stopped, the instance restarted, or the current series stalled."
                    ),
                    source_candle_count=run.source_candle_count,
                    feature_rows_upserted=run.feature_rows_upserted,
                    computed_start_at=run.computed_start_at,
                    computed_end_at=run.computed_end_at,
                )

            if stale_runs:
                logger.warning(
                    "Marked stale feature runs as failed",
                    extra={"run_ids": [run.id for run in stale_runs], "count": len(stale_runs)},
                )
            return len(stale_runs)

    def list_runs(
        self,
        *,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 500,
    ) -> list[FeatureRunResponse]:
        normalized_symbol = normalize_supported_symbol(symbol) if symbol else None
        normalized_timeframe = BinanceUSTimeframe.from_code(timeframe).value if timeframe else None
        rows = self.feature_repository.list_runs(symbol=normalized_symbol, timeframe=normalized_timeframe, limit=limit)
        return [self._build_run_response(row) for row in rows]

    def get_symbol_timeframe_coverages(
        self,
        *,
        exchange_code: str,
        symbols: list[str],
        timeframes: list[str],
    ) -> list[FeatureCoverageResponse]:
        normalized_exchange = normalize_exchange_code(exchange_code)
        exchange = self.candle_repository.get_exchange(normalized_exchange)
        if exchange is None:
            return []

        from sqlalchemy import select
        from app.models import Symbol

        symbol_rows = {
            row.id: row.code
            for row in self.session.scalars(
                select(Symbol).where(Symbol.exchange_id == exchange.id, Symbol.code.in_(symbols))
            )
        }
        raw_rows = self.feature_repository.list_feature_coverages(
            exchange_id=exchange.id,
            symbol_ids=list(symbol_rows.keys()),
            timeframes=timeframes,
        )
        by_key = {
            (symbol_rows.get(item["symbol_id"]), item["timeframe"]): item
            for item in raw_rows
            if symbol_rows.get(item["symbol_id"]) is not None
        }

        payload: list[FeatureCoverageResponse] = []
        for symbol in symbols:
            for timeframe in timeframes:
                item = by_key.get((symbol, timeframe))
                if item is None:
                    payload.append(
                        FeatureCoverageResponse(
                            exchange_code=normalized_exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            feature_count=0,
                            loaded_start_at=None,
                            loaded_end_at=None,
                        )
                    )
                else:
                    payload.append(
                        FeatureCoverageResponse(
                            exchange_code=normalized_exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            feature_count=int(item["feature_count"]),
                            loaded_start_at=ensure_utc(item["loaded_start_at"]) if item["loaded_start_at"] else None,
                            loaded_end_at=ensure_utc(item["loaded_end_at"]) if item["loaded_end_at"] else None,
                        )
                    )
        return payload

    def reset_workspace(self) -> dict[str, int]:
        deleted_runs = self.feature_repository.delete_all_runs()
        deleted_rows = self.feature_repository.delete_all_features()
        self.session.commit()
        logger.warning(
            "Feature workspace reset",
            extra={
                "deleted_feature_runs": deleted_runs,
                "deleted_feature_rows": deleted_rows,
            },
        )
        return {
            "deleted_feature_runs": deleted_runs,
            "deleted_feature_rows": deleted_rows,
        }

    def _build_run_response(self, run: FeatureRun) -> FeatureRunResponse:
        return FeatureRunResponse(
            id=run.id,
            exchange=run.exchange,
            symbol=run.symbol,
            timeframe=run.timeframe,
            lookback_days=run.lookback_days,
            start_at=run.start_at,
            end_at=run.end_at,
            status=run.status.value,
            source_candle_count=run.source_candle_count,
            feature_rows_upserted=run.feature_rows_upserted,
            computed_start_at=run.computed_start_at,
            computed_end_at=run.computed_end_at,
            error_text=run.error_text,
            created_at=run.created_at,
            updated_at=run.updated_at,
        )

    def _compute_feature_rows(self, *, source_candles: list[object], start_at) -> list[_FeatureRow]:
        rows: list[_FeatureRow] = []
        closes = [float(candle.close) for candle in source_candles]
        opens = [float(candle.open) for candle in source_candles]
        highs = [float(candle.high) for candle in source_candles]
        lows = [float(candle.low) for candle in source_candles]
        volumes = [float(candle.volume) for candle in source_candles]

        ema20_values = self._ema(closes, 20)
        ema50_values = self._ema(closes, 50)
        ema200_values = self._ema(closes, 200)
        true_ranges = self._true_ranges(highs, lows, closes)

        for index, candle in enumerate(source_candles):
            if candle.open_time < start_at:
                continue

            close = closes[index]
            open_price = opens[index]
            high = highs[index]
            low = lows[index]
            volume = volumes[index]
            candle_range_pct = (high - low) / close if close > 0 else None

            returns_window_20 = [self._return(closes[item - 1], closes[item]) for item in range(max(index - 19, 1), index + 1)]
            range_window_12 = [
                (highs[item] - lows[item]) / closes[item]
                for item in range(max(index - 11, 0), index + 1)
                if closes[item] > 0
            ]
            volume_window_20 = volumes[max(index - 19, 0) : index + 1]
            high_window_20 = highs[max(index - 19, 0) : index + 1]
            low_window_20 = lows[max(index - 19, 0) : index + 1]
            atr_window_14 = true_ranges[max(index - 13, 0) : index + 1]

            body = abs(close - open_price)
            upper_wick = max(high - max(open_price, close), 0.0)
            lower_wick = max(min(open_price, close) - low, 0.0)
            ema20 = ema20_values[index]
            ema50 = ema50_values[index]
            ema200 = ema200_values[index]

            realized_vol_base = self._safe_stdev(returns_window_20)
            realized_vol_20 = realized_vol_base * sqrt(len(returns_window_20)) if realized_vol_base is not None else None

            rows.append(
                _FeatureRow(
                    open_time=candle.open_time,
                    ret_1=self._series_return(closes, index, 1),
                    ret_3=self._series_return(closes, index, 3),
                    ret_12=self._series_return(closes, index, 12),
                    ret_48=self._series_return(closes, index, 48),
                    range_pct=candle_range_pct,
                    atr_pct=(sum(atr_window_14) / len(atr_window_14) / close) if atr_window_14 and close > 0 else None,
                    realized_vol_20=realized_vol_20,
                    body_pct=body / close if close > 0 else None,
                    upper_wick_pct=upper_wick / close if close > 0 else None,
                    lower_wick_pct=lower_wick / close if close > 0 else None,
                    distance_to_high_20_pct=((close - max(high_window_20)) / max(high_window_20)) if high_window_20 and max(high_window_20) > 0 else None,
                    distance_to_low_20_pct=((close - min(low_window_20)) / min(low_window_20)) if low_window_20 and min(low_window_20) > 0 else None,
                    ema20_dist_pct=((close - ema20) / ema20) if ema20 and ema20 > 0 else None,
                    ema50_dist_pct=((close - ema50) / ema50) if ema50 and ema50 > 0 else None,
                    ema200_dist_pct=((close - ema200) / ema200) if ema200 and ema200 > 0 else None,
                    ema20_slope_pct=self._slope_pct(ema20_values, index),
                    ema50_slope_pct=self._slope_pct(ema50_values, index),
                    ema200_slope_pct=self._slope_pct(ema200_values, index),
                    relative_volume_20=(volume / (sum(volume_window_20) / len(volume_window_20))) if volume_window_20 and sum(volume_window_20) > 0 else None,
                    volume_zscore_20=self._zscore(volume, volume_window_20),
                    compression_ratio_12=(min(range_window_12) / max(range_window_12)) if len(range_window_12) >= 2 and max(range_window_12) > 0 else None,
                    expansion_ratio_12=(range_window_12[-1] / (sum(range_window_12[:-1]) / len(range_window_12[:-1]))) if len(range_window_12) >= 2 and sum(range_window_12[:-1]) > 0 else None,
                )
            )

        return rows

    def _build_processing_windows(
        self,
        *,
        start_at: datetime,
        end_at: datetime,
        timeframe: str,
    ) -> list[tuple[datetime, datetime]]:
        timeframe_value = BinanceUSTimeframe.from_code(timeframe)
        chunk_days = FEATURE_CHUNK_DAYS.get(timeframe, 30)
        chunk_span = timedelta(days=chunk_days)

        windows: list[tuple[datetime, datetime]] = []
        current_start = start_at
        while current_start <= end_at:
            next_start = current_start + chunk_span
            chunk_end = min(end_at, next_start - timeframe_value.interval)
            if chunk_end < current_start:
                chunk_end = current_start
            windows.append((current_start, chunk_end))
            current_start = chunk_end + timeframe_value.interval
        return windows

    @staticmethod
    def _return(previous: float, current: float) -> float:
        if previous <= 0:
            return 0.0
        return (current - previous) / previous

    def _series_return(self, closes: list[float], index: int, bars: int) -> float | None:
        previous_index = index - bars
        if previous_index < 0:
            return None
        previous_close = closes[previous_index]
        current_close = closes[index]
        if previous_close <= 0:
            return None
        return (current_close - previous_close) / previous_close

    def _ema(self, values: list[float], period: int) -> list[float | None]:
        if not values:
            return []
        multiplier = 2 / (period + 1)
        ema_values: list[float | None] = []
        running: float | None = None
        for value in values:
            if running is None:
                running = value
            else:
                running = (value - running) * multiplier + running
            ema_values.append(running)
        return ema_values

    def _true_ranges(self, highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
        if not highs:
            return []
        ranges: list[float] = []
        for index, (high, low) in enumerate(zip(highs, lows)):
            if index == 0:
                ranges.append(high - low)
                continue
            previous_close = closes[index - 1]
            ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        return ranges

    def _slope_pct(self, values: list[float | None], index: int, bars: int = 5) -> float | None:
        previous_index = index - bars
        if previous_index < 0:
            return None
        current_value = values[index]
        previous_value = values[previous_index]
        if current_value is None or previous_value is None or previous_value == 0:
            return None
        return (current_value - previous_value) / previous_value

    def _zscore(self, current: float, window: list[float]) -> float | None:
        if len(window) < 2:
            return None
        mean_value = sum(window) / len(window)
        stdev = self._safe_stdev(window)
        if stdev is None or stdev == 0:
            return None
        return (current - mean_value) / stdev

    def _safe_stdev(self, values: list[float]) -> float | None:
        if len(values) < 2:
            return None
        return pstdev(values)
