from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from math import sqrt
from statistics import pstdev
from typing import Optional

from sqlalchemy.orm import Session

from app.models import FeatureRun
from app.repositories.candle_repository import CandleRepository
from app.repositories.feature_repository import FeatureRepository
from app.schemas.api import FeatureCoverageResponse, FeatureRunResponse
from app.utils.exchanges import normalize_exchange_code
from app.utils.symbols import normalize_supported_symbol
from app.utils.time import ensure_utc, utc_now
from app.integrations.binance_us import BinanceUSTimeframe


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


class FeatureLayerService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.candle_repository = CandleRepository(session)
        self.feature_repository = FeatureRepository(session)

    def run(
        self,
        *,
        exchange_code: str,
        symbol: str,
        timeframe: str,
        lookback_days: int,
    ) -> FeatureRunResponse:
        normalized_exchange = normalize_exchange_code(exchange_code)
        normalized_symbol = normalize_supported_symbol(symbol)
        timeframe_value = BinanceUSTimeframe.from_code(timeframe)

        end_at = utc_now()
        start_at = end_at - timedelta(days=lookback_days)
        warmup_bars = 240
        warmup_start_at = start_at - timeframe_value.interval * warmup_bars

        exchange = self.candle_repository.get_exchange(normalized_exchange)
        if exchange is None:
            raise ValueError(f"Exchange {normalized_exchange} is not loaded")

        symbol_row = self.candle_repository.get_symbol(exchange.id, normalized_symbol)
        if symbol_row is None:
            raise ValueError(f"Symbol {normalized_symbol} is not loaded")

        run = self.feature_repository.create_run(
            exchange=normalized_exchange,
            symbol=normalized_symbol,
            timeframe=timeframe_value.value,
            lookback_days=lookback_days,
            start_at=start_at,
            end_at=end_at,
        )
        self.session.commit()

        source_candles = []
        feature_rows_upserted = 0
        computed_start_at = None
        computed_end_at = None

        try:
            self.feature_repository.mark_running(run)
            self.session.commit()

            source_candles = self.candle_repository.list_candles(
                exchange_code=normalized_exchange,
                symbol_code=normalized_symbol,
                timeframe=timeframe_value.value,
                start_at=warmup_start_at,
                end_at=end_at,
            )

            feature_rows = self._compute_feature_rows(source_candles=source_candles, start_at=start_at)
            if feature_rows:
                feature_rows_upserted = self.feature_repository.upsert_features(
                    exchange_id=exchange.id,
                    symbol_id=symbol_row.id,
                    timeframe=timeframe_value.value,
                    rows=[row.to_payload() for row in feature_rows],
                )
                computed_start_at = feature_rows[0].open_time
                computed_end_at = feature_rows[-1].open_time

            self.feature_repository.mark_completed(
                run,
                source_candle_count=len(source_candles),
                feature_rows_upserted=feature_rows_upserted,
                computed_start_at=computed_start_at,
                computed_end_at=computed_end_at,
            )
            self.session.commit()
        except Exception as exc:
            self.session.rollback()
            persisted = self.session.get(FeatureRun, run.id) or run
            self.feature_repository.mark_failed(
                persisted,
                error_text=str(exc),
                source_candle_count=len(source_candles),
                feature_rows_upserted=feature_rows_upserted,
                computed_start_at=computed_start_at,
                computed_end_at=computed_end_at,
            )
            self.session.commit()
            raise

        return self._build_run_response(run)

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
        raw_rows = self.feature_repository.list_feature_coverages(exchange_id=exchange.id)
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
            recent_range_window_3 = range_window_12[-3:]

            ema20 = ema20_values[index]
            ema50 = ema50_values[index]
            ema200 = ema200_values[index]

            avg_tr_14 = self._mean(true_ranges[max(index - 13, 0) : index + 1])
            atr_pct = avg_tr_14 / close if close > 0 and avg_tr_14 is not None else None
            realized_vol_base = self._safe_stdev(returns_window_20) if returns_window_20 else None
            realized_vol_20 = realized_vol_base * sqrt(len(returns_window_20)) if realized_vol_base is not None else None

            rolling_high_20 = max(highs[max(index - 19, 0) : index + 1]) if index >= 0 else None
            rolling_low_20 = min(lows[max(index - 19, 0) : index + 1]) if index >= 0 else None
            avg_volume_20 = self._mean(volumes[max(index - 19, 0) : index + 1])
            volume_stdev_20 = self._safe_stdev(volumes[max(index - 19, 0) : index + 1])
            avg_range_12 = self._mean(range_window_12)

            rows.append(
                _FeatureRow(
                    open_time=candle.open_time,
                    ret_1=self._period_return(closes, index, 1),
                    ret_3=self._period_return(closes, index, 3),
                    ret_12=self._period_return(closes, index, 12),
                    ret_48=self._period_return(closes, index, 48),
                    range_pct=candle_range_pct,
                    atr_pct=atr_pct,
                    realized_vol_20=realized_vol_20,
                    body_pct=(close - open_price) / open_price if open_price > 0 else None,
                    upper_wick_pct=(high - max(open_price, close)) / open_price if open_price > 0 else None,
                    lower_wick_pct=(min(open_price, close) - low) / open_price if open_price > 0 else None,
                    distance_to_high_20_pct=(rolling_high_20 - close) / close if close > 0 and rolling_high_20 is not None else None,
                    distance_to_low_20_pct=(close - rolling_low_20) / close if close > 0 and rolling_low_20 is not None else None,
                    ema20_dist_pct=(close - ema20) / ema20 if ema20 else None,
                    ema50_dist_pct=(close - ema50) / ema50 if ema50 else None,
                    ema200_dist_pct=(close - ema200) / ema200 if ema200 else None,
                    ema20_slope_pct=(ema20 - ema20_values[index - 1]) / ema20_values[index - 1] if index > 0 and ema20_values[index - 1] else None,
                    ema50_slope_pct=(ema50 - ema50_values[index - 1]) / ema50_values[index - 1] if index > 0 and ema50_values[index - 1] else None,
                    ema200_slope_pct=(ema200 - ema200_values[index - 1]) / ema200_values[index - 1] if index > 0 and ema200_values[index - 1] else None,
                    relative_volume_20=volume / avg_volume_20 if avg_volume_20 else None,
                    volume_zscore_20=(volume - avg_volume_20) / volume_stdev_20 if avg_volume_20 is not None and volume_stdev_20 not in (None, 0) else None,
                    compression_ratio_12=candle_range_pct / avg_range_12 if candle_range_pct is not None and avg_range_12 not in (None, 0) else None,
                    expansion_ratio_12=self._mean(recent_range_window_3) / avg_range_12 if avg_range_12 not in (None, 0) and recent_range_window_3 else None,
                )
            )

        return rows

    def _period_return(self, values: list[float], index: int, period: int) -> float | None:
        if index < period:
            return None
        previous = values[index - period]
        current = values[index]
        return self._return(previous, current)

    def _return(self, previous: float, current: float) -> float | None:
        if previous <= 0:
            return None
        return (current - previous) / previous

    def _ema(self, values: list[float], period: int) -> list[float]:
        if not values:
            return []
        alpha = 2 / (period + 1)
        ema_values: list[float] = [values[0]]
        for value in values[1:]:
            ema_values.append((value * alpha) + (ema_values[-1] * (1 - alpha)))
        return ema_values

    def _true_ranges(self, highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
        values: list[float] = []
        for index, (high, low) in enumerate(zip(highs, lows)):
            previous_close = closes[index - 1] if index > 0 else closes[index]
            values.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        return values

    def _mean(self, values: list[float]) -> float | None:
        if not values:
            return None
        return sum(values) / len(values)

    def _safe_stdev(self, values: list[float | None]) -> float | None:
        numeric_values = [float(value) for value in values if value is not None]
        if len(numeric_values) < 2:
            return None
        return pstdev(numeric_values)
