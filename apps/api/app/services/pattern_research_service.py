from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from statistics import median
from typing import Callable, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Candle, Exchange, Symbol
from app.schemas.research import PatternSummaryResponse, ResearchCoverageResponse, ResearchSummaryResponse
from app.utils.symbols import supported_symbol_codes
from app.utils.time import utc_now


@dataclass(frozen=True)
class _PatternDefinition:
    code: str
    name: str


PATTERN_DEFINITIONS = (
    _PatternDefinition(code="range_breakout", name="Range Breakout"),
    _PatternDefinition(code="flush_reclaim", name="Flush Reclaim"),
    _PatternDefinition(code="compression_release", name="Compression Release"),
)

TIMEFRAME_SCAN_PRIORITY = {
    "4h": 0,
    "1h": 1,
    "15m": 2,
    "5m": 3,
    "1m": 4,
}


class PatternResearchService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_summary(
        self,
        exchange_code: str = "binance_us",
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        lookback_days: int = 730,
        forward_bars: int = 12,
        fee_pct: Decimal = Decimal("0.001"),
        slippage_pct: Decimal = Decimal("0.0005"),
        max_bars_per_series: int = 5000,
        progress_callback: Optional[Callable[[str, str, int, int], None]] = None,
    ) -> ResearchSummaryResponse:
        selected_symbols = [symbol for symbol in (symbols or list(supported_symbol_codes())[:8]) if symbol]
        selected_timeframes = [timeframe for timeframe in (timeframes or ["1m", "5m", "15m", "1h"]) if timeframe]
        generated_at = utc_now()
        scan_start_at = generated_at - timedelta(days=lookback_days)

        coverage = self._build_coverage(
            exchange_code=exchange_code,
            symbols=selected_symbols,
            timeframes=selected_timeframes,
            start_at=scan_start_at,
            end_at=generated_at,
            forward_bars=forward_bars,
        )
        pattern_rows = self._build_pattern_summaries(
            exchange_code=exchange_code,
            coverage=coverage,
            start_at=scan_start_at,
            end_at=generated_at,
            forward_bars=forward_bars,
            fee_pct=fee_pct,
            slippage_pct=slippage_pct,
            max_bars_per_series=max_bars_per_series,
            progress_callback=progress_callback,
        )

        notes = [
            "Research summary scans the loaded history that already exists in the database.",
            "Pattern outcomes use next-bar-forward close logic with simple roundtrip friction = 2 * (fee_pct + slippage_pct).",
            "For responsiveness, each symbol/timeframe scan is capped by max_bars_per_series. Once 2-year history is loaded, the same endpoint evaluates that wider window.",
        ]

        return ResearchSummaryResponse(
            generated_at=generated_at,
            exchange_code=exchange_code,
            lookback_days=lookback_days,
            forward_bars=forward_bars,
            fee_pct=fee_pct,
            slippage_pct=slippage_pct,
            max_bars_per_series=max_bars_per_series,
            coverage=coverage,
            patterns=pattern_rows,
            notes=notes,
        )

    def _build_coverage(
        self,
        exchange_code: str,
        symbols: list[str],
        timeframes: list[str],
        start_at,
        end_at,
        forward_bars: int,
    ) -> list[ResearchCoverageResponse]:
        exchange = self.session.scalar(select(Exchange).where(Exchange.code == exchange_code))
        if exchange is None:
            return []

        rows = self.session.execute(
            select(
                Symbol.code,
                Candle.timeframe,
                func.count(Candle.id),
                func.min(Candle.open_time),
                func.max(Candle.open_time),
            )
            .join(Symbol, Symbol.id == Candle.symbol_id)
            .where(
                Candle.exchange_id == exchange.id,
                Symbol.code.in_(symbols),
                Candle.timeframe.in_(timeframes),
                Candle.open_time >= start_at,
                Candle.open_time <= end_at,
            )
            .group_by(Symbol.code, Candle.timeframe)
            .order_by(Symbol.code.asc(), Candle.timeframe.asc())
        ).all()

        by_key = {(symbol, timeframe): row for symbol, timeframe, *row in rows}
        payload: list[ResearchCoverageResponse] = []
        minimum_bars = max(80, forward_bars + 25)

        for symbol in symbols:
            for timeframe in timeframes:
                match = by_key.get((symbol, timeframe))
                if match is None:
                    payload.append(
                        ResearchCoverageResponse(
                            symbol=symbol,
                            timeframe=timeframe,
                            candle_count=0,
                            completion_pct=Decimal("0"),
                            ready_for_pattern_scan=False,
                        )
                    )
                    continue

                candle_count, loaded_start_at, loaded_end_at = match
                completion_pct = Decimal("100") if candle_count >= minimum_bars else Decimal("0")
                payload.append(
                    ResearchCoverageResponse(
                        symbol=symbol,
                        timeframe=timeframe,
                        candle_count=int(candle_count or 0),
                        loaded_start_at=loaded_start_at,
                        loaded_end_at=loaded_end_at,
                        completion_pct=completion_pct,
                        ready_for_pattern_scan=int(candle_count or 0) >= minimum_bars,
                    )
                )
        return payload

    def _build_pattern_summaries(
        self,
        exchange_code: str,
        coverage: list[ResearchCoverageResponse],
        start_at,
        end_at,
        forward_bars: int,
        fee_pct: Decimal,
        slippage_pct: Decimal,
        max_bars_per_series: int,
        progress_callback: Optional[Callable[[str, str, int, int], None]] = None,
    ) -> list[PatternSummaryResponse]:
        exchange = self.session.scalar(select(Exchange).where(Exchange.code == exchange_code))
        if exchange is None:
            return []

        results: list[PatternSummaryResponse] = []
        friction_pct = float((fee_pct + slippage_pct) * 2)
        ordered_coverage = sorted(
            coverage,
            key=lambda item: (
                TIMEFRAME_SCAN_PRIORITY.get(item.timeframe, 99),
                item.symbol,
            ),
        )
        total = len(ordered_coverage)
        processed = 0

        for item in ordered_coverage:
            try:
                if progress_callback is not None:
                    progress_callback(item.symbol, item.timeframe, processed, total)

                if not item.ready_for_pattern_scan:
                    continue

                symbol = self.session.scalar(
                    select(Symbol).where(Symbol.exchange_id == exchange.id, Symbol.code == item.symbol)
                )
                if symbol is None:
                    continue

                candles = list(
                    self.session.scalars(
                        select(Candle)
                        .where(
                            Candle.exchange_id == exchange.id,
                            Candle.symbol_id == symbol.id,
                            Candle.timeframe == item.timeframe,
                            Candle.open_time >= start_at,
                            Candle.open_time <= end_at,
                        )
                        .order_by(Candle.open_time.desc())
                        .limit(max_bars_per_series)
                    )
                )
                candles.reverse()
                if len(candles) < max(80, forward_bars + 25):
                    continue

                for definition in PATTERN_DEFINITIONS:
                    forward_returns = self._scan_pattern(definition.code, candles, forward_bars)
                    if not forward_returns:
                        continue

                    net_returns = [value - friction_pct for value in forward_returns]
                    sample_size = len(net_returns)
                    avg_forward = sum(forward_returns) / sample_size
                    avg_net = sum(net_returns) / sample_size
                    win_rate = sum(1 for value in net_returns if value > 0) / sample_size * 100
                    verdict = "monitor"
                    if sample_size < 8:
                        verdict = "insufficient_sample"
                    elif avg_net > 0 and win_rate >= 52:
                        verdict = "candidate"
                    elif avg_net <= 0:
                        verdict = "not_profitable"

                    results.append(
                        PatternSummaryResponse(
                            pattern_code=definition.code,
                            pattern_name=definition.name,
                            symbol=item.symbol,
                            timeframe=item.timeframe,
                            sample_size=sample_size,
                            win_rate_pct=_to_percent_decimal(win_rate),
                            avg_forward_return_pct=_to_percent_decimal(avg_forward * 100),
                            median_forward_return_pct=_to_percent_decimal(median(forward_returns) * 100),
                            avg_net_return_pct=_to_percent_decimal(avg_net * 100),
                            best_forward_return_pct=_to_percent_decimal(max(forward_returns) * 100),
                            worst_forward_return_pct=_to_percent_decimal(min(forward_returns) * 100),
                            verdict=verdict,
                        )
                    )
            finally:
                processed += 1
                if progress_callback is not None:
                    progress_callback(item.symbol, item.timeframe, processed, total)

        results.sort(
            key=lambda item: (
                item.verdict != "candidate",
                float(item.avg_net_return_pct) * -1,
                item.sample_size * -1,
            )
        )
        return results[:36]

    def _scan_pattern(self, pattern_code: str, candles: list[Candle], forward_bars: int) -> list[float]:
        returns: list[float] = []
        for index in range(24, len(candles) - forward_bars):
            candle = candles[index]
            prev = candles[index - 1]
            future = candles[index + forward_bars]
            close = float(candle.close)
            open_price = float(candle.open)
            high = float(candle.high)
            low = float(candle.low)
            previous_window = candles[index - 12 : index]
            recent_window = candles[index - 6 : index]

            range_high = max(float(row.high) for row in previous_window)
            range_low = min(float(row.low) for row in previous_window)
            range_width_pct = (range_high - range_low) / range_low if range_low > 0 else 0
            recent_high = max(float(row.high) for row in recent_window)
            recent_low = min(float(row.low) for row in recent_window)
            recent_drawdown_pct = (close - recent_high) / recent_high if recent_high > 0 else 0
            recent_compression_pct = (recent_high - recent_low) / recent_low if recent_low > 0 else 0
            body_pct = abs(close - open_price) / open_price if open_price > 0 else 0

            matched = False
            if pattern_code == "range_breakout":
                matched = range_width_pct <= 0.025 and close > range_high and close > open_price
            elif pattern_code == "flush_reclaim":
                matched = recent_drawdown_pct <= -0.015 and close > float(prev.close) and close > open_price
            elif pattern_code == "compression_release":
                matched = recent_compression_pct <= 0.012 and close > range_high and body_pct >= 0.0035

            if not matched:
                continue

            forward_return = (float(future.close) - close) / close if close > 0 else 0
            returns.append(forward_return)
        return returns


def _to_percent_decimal(value: float) -> Decimal:
    return Decimal(str(round(value, 4)))
