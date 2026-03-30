from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pydantic import Field

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal
from app.strategies.registry import register_strategy

ZERO_DECIMAL = Decimal("0")
ONE_DECIMAL = Decimal("1")


class PatternCandidateConfig(BaseStrategyConfig):
    exit_after_bars: int = Field(default=12, ge=1, le=240)
    risk_reward_multiple: float = Field(default=1.8, gt=0)
    entry_cooldown_bars: int = Field(default=6, ge=0, le=240)
    range_width_threshold: float = Field(default=0.025, gt=0)
    flush_drawdown_threshold: float = Field(default=0.015, gt=0)
    compression_threshold: float = Field(default=0.012, gt=0)
    breakout_body_threshold: float = Field(default=0.0035, gt=0)


@dataclass(frozen=True)
class PatternSetupDescriptor:
    key: str
    name: str
    description: str
    pattern_code: str
    symbol: str
    timeframe: str
    exit_after_bars: int
    stop_loss_pct: float
    take_profit_pct: float
    position_size_pct: float = 0.1


class PatternCandidateStrategy(BaseStrategy):
    spot_only = True
    long_only = True
    status = "active"
    config_model = PatternCandidateConfig

    def __init__(self, descriptor: PatternSetupDescriptor) -> None:
        self.key = descriptor.key
        self.name = descriptor.name
        self.description = descriptor.description
        self.pattern_code = descriptor.pattern_code
        self.symbol = descriptor.symbol
        self.timeframe = descriptor.timeframe
        self.exit_after_bars = descriptor.exit_after_bars
        self._default_stop_loss_pct = descriptor.stop_loss_pct
        self._default_take_profit_pct = descriptor.take_profit_pct
        self._default_position_size_pct = descriptor.position_size_pct

    def default_config(self) -> dict[str, Any]:
        payload = super().default_config()
        payload.update(
            {
                "symbols": [self.symbol],
                "timeframes": [self.timeframe],
                "position_size_pct": self._default_position_size_pct,
                "stop_loss_pct": self._default_stop_loss_pct,
                "take_profit_pct": self._default_take_profit_pct,
                "exit_after_bars": self.exit_after_bars,
            }
        )
        return payload

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: PatternCandidateConfig | None = None,
    ) -> int:
        if timeframe != self.timeframe:
            return 32
        configured_horizon = strategy_config.exit_after_bars if strategy_config is not None else self.exit_after_bars
        return max(32, configured_horizon + 24)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self.parse_config(context.metadata.get("config", {}))
        if context.symbol != self.symbol or context.timeframe != self.timeframe:
            return StrategySignal(
                action="hold",
                reason="unsupported_stream",
                metadata={
                    "reason_skipped": "unsupported_stream",
                    "skip_reason_detail": f"{context.symbol}:{context.timeframe}",
                },
            )

        history = list(context.metadata.get("history", []))
        current_candle = context.metadata.get("current_candle")
        if current_candle is None or len(history) < 25:
            return StrategySignal(
                action="hold",
                reason="insufficient_history",
                metadata={
                    "reason_skipped": "insufficient_history",
                    "skip_reason_detail": f"history_bars={len(history)}",
                },
            )

        if context.metadata.get("has_position"):
            position = context.metadata.get("position") or {}
            bars_held = self._bars_held(position, context.metadata.get("bar_index"), context.timestamp, context.timeframe)
            if bars_held >= config.exit_after_bars:
                return StrategySignal(
                    action="exit",
                    reason="time_horizon_exit",
                    confidence=0.65,
                    metadata={
                        "bars_held": bars_held,
                        "target_horizon_bars": config.exit_after_bars,
                    },
                )
            return StrategySignal(action="hold", reason="position_open")

        current_bar_index = context.metadata.get("bar_index")

        match = self._match_pattern(history, current_candle, config)
        if not match["matched"]:
            return StrategySignal(
                action="hold",
                reason=match["reason"],
                metadata={
                    "reason_skipped": match["reason"],
                    "skip_reason_detail": match["detail"],
                },
            )

        stop_price = Decimal(str(match["stop_price"]))
        close_price = Decimal(str(current_candle.close))
        risk_distance = close_price - stop_price
        if risk_distance <= ZERO_DECIMAL:
            return StrategySignal(
                action="hold",
                reason="invalid_risk_geometry",
                metadata={
                    "reason_skipped": "invalid_risk_geometry",
                    "skip_reason_detail": "stop_not_below_close",
                },
            )

        take_profit_price = close_price + (risk_distance * Decimal(str(config.risk_reward_multiple)))
        return StrategySignal(
            action="enter",
            reason=self.pattern_code,
            confidence=match["confidence"],
            metadata={
                "pattern_code": self.pattern_code,
                "pattern_name": self.name,
                "stop_price": stop_price,
                "take_profit_price": take_profit_price,
                "match_context": match["context"],
                "entry_bar_index": current_bar_index,
            },
        )

    def _match_pattern(
        self,
        history: list[BacktestCandle],
        current_candle: BacktestCandle,
        config: PatternCandidateConfig,
    ) -> dict[str, Any]:
        previous_window = history[-13:-1]
        recent_window = history[-7:-1]
        previous_candle = history[-2]
        close = float(current_candle.close)
        open_price = float(current_candle.open)
        range_high = max(float(row.high) for row in previous_window)
        range_low = min(float(row.low) for row in previous_window)
        range_width_pct = (range_high - range_low) / range_low if range_low > 0 else 0
        recent_high = max(float(row.high) for row in recent_window)
        recent_low = min(float(row.low) for row in recent_window)
        recent_drawdown_pct = (close - recent_high) / recent_high if recent_high > 0 else 0
        recent_compression_pct = (recent_high - recent_low) / recent_low if recent_low > 0 else 0
        body_pct = abs(close - open_price) / open_price if open_price > 0 else 0

        context = {
            "range_width_pct": round(range_width_pct, 6),
            "recent_drawdown_pct": round(recent_drawdown_pct, 6),
            "recent_compression_pct": round(recent_compression_pct, 6),
            "body_pct": round(body_pct, 6),
            "range_high": range_high,
            "range_low": range_low,
            "recent_high": recent_high,
            "recent_low": recent_low,
        }

        if self.pattern_code == "range_breakout":
            matched = range_width_pct <= config.range_width_threshold and close > range_high and close > open_price
            return {
                "matched": matched,
                "reason": "range_breakout_not_confirmed" if not matched else "range_breakout_confirmed",
                "detail": f"range_width_pct={round(range_width_pct, 6)}",
                "stop_price": min(range_low, float(current_candle.low)),
                "confidence": 0.68 if matched else 0,
                "context": context,
            }

        if self.pattern_code == "flush_reclaim":
            matched = (
                recent_drawdown_pct <= -config.flush_drawdown_threshold
                and close > float(previous_candle.close)
                and close > open_price
            )
            return {
                "matched": matched,
                "reason": "flush_reclaim_not_confirmed" if not matched else "flush_reclaim_confirmed",
                "detail": f"recent_drawdown_pct={round(recent_drawdown_pct, 6)}",
                "stop_price": recent_low,
                "confidence": 0.7 if matched else 0,
                "context": context,
            }

        matched = (
            recent_compression_pct <= config.compression_threshold
            and close > range_high
            and body_pct >= config.breakout_body_threshold
        )
        return {
            "matched": matched,
            "reason": "compression_release_not_confirmed" if not matched else "compression_release_confirmed",
            "detail": f"recent_compression_pct={round(recent_compression_pct, 6)}",
            "stop_price": min(range_low, recent_low),
            "confidence": 0.69 if matched else 0,
            "context": context,
        }

    def _bars_held(
        self,
        position: dict[str, Any],
        current_bar_index: Any,
        timestamp,
        timeframe: str,
    ) -> int:
        entry_metadata = position.get("entry_metadata") if isinstance(position, dict) else None
        entry_bar_index = entry_metadata.get("entry_bar_index") if isinstance(entry_metadata, dict) else None
        if isinstance(entry_bar_index, int) and isinstance(current_bar_index, int):
            return max(0, current_bar_index - entry_bar_index)

        entry_time = position.get("entry_time")
        if entry_time is None or timestamp is None:
            return 0

        seconds = max(0, int((timestamp - entry_time).total_seconds()))
        duration_seconds = timeframe_to_seconds(timeframe)
        if duration_seconds <= 0:
            return 0
        return seconds // duration_seconds


def timeframe_to_seconds(timeframe: str) -> int:
    mapping = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
    }
    return mapping.get(timeframe, 0)


PATTERN_SETUPS: tuple[PatternSetupDescriptor, ...] = (
    PatternSetupDescriptor(
        key="avax_1h_compression_release",
        name="AVAX 1h Compression Release",
        description="Executable strategy candidate built from the approved AVAX-USDT 1h Compression Release setup.",
        pattern_code="compression_release",
        symbol="AVAX-USDT",
        timeframe="1h",
        exit_after_bars=24,
        stop_loss_pct=0.025,
        take_profit_pct=0.05,
    ),
    PatternSetupDescriptor(
        key="oneinch_1h_flush_reclaim",
        name="1INCH 1h Flush Reclaim",
        description="Executable strategy candidate built from the approved 1INCH-USDT 1h Flush Reclaim setup.",
        pattern_code="flush_reclaim",
        symbol="1INCH-USDT",
        timeframe="1h",
        exit_after_bars=12,
        stop_loss_pct=0.03,
        take_profit_pct=0.06,
    ),
    PatternSetupDescriptor(
        key="gala_1h_range_breakout",
        name="GALA 1h Range Breakout",
        description="Executable strategy candidate built from the approved GALA-USDT 1h Range Breakout setup.",
        pattern_code="range_breakout",
        symbol="GALA-USDT",
        timeframe="1h",
        exit_after_bars=12,
        stop_loss_pct=0.025,
        take_profit_pct=0.055,
    ),
    PatternSetupDescriptor(
        key="ada_1h_compression_release",
        name="ADA 1h Compression Release",
        description="Executable strategy candidate built from the approved ADA-USDT 1h Compression Release setup.",
        pattern_code="compression_release",
        symbol="ADA-USDT",
        timeframe="1h",
        exit_after_bars=12,
        stop_loss_pct=0.025,
        take_profit_pct=0.05,
    ),
    PatternSetupDescriptor(
        key="gala_1h_compression_release",
        name="GALA 1h Compression Release",
        description="Executable strategy candidate built from the approved GALA-USDT 1h Compression Release setup.",
        pattern_code="compression_release",
        symbol="GALA-USDT",
        timeframe="1h",
        exit_after_bars=12,
        stop_loss_pct=0.03,
        take_profit_pct=0.06,
    ),
    PatternSetupDescriptor(
        key="bnb_4h_range_breakout",
        name="BNB 4h Range Breakout",
        description="Executable strategy candidate built from the approved BNB-USDT 4h Range Breakout setup.",
        pattern_code="range_breakout",
        symbol="BNB-USDT",
        timeframe="4h",
        exit_after_bars=24,
        stop_loss_pct=0.035,
        take_profit_pct=0.08,
    ),
    PatternSetupDescriptor(
        key="oneinch_4h_flush_reclaim",
        name="1INCH 4h Flush Reclaim",
        description="Executable strategy candidate built from the approved 1INCH-USDT 4h Flush Reclaim setup.",
        pattern_code="flush_reclaim",
        symbol="1INCH-USDT",
        timeframe="4h",
        exit_after_bars=24,
        stop_loss_pct=0.04,
        take_profit_pct=0.09,
    ),
    PatternSetupDescriptor(
        key="gala_5m_flush_reclaim",
        name="GALA 5m Flush Reclaim",
        description="Executable strategy candidate built from the approved GALA-USDT 5m Flush Reclaim setup.",
        pattern_code="flush_reclaim",
        symbol="GALA-USDT",
        timeframe="5m",
        exit_after_bars=24,
        stop_loss_pct=0.015,
        take_profit_pct=0.03,
    ),
    PatternSetupDescriptor(
        key="iota_5m_flush_reclaim",
        name="IOTA 5m Flush Reclaim",
        description="Executable strategy candidate built from the approved IOTA-USDT 5m Flush Reclaim setup.",
        pattern_code="flush_reclaim",
        symbol="IOTA-USDT",
        timeframe="5m",
        exit_after_bars=24,
        stop_loss_pct=0.015,
        take_profit_pct=0.03,
    ),
    PatternSetupDescriptor(
        key="iota_15m_flush_reclaim",
        name="IOTA 15m Flush Reclaim",
        description="Executable strategy candidate built from the approved IOTA-USDT 15m Flush Reclaim setup.",
        pattern_code="flush_reclaim",
        symbol="IOTA-USDT",
        timeframe="15m",
        exit_after_bars=24,
        stop_loss_pct=0.02,
        take_profit_pct=0.04,
    ),
)


REGISTERED_PATTERN_CANDIDATES = tuple(register_strategy(PatternCandidateStrategy(descriptor)) for descriptor in PATTERN_SETUPS)
