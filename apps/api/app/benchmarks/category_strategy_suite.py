from __future__ import annotations

from statistics import pstdev
from typing import Any, Optional, Sequence

from pydantic import Field

from app.schemas.backtest import BacktestCandle
from app.strategies.base import BaseStrategy, BaseStrategyConfig, StrategyContext, StrategySignal

EPSILON = 1e-9


class ResearchStrategyBase(BaseStrategy):
    status = "research"

    def _config(self, context: StrategyContext) -> BaseStrategyConfig:
        raw_config = context.metadata.get("config")
        if isinstance(raw_config, self.config_model):
            return raw_config
        if hasattr(raw_config, "model_dump"):
            return self.parse_config(raw_config.model_dump())
        if isinstance(raw_config, dict):
            return self.parse_config(raw_config)
        return self.config_model()

    def _history(self, context: StrategyContext) -> list[BacktestCandle]:
        raw_history = context.metadata.get("history", [])
        if isinstance(raw_history, Sequence):
            return list(raw_history)
        return list(raw_history or [])

    def _has_position(self, context: StrategyContext) -> bool:
        return bool(context.metadata.get("has_position"))

    def _position_age_bars(self, context: StrategyContext) -> Optional[int]:
        position = context.metadata.get("position")
        bar_index = context.metadata.get("bar_index")
        if not isinstance(position, dict) or bar_index is None:
            return None
        entry_bar_index = position.get("entry_bar_index")
        if entry_bar_index is None:
            return None
        return int(bar_index) - int(entry_bar_index)

    def _hold(self, reason: str, **metadata: Any) -> StrategySignal:
        return StrategySignal(action="hold", reason=reason, metadata=metadata)

    def _enter(
        self,
        reason: str,
        stop_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
        **metadata: Any,
    ) -> StrategySignal:
        payload = dict(metadata)
        if stop_price is not None:
            payload["stop_price"] = self._format_price(stop_price)
        if take_profit_price is not None:
            payload["take_profit_price"] = self._format_price(take_profit_price)
        return StrategySignal(action="enter", reason=reason, metadata=payload)

    def _exit(self, reason: str, **metadata: Any) -> StrategySignal:
        return StrategySignal(action="exit", reason=reason, metadata=metadata)

    @staticmethod
    def _format_price(price: float) -> str:
        return f"{price:.8f}"

    @staticmethod
    def _close_series(history: Sequence[BacktestCandle]) -> list[float]:
        return [float(candle.close) for candle in history]

    @staticmethod
    def _average_volume(history: Sequence[BacktestCandle], period: int) -> Optional[float]:
        if len(history) < period or period <= 0:
            return None
        window = history[-period:]
        return sum(float(candle.volume) for candle in window) / float(period)

    @staticmethod
    def _average_dollar_volume(history: Sequence[BacktestCandle], period: int) -> Optional[float]:
        if len(history) < period or period <= 0:
            return None
        window = history[-period:]
        return sum(float(candle.close) * float(candle.volume) for candle in window) / float(period)

    @staticmethod
    def _highest_high(history: Sequence[BacktestCandle], period: int) -> Optional[float]:
        if len(history) < period or period <= 0:
            return None
        return max(float(candle.high) for candle in history[-period:])

    @staticmethod
    def _lowest_low(history: Sequence[BacktestCandle], period: int) -> Optional[float]:
        if len(history) < period or period <= 0:
            return None
        return min(float(candle.low) for candle in history[-period:])

    @staticmethod
    def _sma(values: Sequence[float], period: int) -> Optional[float]:
        if len(values) < period or period <= 0:
            return None
        sample = values[-period:]
        return sum(sample) / float(period)

    @staticmethod
    def _ema(values: Sequence[float], period: int) -> Optional[float]:
        if len(values) < period or period <= 0:
            return None
        multiplier = 2.0 / float(period + 1)
        ema_value = sum(values[:period]) / float(period)
        for value in values[period:]:
            ema_value = (value * multiplier) + (ema_value * (1.0 - multiplier))
        return ema_value

    @staticmethod
    def _stddev(values: Sequence[float], period: int) -> Optional[float]:
        if len(values) < period or period <= 0:
            return None
        sample = values[-period:]
        if len(sample) < 2:
            return 0.0
        return float(pstdev(sample))

    def _atr(self, history: Sequence[BacktestCandle], period: int) -> Optional[float]:
        if len(history) < period + 1 or period <= 0:
            return None

        true_ranges: list[float] = []
        start_index = len(history) - period
        previous_close = self._candle_close(history[start_index - 1])
        for candle in history[start_index:]:
            high = self._candle_high(candle)
            low = self._candle_low(candle)
            true_ranges.append(
                max(
                    high - low,
                    abs(high - previous_close),
                    abs(low - previous_close),
                )
            )
            previous_close = self._candle_close(candle)
        return sum(true_ranges) / float(period)

    @staticmethod
    def _pct_change(current: float, previous: float) -> float:
        if abs(previous) <= EPSILON:
            return 0.0
        return (current - previous) / previous

    @staticmethod
    def _range_location(close_price: float, range_low: float, range_high: float) -> Optional[float]:
        width = range_high - range_low
        if width <= EPSILON:
            return None
        return (close_price - range_low) / width

    @staticmethod
    def _close_location_in_candle(candle: BacktestCandle) -> float:
        high = float(candle.high)
        low = float(candle.low)
        width = high - low
        if width <= EPSILON:
            return 0.5
        return (float(candle.close) - low) / width

    @staticmethod
    def _candle_open(candle: BacktestCandle) -> float:
        return float(candle.open)

    @staticmethod
    def _candle_close(candle: BacktestCandle) -> float:
        return float(candle.close)

    @staticmethod
    def _candle_low(candle: BacktestCandle) -> float:
        return float(candle.low)

    @staticmethod
    def _candle_high(candle: BacktestCandle) -> float:
        return float(candle.high)


class TrendFollowingConfig(BaseStrategyConfig):
    fast_ema_period: int = Field(default=50, ge=2)
    slow_ema_period: int = Field(default=200, ge=3)
    breakout_lookback: int = Field(default=20, ge=5)
    exit_ema_period: int = Field(default=20, ge=2)
    min_trend_gap_pct: float = Field(default=0.01, ge=0)
    slope_lookback_bars: int = Field(default=12, ge=1)
    min_slow_ema_slope_pct: float = Field(default=0.003, ge=0)
    volume_period: int = Field(default=20, ge=2)
    min_volume_multiple: float = Field(default=1.0, gt=0)
    min_average_dollar_volume: float = Field(default=200000.0, ge=0)
    atr_period: int = Field(default=14, ge=2)
    min_atr_pct: float = Field(default=0.01, ge=0)
    max_atr_pct: float = Field(default=0.08, ge=0)
    breakout_buffer_pct: float = Field(default=0.001, ge=0)
    breakout_min_body_pct: float = Field(default=0.002, ge=0)
    breakout_min_close_location: float = Field(default=0.6, ge=0, le=1)
    recent_pullback_lookback: int = Field(default=12, ge=2)
    pullback_proximity_pct: float = Field(default=0.01, ge=0)
    max_extension_above_fast_ema_pct: float = Field(default=0.04, ge=0)
    stop_buffer_pct: float = Field(default=0.003, ge=0)
    max_bars_in_trade: int = Field(default=240, ge=2)
    stop_loss_pct: float = 0.05
    take_profit_pct: float = 0.0


class TrendFollowingStrategy(ResearchStrategyBase):
    key = "trend_following_research"
    name = "TrendFollowingResearch"
    description = "Long-only trend-following model using EMA alignment and breakout confirmation."
    config_model = TrendFollowingConfig

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: TrendFollowingConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return max(
            config.slow_ema_period + config.slope_lookback_bars + 5,
            config.breakout_lookback + 5,
            config.exit_ema_period + 5,
            config.volume_period + 5,
            config.atr_period + 5,
            config.recent_pullback_lookback + 5,
        )

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        closes = self._close_series(history)
        current_bar = history[-1]
        current_close = closes[-1]
        current_open = self._candle_open(current_bar)
        previous_close = closes[-2]
        fast_ema = self._ema(closes, config.fast_ema_period)
        slow_ema = self._ema(closes, config.slow_ema_period)
        exit_ema = self._ema(closes, config.exit_ema_period)
        breakout_level = self._highest_high(history[:-1], config.breakout_lookback)
        slow_ema_reference = self._ema(closes[:-config.slope_lookback_bars], config.slow_ema_period)
        average_volume = self._average_volume(history[:-1], config.volume_period)
        average_dollar_volume = self._average_dollar_volume(history[:-1], config.volume_period)
        atr_value = self._atr(history, config.atr_period)
        recent_pullback_low = self._lowest_low(history[:-1], config.recent_pullback_lookback)
        recent_swing_low = self._lowest_low(
            history[:-1],
            max(config.breakout_lookback, config.recent_pullback_lookback),
        )
        if None in {
            fast_ema,
            slow_ema,
            exit_ema,
            breakout_level,
            slow_ema_reference,
            average_volume,
            average_dollar_volume,
            atr_value,
            recent_pullback_low,
            recent_swing_low,
        }:
            return self._hold("indicator_not_ready")

        trend_gap_pct = self._pct_change(fast_ema, slow_ema)
        slow_ema_slope_pct = self._pct_change(slow_ema, slow_ema_reference)
        atr_pct = atr_value / current_close if current_close > EPSILON else 0.0
        extension_above_fast_ema_pct = (
            (current_close - fast_ema) / fast_ema if fast_ema > EPSILON else 0.0
        )
        close_location = self._close_location_in_candle(current_bar)
        if self._has_position(context):
            age = self._position_age_bars(context) or 0
            if age >= config.max_bars_in_trade:
                return self._exit("time_stop")
            if current_close < exit_ema or fast_ema < slow_ema:
                return self._exit("trend_break")
            return self._hold("trend_hold")

        if fast_ema <= slow_ema:
            return self._hold("trend_not_up")
        if trend_gap_pct < config.min_trend_gap_pct:
            return self._hold("trend_not_strong_enough")
        if slow_ema_slope_pct < config.min_slow_ema_slope_pct:
            return self._hold("trend_not_persistent")
        if average_volume <= EPSILON or float(current_bar.volume) < (average_volume * config.min_volume_multiple):
            return self._hold("volume_not_confirmed")
        if average_dollar_volume < config.min_average_dollar_volume:
            return self._hold("liquidity_too_low")
        if atr_pct < config.min_atr_pct or atr_pct > config.max_atr_pct:
            return self._hold("volatility_not_in_band")
        if recent_pullback_low > (fast_ema * (1.0 + config.pullback_proximity_pct)):
            return self._hold("pullback_not_recent_enough")
        if extension_above_fast_ema_pct > config.max_extension_above_fast_ema_pct:
            return self._hold("trend_too_extended")
        if current_close <= breakout_level * (1.0 + config.breakout_buffer_pct):
            return self._hold("breakout_not_confirmed")
        if current_close <= previous_close:
            return self._hold("follow_through_not_confirmed")
        body_pct = (
            (current_close - current_open) / current_open
            if current_close > current_open and current_open > EPSILON
            else 0.0
        )
        if body_pct < config.breakout_min_body_pct:
            return self._hold("breakout_bar_too_weak")
        if close_location < config.breakout_min_close_location:
            return self._hold("close_not_strong_enough")

        structure_stop = recent_swing_low * (1.0 - config.stop_buffer_pct)
        capped_stop = current_close * (1.0 - config.stop_loss_pct)
        stop_price = max(structure_stop, capped_stop)
        if stop_price >= current_close:
            return self._hold("stop_not_valid")
        return self._enter(
            "trend_breakout_entry",
            stop_price=stop_price,
            trend_gap_pct=trend_gap_pct,
            slow_ema_slope_pct=slow_ema_slope_pct,
            atr_pct=atr_pct,
            average_dollar_volume=average_dollar_volume,
        )


class MeanReversionConfig(BaseStrategyConfig):
    lookback: int = Field(default=20, ge=5)
    entry_zscore: float = Field(default=2.0, gt=0)
    stop_buffer_pct: float = Field(default=0.003, ge=0)
    max_bars_in_trade: int = Field(default=18, ge=1)
    stop_loss_pct: float = 0.02
    take_profit_pct: float = 0.0


class MeanReversionStrategy(ResearchStrategyBase):
    key = "mean_reversion_research"
    name = "MeanReversionResearch"
    description = "Long-only mean-reversion model using z-score dislocation and bounce confirmation."
    config_model = MeanReversionConfig

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: MeanReversionConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return config.lookback + 5

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        closes = self._close_series(history)
        current_bar = history[-1]
        current_close = self._candle_close(current_bar)
        current_open = self._candle_open(current_bar)
        previous_close = closes[-2]
        mean_price = self._sma(closes, config.lookback)
        stddev = self._stddev(closes, config.lookback)
        local_low = self._lowest_low(history, min(config.lookback, 5))
        if mean_price is None or stddev is None or local_low is None:
            return self._hold("indicator_not_ready")
        if stddev <= EPSILON:
            return self._hold("range_not_wide_enough")

        zscore = (current_close - mean_price) / stddev
        if self._has_position(context):
            age = self._position_age_bars(context) or 0
            if current_close >= mean_price:
                return self._exit("mean_reached")
            if age >= config.max_bars_in_trade:
                return self._exit("time_stop")
            return self._hold("mean_reversion_hold")

        if zscore > (-1.0 * config.entry_zscore):
            return self._hold("stretch_not_large_enough")
        if current_close <= current_open or current_close <= previous_close:
            return self._hold("reclaim_not_confirmed")

        stop_price = local_low * (1.0 - config.stop_buffer_pct)
        take_profit_price = mean_price
        if stop_price <= 0 or stop_price >= current_close:
            return self._hold("stop_not_valid")
        if take_profit_price <= current_close:
            return self._hold("target_not_valid")
        return self._enter(
            "mean_reversion_entry",
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            zscore=zscore,
        )


class BreakoutConfig(BaseStrategyConfig):
    breakout_lookback: int = Field(default=20, ge=5)
    compression_lookback: int = Field(default=20, ge=5)
    max_range_width_pct: float = Field(default=0.04, gt=0)
    volume_period: int = Field(default=20, ge=2)
    min_volume_multiple: float = Field(default=1.2, gt=0)
    exit_ema_period: int = Field(default=20, ge=2)
    max_bars_in_trade: int = Field(default=48, ge=1)
    stop_loss_pct: float = 0.03
    take_profit_pct: float = 0.0


class BreakoutStrategy(ResearchStrategyBase):
    key = "breakout_research"
    name = "BreakoutResearch"
    description = "Long-only breakout model after range compression and volume expansion."
    config_model = BreakoutConfig

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: BreakoutConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return max(
            config.breakout_lookback + 5,
            config.compression_lookback + 5,
            config.volume_period + 5,
            config.exit_ema_period + 5,
        )

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        closes = self._close_series(history)
        current_bar = history[-1]
        current_close = self._candle_close(current_bar)
        current_open = self._candle_open(current_bar)
        current_volume = float(current_bar.volume)
        previous_close = closes[-2]
        breakout_level = self._highest_high(history[:-1], config.breakout_lookback)
        range_high = self._highest_high(history[:-1], config.compression_lookback)
        range_low = self._lowest_low(history[:-1], config.compression_lookback)
        average_volume = self._average_volume(history[:-1], config.volume_period)
        exit_ema = self._ema(closes, config.exit_ema_period)
        if None in {breakout_level, range_high, range_low, average_volume, exit_ema}:
            return self._hold("indicator_not_ready")
        if range_low <= EPSILON:
            return self._hold("invalid_range")

        range_width_pct = (range_high - range_low) / range_low
        if self._has_position(context):
            age = self._position_age_bars(context) or 0
            if age >= config.max_bars_in_trade:
                return self._exit("time_stop")
            if current_close < exit_ema or current_close < breakout_level:
                return self._exit("breakout_failed")
            return self._hold("breakout_hold")

        if range_width_pct > config.max_range_width_pct:
            return self._hold("range_not_tight_enough")
        if current_close <= breakout_level:
            return self._hold("breakout_not_confirmed")
        if current_close <= current_open or current_close <= previous_close:
            return self._hold("breakout_bar_too_weak")
        if average_volume <= EPSILON or current_volume < (average_volume * config.min_volume_multiple):
            return self._hold("volume_not_confirmed")
        return self._enter("breakout_entry", range_width_pct=range_width_pct)


class PullbackConfig(BaseStrategyConfig):
    fast_ema_period: int = Field(default=20, ge=2)
    slow_ema_period: int = Field(default=50, ge=3)
    signal_ema_period: int = Field(default=10, ge=2)
    pullback_lookback: int = Field(default=20, ge=5)
    min_pullback_pct: float = Field(default=0.01, ge=0)
    max_pullback_pct: float = Field(default=0.05, gt=0)
    stop_buffer_pct: float = Field(default=0.003, ge=0)
    max_bars_in_trade: int = Field(default=72, ge=1)
    stop_loss_pct: float = 0.025
    take_profit_pct: float = 0.0


class PullbackStrategy(ResearchStrategyBase):
    key = "pullback_research"
    name = "PullbackResearch"
    description = "Long-only pullback model that buys retracements inside an uptrend."
    config_model = PullbackConfig

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: PullbackConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return max(config.slow_ema_period + 5, config.pullback_lookback + 5, config.signal_ema_period + 5)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        closes = self._close_series(history)
        current_bar = history[-1]
        current_close = self._candle_close(current_bar)
        previous_close = closes[-2]
        current_low = self._candle_low(current_bar)
        fast_ema = self._ema(closes, config.fast_ema_period)
        slow_ema = self._ema(closes, config.slow_ema_period)
        signal_ema = self._ema(closes, config.signal_ema_period)
        recent_high = self._highest_high(history[:-1], config.pullback_lookback)
        local_low = self._lowest_low(history, min(config.pullback_lookback, 8))
        if None in {fast_ema, slow_ema, signal_ema, recent_high, local_low}:
            return self._hold("indicator_not_ready")
        if recent_high <= EPSILON:
            return self._hold("invalid_range")

        pullback_pct = (recent_high - current_close) / recent_high
        if self._has_position(context):
            age = self._position_age_bars(context) or 0
            if age >= config.max_bars_in_trade:
                return self._exit("time_stop")
            if current_close < slow_ema:
                return self._exit("trend_break")
            return self._hold("pullback_hold")

        if fast_ema <= slow_ema:
            return self._hold("trend_not_up")
        if pullback_pct < config.min_pullback_pct or pullback_pct > config.max_pullback_pct:
            return self._hold("pullback_depth_invalid")
        if current_low > fast_ema:
            return self._hold("pullback_not_detected")
        if current_close <= signal_ema or current_close <= previous_close:
            return self._hold("trigger_not_confirmed")

        stop_price = local_low * (1.0 - config.stop_buffer_pct)
        take_profit_price = recent_high
        if stop_price <= 0 or stop_price >= current_close:
            return self._hold("stop_not_valid")
        if take_profit_price <= current_close:
            return self._hold("target_not_valid")
        return self._enter(
            "pullback_entry",
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            pullback_pct=pullback_pct,
        )


class RangeTradingConfig(BaseStrategyConfig):
    range_lookback: int = Field(default=30, ge=5)
    min_range_width_pct: float = Field(default=0.02, ge=0)
    max_range_width_pct: float = Field(default=0.08, gt=0)
    max_center_shift_pct: float = Field(default=0.01, ge=0)
    entry_zone_pct: float = Field(default=0.25, gt=0, lt=1)
    exit_zone_pct: float = Field(default=0.75, gt=0, lt=1)
    stop_buffer_pct: float = Field(default=0.003, ge=0)
    max_bars_in_trade: int = Field(default=24, ge=1)
    stop_loss_pct: float = 0.0
    take_profit_pct: float = 0.0


class RangeTradingStrategy(ResearchStrategyBase):
    key = "range_trading_research"
    name = "RangeTradingResearch"
    description = "Long-only range model buying the lower edge of a sideways channel."
    config_model = RangeTradingConfig

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: RangeTradingConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return config.range_lookback + 5

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        current_bar = history[-1]
        current_close = self._candle_close(current_bar)
        previous_close = self._candle_close(history[-2])
        range_high = self._highest_high(history, config.range_lookback)
        range_low = self._lowest_low(history, config.range_lookback)
        if range_high is None or range_low is None or range_low <= EPSILON:
            return self._hold("indicator_not_ready")

        range_width_pct = (range_high - range_low) / range_low
        location = self._range_location(current_close, range_low, range_high)
        closes = self._close_series(history[-config.range_lookback :])
        midpoint = len(closes) // 2
        first_half_mean = sum(closes[:midpoint]) / float(max(midpoint, 1))
        second_half_mean = sum(closes[midpoint:]) / float(max(len(closes) - midpoint, 1))
        center_shift_pct = abs(self._pct_change(second_half_mean, first_half_mean))
        if location is None:
            return self._hold("range_not_valid")

        if self._has_position(context):
            age = self._position_age_bars(context) or 0
            if age >= config.max_bars_in_trade:
                return self._exit("time_stop")
            if location >= config.exit_zone_pct:
                return self._exit("range_target_reached")
            if range_width_pct > config.max_range_width_pct or center_shift_pct > config.max_center_shift_pct:
                return self._exit("range_broken")
            return self._hold("range_hold")

        if range_width_pct < config.min_range_width_pct or range_width_pct > config.max_range_width_pct:
            return self._hold("range_not_tight_enough")
        if center_shift_pct > config.max_center_shift_pct:
            return self._hold("trend_detected")
        if location > config.entry_zone_pct:
            return self._hold("not_near_support")
        if current_close <= previous_close:
            return self._hold("bounce_not_confirmed")

        stop_price = range_low * (1.0 - config.stop_buffer_pct)
        take_profit_price = range_low + ((range_high - range_low) * config.exit_zone_pct)
        if stop_price <= 0 or stop_price >= current_close:
            return self._hold("stop_not_valid")
        if take_profit_price <= current_close:
            return self._hold("target_not_valid")
        return self._enter(
            "range_entry",
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            range_width_pct=range_width_pct,
            location=location,
        )


class MomentumConfig(BaseStrategyConfig):
    trend_ema_period: int = Field(default=50, ge=2)
    exit_ema_period: int = Field(default=20, ge=2)
    impulse_lookback: int = Field(default=4, ge=1)
    min_return_pct: float = Field(default=0.015, gt=0)
    volume_period: int = Field(default=20, ge=2)
    min_volume_multiple: float = Field(default=1.2, gt=0)
    max_bars_in_trade: int = Field(default=12, ge=1)
    stop_loss_pct: float = 0.02
    take_profit_pct: float = 0.04


class MomentumStrategy(ResearchStrategyBase):
    key = "momentum_research"
    name = "MomentumResearch"
    description = "Long-only momentum model buying short-term acceleration with volume."
    config_model = MomentumConfig

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: MomentumConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return max(
            config.trend_ema_period + 5,
            config.exit_ema_period + 5,
            config.impulse_lookback + 5,
            config.volume_period + 5,
        )

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        closes = self._close_series(history)
        current_bar = history[-1]
        current_close = self._candle_close(current_bar)
        current_volume = float(current_bar.volume)
        previous_high = self._candle_high(history[-2])
        trend_ema = self._ema(closes, config.trend_ema_period)
        exit_ema = self._ema(closes, config.exit_ema_period)
        average_volume = self._average_volume(history[:-1], config.volume_period)
        impulse_reference = closes[-(config.impulse_lookback + 1)]
        impulse_return_pct = self._pct_change(current_close, impulse_reference)
        if None in {trend_ema, exit_ema, average_volume}:
            return self._hold("indicator_not_ready")

        if self._has_position(context):
            age = self._position_age_bars(context) or 0
            if age >= config.max_bars_in_trade:
                return self._exit("time_stop")
            if current_close < exit_ema:
                return self._exit("momentum_lost")
            return self._hold("momentum_hold")

        if current_close <= trend_ema:
            return self._hold("below_trend_filter")
        if impulse_return_pct < config.min_return_pct:
            return self._hold("impulse_too_weak")
        if average_volume <= EPSILON or current_volume < (average_volume * config.min_volume_multiple):
            return self._hold("volume_not_confirmed")
        if current_close <= previous_high:
            return self._hold("follow_through_not_confirmed")
        return self._enter("momentum_entry", impulse_return_pct=impulse_return_pct)


class ScalpingConfig(BaseStrategyConfig):
    trend_ema_period: int = Field(default=30, ge=2)
    micro_ema_period: int = Field(default=9, ge=2)
    dip_threshold_pct: float = Field(default=0.002, gt=0)
    reclaim_buffer_pct: float = Field(default=0.0005, ge=0)
    volume_period: int = Field(default=20, ge=2)
    min_volume_multiple: float = Field(default=0.8, gt=0)
    max_bars_in_trade: int = Field(default=4, ge=1)
    stop_loss_pct: float = 0.004
    take_profit_pct: float = 0.007


class ScalpingStrategy(ResearchStrategyBase):
    key = "scalping_research"
    name = "ScalpingResearch"
    description = "Long-only scalp model for very short reclaim setups after a micro pullback."
    config_model = ScalpingConfig

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: ScalpingConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        return max(config.trend_ema_period + 5, config.micro_ema_period + 5, config.volume_period + 5)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        closes = self._close_series(history)
        current_bar = history[-1]
        current_close = self._candle_close(current_bar)
        previous_close = self._candle_close(history[-2])
        current_low = self._candle_low(current_bar)
        current_volume = float(current_bar.volume)
        trend_ema = self._ema(closes, config.trend_ema_period)
        micro_ema = self._ema(closes, config.micro_ema_period)
        average_volume = self._average_volume(history[:-1], config.volume_period)
        if None in {trend_ema, micro_ema, average_volume}:
            return self._hold("indicator_not_ready")

        if self._has_position(context):
            age = self._position_age_bars(context) or 0
            if age >= config.max_bars_in_trade:
                return self._exit("time_stop")
            if current_close < micro_ema:
                return self._exit("micro_trend_lost")
            return self._hold("scalp_hold")

        if current_close <= trend_ema:
            return self._hold("below_trend_filter")
        if current_low > (micro_ema * (1.0 - config.dip_threshold_pct)):
            return self._hold("micro_dip_not_detected")
        if current_close < (micro_ema * (1.0 + config.reclaim_buffer_pct)):
            return self._hold("reclaim_not_confirmed")
        if current_close <= previous_close:
            return self._hold("no_uptick")
        if average_volume <= EPSILON or current_volume < (average_volume * config.min_volume_multiple):
            return self._hold("volume_too_low")
        return self._enter("scalp_entry")


class RegimeAwareConfig(BaseStrategyConfig):
    regime_fast_ema_period: int = Field(default=30, ge=2)
    regime_slow_ema_period: int = Field(default=120, ge=3)
    regime_slope_lookback_bars: int = Field(default=10, ge=1)
    trend_min_gap_pct: float = Field(default=0.006, ge=0)
    trend_min_slope_pct: float = Field(default=0.003, ge=0)
    flat_range_lookback: int = Field(default=30, ge=5)
    flat_max_width_pct: float = Field(default=0.06, gt=0)
    flat_max_center_shift_pct: float = Field(default=0.008, ge=0)
    flat_min_atr_pct: float = Field(default=0.006, ge=0)
    flat_max_atr_pct: float = Field(default=0.05, ge=0)
    atr_period: int = Field(default=14, ge=2)
    min_average_dollar_volume: float = Field(default=200000.0, ge=0)
    volume_period: int = Field(default=20, ge=2)
    trend_config: dict[str, Any] = Field(
        default_factory=lambda: {
            "fast_ema_period": 20,
            "slow_ema_period": 100,
            "breakout_lookback": 20,
            "exit_ema_period": 20,
            "min_trend_gap_pct": 0.005,
            "slope_lookback_bars": 10,
            "min_slow_ema_slope_pct": 0.003,
            "volume_period": 20,
            "min_volume_multiple": 0.9,
            "min_average_dollar_volume": 200000,
            "atr_period": 14,
            "min_atr_pct": 0.008,
            "max_atr_pct": 0.07,
            "breakout_buffer_pct": 0.001,
            "breakout_min_body_pct": 0.002,
            "breakout_min_close_location": 0.6,
            "recent_pullback_lookback": 10,
            "pullback_proximity_pct": 0.015,
            "max_extension_above_fast_ema_pct": 0.035,
            "max_bars_in_trade": 168,
            "stop_loss_pct": 0.04,
        }
    )
    range_config: dict[str, Any] = Field(
        default_factory=lambda: {
            "range_lookback": 30,
            "min_range_width_pct": 0.02,
            "max_range_width_pct": 0.06,
            "max_center_shift_pct": 0.008,
            "entry_zone_pct": 0.2,
            "exit_zone_pct": 0.75,
            "max_bars_in_trade": 18,
            "stop_buffer_pct": 0.003,
        }
    )
    mean_config: dict[str, Any] = Field(
        default_factory=lambda: {
            "lookback": 20,
            "entry_zscore": 2.0,
            "max_bars_in_trade": 12,
            "stop_buffer_pct": 0.003,
            "stop_loss_pct": 0.02,
        }
    )


class RegimeAwareStrategy(ResearchStrategyBase):
    key = "regime_aware_research"
    name = "RegimeAwareResearch"
    description = "Trend-following in trend regimes, range/mean-reversion in flat regimes, otherwise no-trade."
    config_model = RegimeAwareConfig

    def __init__(self) -> None:
        self._trend_strategy = TrendFollowingStrategy()
        self._range_strategy = RangeTradingStrategy()
        self._mean_strategy = MeanReversionStrategy()

    def required_history_bars(
        self,
        timeframe: str,
        strategy_config: RegimeAwareConfig | None = None,
    ) -> int:
        config = strategy_config or self.config_model()
        trend_config = self._trend_strategy.parse_config(config.trend_config)
        range_config = self._range_strategy.parse_config(config.range_config)
        mean_config = self._mean_strategy.parse_config(config.mean_config)
        return max(
            config.regime_slow_ema_period + config.regime_slope_lookback_bars + 5,
            config.flat_range_lookback + 5,
            config.atr_period + 5,
            config.volume_period + 5,
            self._trend_strategy.required_history_bars(timeframe, trend_config),
            self._range_strategy.required_history_bars(timeframe, range_config),
            self._mean_strategy.required_history_bars(timeframe, mean_config),
        )

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        config = self._config(context)
        history = self._history(context)
        required = self.required_history_bars(context.timeframe, config)
        if len(history) < required:
            return self._hold("insufficient_history")

        active_component = self._active_component(context)
        if active_component is not None:
            delegated = self._delegate_active_component(active_component, context, config)
            delegated.metadata.setdefault("active_component", active_component)
            return delegated

        regime = self._classify_regime(history=history, config=config)
        if regime == "trend":
            signal = self._delegate_component(
                strategy=self._trend_strategy,
                component="trend",
                component_config=config.trend_config,
                context=context,
                regime=regime,
            )
            if signal.action != "hold":
                return signal
            return self._hold("trend_component_no_entry", regime=regime)

        if regime == "flat":
            range_signal = self._delegate_component(
                strategy=self._range_strategy,
                component="range",
                component_config=config.range_config,
                context=context,
                regime=regime,
            )
            if range_signal.action != "hold":
                return range_signal

            mean_signal = self._delegate_component(
                strategy=self._mean_strategy,
                component="mean_reversion",
                component_config=config.mean_config,
                context=context,
                regime=regime,
            )
            if mean_signal.action != "hold":
                return mean_signal
            return self._hold("flat_component_no_entry", regime=regime)

        return self._hold("neutral_regime", regime=regime)

    def _active_component(self, context: StrategyContext) -> Optional[str]:
        position = context.metadata.get("position")
        if not isinstance(position, dict):
            return None
        entry_metadata = position.get("entry_metadata")
        if not isinstance(entry_metadata, dict):
            return None
        component = entry_metadata.get("component")
        if component in {"trend", "range", "mean_reversion"}:
            return str(component)
        return None

    def _delegate_active_component(
        self,
        component: str,
        context: StrategyContext,
        config: RegimeAwareConfig,
    ) -> StrategySignal:
        if component == "trend":
            return self._delegate_component(self._trend_strategy, component, config.trend_config, context, "trend")
        if component == "range":
            return self._delegate_component(self._range_strategy, component, config.range_config, context, "flat")
        return self._delegate_component(self._mean_strategy, component, config.mean_config, context, "flat")

    def _delegate_component(
        self,
        strategy: ResearchStrategyBase,
        component: str,
        component_config: dict[str, Any],
        context: StrategyContext,
        regime: str,
    ) -> StrategySignal:
        delegated_metadata = dict(context.metadata)
        delegated_metadata["config"] = dict(component_config)
        signal = strategy.generate_signal(
            StrategyContext(
                symbol=context.symbol,
                timeframe=context.timeframe,
                timestamp=context.timestamp,
                mode=context.mode,
                metadata=delegated_metadata,
            )
        )
        payload = dict(signal.metadata)
        payload.setdefault("regime", regime)
        payload.setdefault("component", component)
        if signal.action == "enter":
            payload["component"] = component
        return StrategySignal(
            action=signal.action,
            side=signal.side,
            reason=signal.reason,
            confidence=signal.confidence,
            metadata=payload,
        )

    def _classify_regime(
        self,
        history: Sequence[BacktestCandle],
        config: RegimeAwareConfig,
    ) -> str:
        closes = self._close_series(history)
        current_close = closes[-1]
        fast_ema = self._ema(closes, config.regime_fast_ema_period)
        slow_ema = self._ema(closes, config.regime_slow_ema_period)
        slow_reference = self._ema(
            closes[:-config.regime_slope_lookback_bars],
            config.regime_slow_ema_period,
        )
        atr_value = self._atr(history, config.atr_period)
        average_dollar_volume = self._average_dollar_volume(history[:-1], config.volume_period)
        range_high = self._highest_high(history, config.flat_range_lookback)
        range_low = self._lowest_low(history, config.flat_range_lookback)
        if None in {fast_ema, slow_ema, slow_reference, atr_value, average_dollar_volume, range_high, range_low}:
            return "neutral"

        trend_gap_pct = self._pct_change(fast_ema, slow_ema)
        slow_slope_pct = self._pct_change(slow_ema, slow_reference)
        atr_pct = atr_value / current_close if current_close > EPSILON else 0.0
        range_width_pct = (range_high - range_low) / range_low if range_low > EPSILON else 0.0
        recent_closes = closes[-config.flat_range_lookback :]
        midpoint = len(recent_closes) // 2
        first_half = recent_closes[:midpoint]
        second_half = recent_closes[midpoint:]
        first_mean = sum(first_half) / float(max(len(first_half), 1))
        second_mean = sum(second_half) / float(max(len(second_half), 1))
        center_shift_pct = abs(self._pct_change(second_mean, first_mean))

        if (
            fast_ema > slow_ema
            and trend_gap_pct >= config.trend_min_gap_pct
            and slow_slope_pct >= config.trend_min_slope_pct
            and average_dollar_volume >= config.min_average_dollar_volume
        ):
            return "trend"

        if (
            range_width_pct <= config.flat_max_width_pct
            and center_shift_pct <= config.flat_max_center_shift_pct
            and config.flat_min_atr_pct <= atr_pct <= config.flat_max_atr_pct
            and average_dollar_volume >= config.min_average_dollar_volume
        ):
            return "flat"

        return "neutral"
