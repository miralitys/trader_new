from __future__ import annotations

from decimal import Decimal
from typing import Sequence

from app.engines.base import EngineBase
from app.schemas.backtest import BacktestMetrics, BacktestTrade, EquityPoint

ZERO = Decimal("0")
HUNDRED = Decimal("100")


class PerformanceEngine(EngineBase):
    engine_name = "performance_engine"
    purpose = "PnL, drawdown, expectancy, and other strategy performance analytics."

    def calculate_metrics(
        self,
        trades: Sequence[BacktestTrade],
        equity_curve: Sequence[EquityPoint],
        initial_capital: Decimal,
        final_equity: Decimal,
    ) -> BacktestMetrics:
        total_trades = len(trades)
        total_return_pct = ZERO
        if initial_capital > ZERO:
            total_return_pct = ((final_equity - initial_capital) / initial_capital) * HUNDRED

        winning_trades = [trade for trade in trades if trade.pnl > ZERO]
        losing_trades = [trade for trade in trades if trade.pnl < ZERO]
        gross_profit = sum((trade.pnl for trade in winning_trades), ZERO)
        gross_loss = abs(sum((trade.pnl for trade in losing_trades), ZERO))

        win_rate_pct = ZERO
        if total_trades:
            win_rate_pct = (Decimal(len(winning_trades)) / Decimal(total_trades)) * HUNDRED

        if gross_loss == ZERO:
            profit_factor = gross_profit if gross_profit > ZERO else ZERO
        else:
            profit_factor = gross_profit / gross_loss

        expectancy = ZERO
        if total_trades:
            expectancy = sum((trade.pnl for trade in trades), ZERO) / Decimal(total_trades)
        gross_expectancy = ZERO
        if total_trades:
            gross_expectancy = sum((trade.gross_pnl for trade in trades), ZERO) / Decimal(total_trades)

        avg_winner = ZERO
        if winning_trades:
            avg_winner = gross_profit / Decimal(len(winning_trades))

        avg_loser = ZERO
        if losing_trades:
            avg_loser = sum((trade.pnl for trade in losing_trades), ZERO) / Decimal(len(losing_trades))

        return BacktestMetrics(
            total_return_pct=total_return_pct,
            max_drawdown_pct=self.calculate_max_drawdown(equity_curve),
            win_rate_pct=win_rate_pct,
            profit_factor=profit_factor,
            expectancy=expectancy,
            gross_expectancy=gross_expectancy,
            net_expectancy=expectancy,
            avg_winner=avg_winner,
            avg_loser=avg_loser,
            total_trades=total_trades,
        )

    def calculate_max_drawdown(self, equity_curve: Sequence[EquityPoint]) -> Decimal:
        if not equity_curve:
            return ZERO

        peak = equity_curve[0].equity
        max_drawdown_pct = ZERO
        for point in equity_curve:
            if point.equity > peak:
                peak = point.equity
            if peak <= ZERO:
                continue
            drawdown_pct = ((peak - point.equity) / peak) * HUNDRED
            if drawdown_pct > max_drawdown_pct:
                max_drawdown_pct = drawdown_pct
        return max_drawdown_pct
