from __future__ import annotations

from math import sqrt
from statistics import mean, pstdev


def calculate_trade_metrics(trades: list, initial_capital: float = 10000.0) -> dict:
    if not trades:
        return {
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe_like": 0.0,
            "win_rate": 0.0,
            "avg_winner": 0.0,
            "avg_loser": 0.0,
            "expectancy": 0.0,
            "profit_factor": 0.0,
            "total_trades": 0,
            "net_pnl": 0.0,
            "equity_curve": [],
        }

    ordered = sorted(trades, key=lambda trade: getattr(trade, "exit_time", trade["exit_time"]))
    net_pnls = [float(getattr(trade, "net_pnl", trade["net_pnl"])) for trade in ordered]
    wins = [value for value in net_pnls if value > 0]
    losses = [value for value in net_pnls if value < 0]

    equity = initial_capital
    peak = initial_capital
    max_drawdown = 0.0
    equity_curve = []
    for trade in ordered:
        exit_time = getattr(trade, "exit_time", trade["exit_time"])
        net_pnl = float(getattr(trade, "net_pnl", trade["net_pnl"]))
        equity += net_pnl
        peak = max(peak, equity)
        drawdown = (peak - equity) / peak if peak else 0.0
        max_drawdown = max(max_drawdown, drawdown)
        equity_curve.append(
            {
                "time": exit_time.isoformat() if hasattr(exit_time, "isoformat") else str(exit_time),
                "equity": round(equity, 2),
                "drawdown": round(drawdown * 100, 2),
            }
        )

    total_return = ((equity - initial_capital) / initial_capital * 100) if initial_capital else 0.0
    first_time = getattr(ordered[0], "entry_time", ordered[0].get("entry_time"))
    last_time = getattr(ordered[-1], "exit_time", ordered[-1].get("exit_time"))
    duration_days = max((last_time - first_time).days, 1) if first_time and last_time else 0
    annualized_return = (
        ((equity / initial_capital) ** (365 / duration_days) - 1) * 100
        if initial_capital > 0 and duration_days > 0 and equity > 0
        else 0.0
    )
    sharpe_like = (
        (mean(net_pnls) / pstdev(net_pnls) * sqrt(len(net_pnls))) if len(net_pnls) > 1 and pstdev(net_pnls) else 0.0
    )
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else (999.0 if wins else 0.0)

    return {
        "total_return": round(total_return, 2),
        "annualized_return": round(annualized_return, 2),
        "max_drawdown": round(max_drawdown * 100, 2),
        "sharpe_like": round(sharpe_like, 2),
        "win_rate": round((len(wins) / len(net_pnls)) * 100, 2),
        "avg_winner": round(mean(wins), 2) if wins else 0.0,
        "avg_loser": round(mean(losses), 2) if losses else 0.0,
        "expectancy": round(sum(net_pnls) / len(net_pnls), 2),
        "profit_factor": round(profit_factor, 2),
        "total_trades": len(net_pnls),
        "net_pnl": round(sum(net_pnls), 2),
        "equity_curve": equity_curve,
    }
