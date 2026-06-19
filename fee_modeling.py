"""
Fee Modeling Module for Sharp/Swing Trading System
"""
from dataclasses import dataclass
from typing import Tuple

SEC_FEE_RATE = 0.0000278
FINRA_TAF = 0.000166
DEFAULT_SPREAD_PCT = 0.0005
SLIPPAGE_MULTIPLIER = 1.0

@dataclass
class TradeCosts:
    commission: float
    spread_cost: float
    slippage: float
    sec_fee: float
    finra_fee: float
    total: float

def calculate_trade_costs(shares: int, price: float, is_sell: bool = False,
                          spread_pct: float = DEFAULT_SPREAD_PCT) -> TradeCosts:
    trade_value = shares * price
    commission = 0.0
    spread_cost = trade_value * spread_pct
    slippage = spread_cost * SLIPPAGE_MULTIPLIER
    sec_fee = trade_value * SEC_FEE_RATE if is_sell else 0.0
    finra_fee = min(shares * FINRA_TAF, 8.30)
    total = commission + spread_cost + slippage + sec_fee + finra_fee
    return TradeCosts(commission, spread_cost, slippage, sec_fee, finra_fee, total)

def calculate_net_pnl(entry_price: float, exit_price: float, shares: int,
                      direction: str = "LONG", spread_pct: float = DEFAULT_SPREAD_PCT) -> Tuple[float, float, float]:
    if direction == "LONG":
        gross_pnl = (exit_price - entry_price) * shares
    else:
        gross_pnl = (entry_price - exit_price) * shares
    entry_costs = calculate_trade_costs(shares, entry_price, is_sell=False, spread_pct=spread_pct)
    exit_costs = calculate_trade_costs(shares, exit_price, is_sell=True, spread_pct=spread_pct)
    fees = entry_costs.total + exit_costs.total
    net_pnl = gross_pnl - fees
    return gross_pnl, fees, net_pnl

def estimate_spread_from_volume(avg_daily_volume: float, price: float) -> float:
    dollar_volume = avg_daily_volume * price
    if dollar_volume > 100_000_000: return 0.0002
    elif dollar_volume > 10_000_000: return 0.0005
    elif dollar_volume > 1_000_000: return 0.001
    elif dollar_volume > 100_000: return 0.002
    else: return 0.005
