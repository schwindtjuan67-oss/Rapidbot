from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Position:
    side: str
    qty: float
    entry_price: float
    entry_ts: int
    stop_price: Optional[float] = None
    take_profit: Optional[float] = None


@dataclass
class Order:
    order_id: int
    ts: int
    action: str
    side: str
    qty: float
    price: float
    reason: str


@dataclass
class Trade:
    trade_id: int
    entry_ts: int
    exit_ts: int
    side: str
    qty: float
    entry_price: float
    exit_price: float
    pnl_gross: float
    pnl_net: float
    r_multiple: Optional[float]
