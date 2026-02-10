# scripts/strategies/signal_models.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Literal, Optional

Side = Literal["LONG", "SHORT"]

@dataclass(frozen=True)
class Signal:
    # Required (tu contrato)
    side: Side
    entry: float
    sl: float
    tp1: float
    tp2: float
    reason: str
    confidence: float  # 0..1
    strategy_name: str

    # Optional (útil para orquestador / logs)
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    ts: Optional[int] = None  # close_ts candle que dispara
    tags: Optional[Dict[str, Any]] = None  # metadata libre (scores, regime, etc.)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # limpieza: no mandar basura
        if d.get("tags") is None:
            d.pop("tags", None)
        return d

def clamp01(x: float) -> float:
    if x < 0:
        return 0.0
    if x > 1:
        return 1.0
    return x

def validate_signal(s: Signal) -> None:
    # Fail-fast, ayuda muchísimo a evitar bugs raros en vivo.
    if s.side not in ("LONG", "SHORT"):
        raise ValueError(f"Invalid side={s.side}")
    for k in ("entry", "sl", "tp1", "tp2"):
        v = getattr(s, k)
        if not (isinstance(v, (int, float)) and v > 0):
            raise ValueError(f"Invalid {k}={v}")
    if s.side == "LONG":
        if not (s.sl < s.entry < s.tp1 < s.tp2):
            raise ValueError(f"Bad levels LONG: sl<{s.sl} entry<{s.entry} tp1<{s.tp1} tp2<{s.tp2}")
    else:
        if not (s.sl > s.entry > s.tp1 > s.tp2):
            raise ValueError(f"Bad levels SHORT: sl>{s.sl} entry>{s.entry} tp1>{s.tp1} tp2>{s.tp2}")
    if not (0.0 <= s.confidence <= 1.0):
        raise ValueError(f"confidence out of [0,1]: {s.confidence}")
    if not s.strategy_name:
        raise ValueError("strategy_name empty")
    if not s.reason:
        raise ValueError("reason empty")
