# scripts/strategies/legacy_adapter.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from scripts.strategies.signal_models import Signal, clamp01

def _float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)

def _get(d: Any, k: str, default=None):
    if isinstance(d, dict):
        return d.get(k, default)
    return getattr(d, k, default)

def legacy_to_signal(
    legacy_out: Dict[str, Any],
    strategy_name: str,
    default_conf: float = 0.55,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    ts: Optional[int] = None,
) -> Optional[Signal]:
    """
    Convierte salidas legacy (dict) a Signal estándar.
    Espera algo tipo:
      {"side":"LONG", "entry":..., "sl":..., "tp1":..., "tp2":...}
    o a veces:
      {"signal":"LONG", "entry":..., ...}
    """
    if not legacy_out:
        return None

    side = _get(legacy_out, "side", None) or _get(legacy_out, "signal", None)
    if side not in ("LONG", "SHORT"):
        return None

    entry = _float(_get(legacy_out, "entry", 0))
    sl   = _float(_get(legacy_out, "sl", 0))
    tp1  = _float(_get(legacy_out, "tp1", 0))
    tp2  = _float(_get(legacy_out, "tp2", 0))

    reason = _get(legacy_out, "reason", "") or _get(legacy_out, "msg", "") or "legacy_signal"
    conf_env = os.getenv("VPC_BASE_CONFIDENCE") if strategy_name == "VPC-Base" else None
    conf = _float(_get(legacy_out, "confidence", None), default_conf)
    if conf_env is not None:
        conf = _float(conf_env, conf)
    conf = clamp01(conf)

    # Si legacy no traía tp2, lo reconstruimos conservador:
    if tp1 <= 0 or tp2 <= 0:
        # si tp1 existe y tp2 no, hacemos tp2=2R; si no, abortamos
        if entry > 0 and sl > 0 and tp1 > 0:
            if side == "LONG":
                R = entry - sl
                tp2 = entry + 2.0 * R
            else:
                R = sl - entry
                tp2 = entry - 2.0 * R
        else:
            return None

    return Signal(
        side=side,
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        reason=str(reason),
        confidence=conf,
        strategy_name=strategy_name,
        symbol=symbol,
        timeframe=timeframe,
        ts=ts,
        tags={"legacy": True},
    )
