from __future__ import annotations

from typing import Any, Dict, Optional

from scripts.strategies.signal_models import Signal, clamp01


def to_broker_signal(signal: Signal) -> Dict[str, Any]:
    action = "ENTER_LONG" if signal.side == "LONG" else "ENTER_SHORT"
    return {
        "action": action,
        "stop": float(signal.sl),
        "tp": float(signal.tp1),
        "tp2": float(signal.tp2),
        "strategy": signal.strategy_name,
        "confidence": clamp01(float(signal.confidence)),
        "reason": signal.reason,
    }


def normalize_strategy_output(x: Any) -> Optional[Dict[str, Any]]:
    if x is None:
        return None

    if isinstance(x, Signal):
        return to_broker_signal(x)

    if isinstance(x, dict):
        out = dict(x)
        out.setdefault("strategy", "unknown")
        out["confidence"] = clamp01(float(out.get("confidence", 0.5) or 0.5))
        out.setdefault("reason", "")
        return out

    return None


__all__ = ["to_broker_signal", "normalize_strategy_output"]
