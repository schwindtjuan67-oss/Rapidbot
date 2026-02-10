"""
Multi-strategy orchestrator (single-symbol) for this repo.

This exports class Strategy so it is compatible with scripts/run_shadow.py.

It loads N underlying strategy modules (each must export Strategy) and chooses
at most ONE signal per candle.

Configure via env:
  - STRATEGY_MODULE=scripts.strategies.orchestrator
  - ORCH_STRATEGY_LIST: comma-separated module paths, priority order
      example: scripts.solusdt_vwap_bot,scripts.strategies.vpc_retest

Selection:
  - ORCH_POLICY=best_confidence (default): pick highest `confidence` (0..1)
  - ORCH_POLICY=first: pick first signal in list order

Spam control:
  - ORCH_COOLDOWN_BARS (default 0): after emitting ENTER, ignore further ENTER
    for N closed candles (cross-strategy cooldown).

Notes:
  - PaperBroker ignores unknown signal keys, so we can safely include:
      strategy, confidence, reason, tp2, etc.
"""

from __future__ import annotations

import importlib
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from scripts.strategies.signal_adapters import normalize_strategy_output


def _split_csv(raw: str) -> List[str]:
    items: List[str] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if part:
            items.append(part)
    return items


def _as_float(x: Any, default: float) -> float:
    try:
        v = float(x)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


class Strategy:
    """
    Orchestrator Strategy.

    Interface expected by scripts/run_shadow.py:
      on_candle(candle: dict, state: dict) -> dict|None
      (optional) prefill(candles: list[dict]) -> None
    """

    def __init__(self) -> None:
        raw = os.getenv("ORCH_STRATEGY_LIST", "").strip()
        if not raw:
            # sensible default: keep current behavior if user forgets to set list
            raw = os.getenv("STRATEGY_LIST", "").strip()
        if not raw:
            raw = "scripts.solusdt_vwap_bot"

        self.policy = os.getenv("ORCH_POLICY", "best_confidence").strip().lower()
        self.cooldown_bars = max(0, int(os.getenv("ORCH_COOLDOWN_BARS", "0") or "0"))
        self.allow_exit = os.getenv("ORCH_ALLOW_EXIT", "0").strip() == "1"

        self.modules: List[str] = _split_csv(raw)
        self.children: List[Tuple[str, Any]] = []  # (module_path, StrategyInstance)

        for mp in self.modules:
            mod = importlib.import_module(mp)
            if not hasattr(mod, "Strategy"):
                raise RuntimeError(f"Orchestrator requires Strategy export in module: {mp}")
            self.children.append((mp, mod.Strategy()))

        # internal counters (cross-strategy cooldown)
        self._bar_index = 0
        self._last_enter_bar: Optional[int] = None

    def prefill(self, candles: Sequence[Dict[str, Any]]) -> None:
        """Warm-up: call prefill() in each child if available, else feed candles."""
        items = list(candles or [])
        if not items:
            return
        for _, strat in self.children:
            if hasattr(strat, "prefill"):
                try:
                    strat.prefill(items)
                    continue
                except Exception:
                    pass
            # fallback: feed sequentially (signals ignored)
            try:
                st: Dict[str, Any] = {}
                for c in items:
                    strat.on_candle(c, st)
            except Exception:
                continue

    def on_candle(self, candle: Dict[str, Any], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self._bar_index += 1

        # cross-strategy cooldown after ENTER
        if self._last_enter_bar is not None and self.cooldown_bars > 0:
            if (self._bar_index - self._last_enter_bar) < self.cooldown_bars:
                # Still allow EXIT if enabled (optional), else ignore everything
                if not self.allow_exit:
                    return None

        # Per-child state namespaces
        sub = state.setdefault("orch", {})
        signals: List[Tuple[int, Dict[str, Any]]] = []

        for prio, (mp, strat) in enumerate(self.children):
            child_state = sub.setdefault(mp, {})
            try:
                sig = normalize_strategy_output(strat.on_candle(candle, child_state))
            except Exception:
                continue
            if not sig:
                continue

            # Normalize metadata
            if "strategy" not in sig:
                sig["strategy"] = mp
            if "confidence" not in sig:
                sig["confidence"] = 0.50
            if "reason" not in sig:
                sig["reason"] = ""

            action = str(sig.get("action", "")).upper().strip()
            if action.startswith("ENTER_"):
                signals.append((prio, sig))
            elif action == "EXIT" and self.allow_exit:
                # EXIT is optional and can be dangerous in multi-strategy mode.
                # If enabled, we treat EXIT as highest priority (prio=-1).
                signals.append((-1, sig))

        if not signals:
            return None

        chosen = None
        if self.policy == "first":
            # smallest prio wins (list order), but EXIT(-1) wins if present
            chosen = sorted(signals, key=lambda t: t[0])[0][1]
        else:
            # best_confidence: higher confidence wins, ties resolved by priority order
            chosen = sorted(
                signals,
                key=lambda t: (-_as_float(t[1].get("confidence"), 0.0), t[0]),
            )[0][1]

        action = str(chosen.get("action", "")).upper().strip()
        if action.startswith("ENTER_"):
            self._last_enter_bar = self._bar_index

        # add orchestrator metadata (harmless)
        chosen["orchestrator"] = "orch"
        chosen["orch_policy"] = self.policy
        return chosen


__all__ = ["Strategy"]
