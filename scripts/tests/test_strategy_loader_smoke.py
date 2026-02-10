from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.strategies.loader import load_strategy


def _candles(n: int = 10) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    ts = 1_700_000_000_000
    px = 100.0
    for i in range(n):
        px = px + (0.15 if i % 2 == 0 else -0.05)
        out.append(
            {
                "symbol": "SOLUSDT",
                "interval": "5m",
                "open_ts": ts,
                "close_ts": ts + 299_999,
                "timestamp": ts + 299_999,
                "open": px - 0.1,
                "high": px + 0.2,
                "low": px - 0.3,
                "close": px,
                "volume": 1000.0 + i,
            }
        )
        ts += 300_000
    return out


def _run_module(module_path: str) -> Optional[Dict[str, float]]:
    strategy = load_strategy(module_path)
    out: Optional[Dict[str, float]] = None
    for candle in _candles():
        out = strategy.on_candle(candle, {})
        assert out is None or isinstance(out, dict)
    return out


def test_loader_smoke_solusdt_vwap_bot() -> None:
    try:
        _run_module("scripts.solusdt_vwap_bot")
    except ModuleNotFoundError as exc:
        pytest.skip(f"Missing optional dependency for legacy strategy: {exc}")


def test_loader_smoke_vpc_retest() -> None:
    _run_module("scripts.strategies.vpc_retest")


def test_loader_smoke_orchestrator() -> None:
    old = os.environ.get("ORCH_STRATEGY_LIST")
    os.environ["ORCH_STRATEGY_LIST"] = "scripts.strategies.vpc_retest"
    try:
        _run_module("scripts.strategies.orchestrator")
    finally:
        if old is None:
            os.environ.pop("ORCH_STRATEGY_LIST", None)
        else:
            os.environ["ORCH_STRATEGY_LIST"] = old
