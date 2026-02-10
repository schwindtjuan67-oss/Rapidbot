from __future__ import annotations

import os
import random
import sys
import tempfile
from typing import Dict, List

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.paper.paper_engine import PaperConfig, PaperEngine
from scripts.strategies.demo_crossover_5m import Strategy


def _build_candles() -> List[Dict[str, float]]:
    candles = []
    ts = 1_700_000_000_000
    price = 50.0
    for i in range(60):
        drift = 0.2 if i % 10 < 5 else -0.1
        price += drift + random.uniform(-0.05, 0.05)
        high = price + random.uniform(0.1, 0.3)
        low = price - random.uniform(0.1, 0.3)
        close = price + random.uniform(-0.05, 0.05)
        candles.append(
            {
                "symbol": "SOLUSDT",
                "interval": "5m",
                "open_ts": ts,
                "close_ts": ts + 299_999,
                "open": price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 100.0,
                "raw_schema": "synthetic",
            }
        )
        ts += 300_000
    return candles


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = PaperEngine(PaperConfig(starting_equity=5000.0), tmpdir)
        strategy = Strategy()
        candles = _build_candles()
        for candle in candles:
            signal = strategy.on_candle(candle, {})
            engine.on_candle(candle, signal)

        trades_path = os.path.join(tmpdir, "paper_trades.csv")
        equity_path = os.path.join(tmpdir, "paper_equity.csv")
        if not os.path.exists(trades_path) or not os.path.exists(equity_path):
            raise SystemExit("Expected paper logs not found.")
        with open(trades_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        if len(lines) < 2:
            raise SystemExit("Expected at least one paper trade.")
        with open(equity_path, "r", encoding="utf-8") as f:
            eq_lines = f.read().splitlines()
        if len(eq_lines) < 2:
            raise SystemExit("Expected equity updates.")
    print("smoke_paper passed")


if __name__ == "__main__":
    main()
