from __future__ import annotations

import argparse
import importlib
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

import csv
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.paper.paper_engine import PaperConfig, PaperEngine
from scripts.telegram_notifier import TelegramNotifier


def _load_strategy(module_name: str, symbol: str, tf: str) -> Any:
    module = importlib.import_module(module_name)
    if not hasattr(module, "Strategy"):
        raise RuntimeError(f"Strategy not found in {module_name}")
    try:
        return module.Strategy(config={"symbol": symbol, "tf": tf})
    except TypeError:
        return module.Strategy()


def _parse_ts_ms(value: str) -> int:
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _load_candles(csv_path: Path, symbol: str, tf: str, max_bars: int) -> list[Dict[str, Any]]:
    candles: list[Dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(candles) >= max_bars:
                break
            if row.get("ts") is not None:
                close_ts = _parse_ts_ms(str(row.get("ts")))
            elif row.get("close_ts") is not None:
                close_ts = int(float(str(row.get("close_ts"))))
            else:
                raise RuntimeError("CSV must contain ts or close_ts column")
            candles.append(
                {
                    "symbol": symbol,
                    "interval": tf,
                    "open_ts": close_ts,
                    "close_ts": close_ts,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0) or 0.0),
                }
            )
    return candles


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke E2E shadow(paper)->trade->telegram")
    parser.add_argument("--csv", default=r".\Datasets\SOLUSDT\5m\SOLUSDT_5m_um.csv")
    parser.add_argument("--strategy", default=os.getenv("STRATEGY_MODULE", "scripts.strategies.demo_crossover_5m"))
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "SOLUSDT"))
    parser.add_argument("--tf", default=os.getenv("TF", "5m"))
    parser.add_argument("--max_bars", type=int, default=3000)
    args = parser.parse_args()

    notifier = TelegramNotifier()
    strategy = _load_strategy(args.strategy, args.symbol, args.tf)
    csv_path = Path(str(args.csv).replace("\\", "/"))
    candles = _load_candles(csv_path, args.symbol, args.tf, args.max_bars)

    counters = {"entry": 0, "exit": 0, "entry_sent": 0, "exit_sent": 0}
    last_exit: Optional[Dict[str, Any]] = None

    def _on_event(event: Dict[str, Any]) -> None:
        nonlocal last_exit
        et = str(event.get("event_type") or "").upper()
        if et == "ENTRY":
            counters["entry"] += 1
            counters["entry_sent"] += int(bool(notifier.notify_vip_event(event)))
        elif et == "EXIT":
            counters["exit"] += 1
            counters["exit_sent"] += int(bool(notifier.notify_vip_event(event)))
            last_exit = event

    with tempfile.TemporaryDirectory(prefix="smoke_tg_e2e_") as tmp:
        engine = PaperEngine(
            config=PaperConfig(starting_equity=5000.0),
            log_dir=tmp,
            symbol=args.symbol,
            tf=args.tf,
            exchange="paper",
            on_event=_on_event,
        )

        for candle in candles:
            try:
                signal = strategy.on_candle(candle, {})
            except TypeError:
                signal = strategy.on_candle(candle)
            engine.on_candle(candle, signal, reason="smoke")
            if counters["entry"] >= 1 and counters["exit"] >= 1:
                break

        if counters["entry"] >= 1 and counters["exit"] >= 1 and counters["entry_sent"] >= 1 and counters["exit_sent"] >= 1:
            last_pnl = last_exit.get("pnl") if last_exit else 0.0
            print(
                f"[SMOKE] PASS entry_sent={counters['entry_sent']} exit_sent={counters['exit_sent']} last_pnl={last_pnl} equity={engine.broker.equity}",
                flush=True,
            )
            return 0

        print(
            f"[SMOKE] FAIL entry={counters['entry']} exit={counters['exit']} entry_sent={counters['entry_sent']} exit_sent={counters['exit_sent']}",
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
