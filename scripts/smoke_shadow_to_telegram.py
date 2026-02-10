from __future__ import annotations

r"""
Smoke test Shadow/Paper -> Telegram VIP.

Comandos (Windows):
  set TELEGRAM_ENABLED=1
  set TELEGRAM_DRY_RUN=1
  .\.venv\Scripts\python.exe .\scripts\smoke_shadow_to_telegram.py --csv .\Datasets\SOLUSDT\5m\SOLUSDT_5m_um.csv --symbol SOLUSDT --tf 5m

  set TELEGRAM_ENABLED=1
  set TELEGRAM_DRY_RUN=0
  set TELEGRAM_BOT_TOKEN=xxxx
  set TELEGRAM_CHAT_ID=yyyy
  .\.venv\Scripts\python.exe .\scripts\smoke_shadow_to_telegram.py --csv .\Datasets\SOLUSDT\5m\SOLUSDT_5m_um.csv --symbol SOLUSDT --tf 5m
"""

import argparse
import csv
import importlib
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.paper.paper_engine import PaperConfig, PaperEngine
from scripts.telegram_notifier import TelegramNotifier


def _parse_ts_to_ms(ts: str) -> int:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _resolve_csv_path(csv_path: Optional[str], symbol: str, tf: str) -> Path:
    if csv_path:
        p = Path(csv_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"CSV no existe: {p}")
    default = ROOT / "Datasets" / symbol / tf / f"{symbol}_{tf}_um.csv"
    if default.exists():
        return default
    fallback_dir = ROOT / "Datasets" / symbol
    if fallback_dir.exists():
        matches = sorted(fallback_dir.rglob("*.csv"))
        if matches:
            return matches[0]
    raise FileNotFoundError("No se encontró CSV en Datasets/SOLUSDT")


def _load_candles(path: Path, symbol: str, tf: str) -> List[Dict[str, Any]]:
    candles: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        prev_open_ts: Optional[int] = None
        for row in reader:
            ts_ms = _parse_ts_to_ms(str(row["ts"]))
            open_ts = prev_open_ts if prev_open_ts is not None else ts_ms
            close_ts = ts_ms
            candles.append(
                {
                    "symbol": symbol,
                    "interval": tf,
                    "open_ts": open_ts,
                    "close_ts": close_ts,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0) or 0.0),
                    "raw_schema": "csv",
                }
            )
            prev_open_ts = ts_ms
    return candles


def _load_strategy(module_name: str) -> Any:
    m = importlib.import_module(module_name)
    if not hasattr(m, "Strategy"):
        raise RuntimeError(f"Module {module_name} no tiene Strategy")
    return m.Strategy()


def _slice_candles(
    candles: List[Dict[str, Any]],
    start_ms: Optional[int],
    end_ms: Optional[int],
    max_bars: Optional[int],
) -> List[Dict[str, Any]]:
    out = candles
    if start_ms is not None:
        out = [c for c in out if int(c["close_ts"]) >= start_ms]
    if end_ms is not None:
        out = [c for c in out if int(c["close_ts"]) <= end_ms]
    if max_bars is not None and max_bars > 0:
        out = out[:max_bars]
    return out


def _run_once(candles: List[Dict[str, Any]], args: argparse.Namespace, dry_run: int) -> Tuple[int, Dict[str, int]]:
    events = {"open": 0, "close": 0, "tg_entry": 0, "tg_exit": 0}

    def _sink(kind: str, payload: Dict[str, Any]) -> None:
        if kind == "OPEN":
            events["open"] += 1
        elif kind == "CLOSE":
            events["close"] += 1
        elif kind == "TG":
            if payload.get("kind") == "entry":
                events["tg_entry"] += 1
            elif payload.get("kind") == "exit":
                events["tg_exit"] += 1
            print(f"[TG] SEND kind={payload.get('kind')} ok={payload.get('ok')} dry_run={payload.get('dry_run')}", flush=True)

    os.environ["TELEGRAM_DRY_RUN"] = str(dry_run)
    tg_enabled = os.getenv("TELEGRAM_ENABLED", "").strip() == "1"
    notifier = TelegramNotifier(enabled=tg_enabled)

    with tempfile.TemporaryDirectory(prefix="smoke_shadow_tg_") as tmp:
        engine = PaperEngine(
            PaperConfig(starting_equity=5000.0),
            tmp,
            notifier=notifier,
            symbol=args.symbol,
            tf=args.tf,
            exchange="paper",
            event_sink=_sink,
        )
        strategy = _load_strategy(args.strategy)
        for candle in candles:
            signal = strategy.on_candle(candle, {})
            engine.on_candle(candle, signal)
            if events["tg_entry"] >= 1 and events["tg_exit"] >= 1 and engine.broker.position is None:
                break

        trades_path = Path(tmp) / "paper_trades.csv"
        trade_count = 0
        with trades_path.open("r", encoding="utf-8") as f:
            trade_count = max(0, sum(1 for _ in f) - 1)
    return trade_count, events


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test Shadow -> Trade Event -> Telegram VIP")
    parser.add_argument("--csv", default=None)
    parser.add_argument("--symbol", default="SOLUSDT")
    parser.add_argument("--tf", default="5m")
    parser.add_argument("--strategy", default="scripts.strategies.demo_crossover_5m")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--max_bars", type=int, default=None)
    parser.add_argument("--max_windows", type=int, default=200)
    parser.add_argument("--window_bars", type=int, default=2000)
    parser.add_argument("--dry_run", type=int, default=1, choices=[0, 1])
    args = parser.parse_args()

    csv_path = _resolve_csv_path(args.csv, args.symbol, args.tf)
    candles = _load_candles(csv_path, args.symbol, args.tf)
    start_ms = _parse_ts_to_ms(args.start) if args.start else None
    end_ms = _parse_ts_to_ms(args.end) if args.end else None
    base = _slice_candles(candles, start_ms, end_ms, args.max_bars)
    if not base:
        raise SystemExit("ERROR: rango sin velas")

    trade_count, events = _run_once(base, args, args.dry_run)
    if trade_count == 0:
        found = False
        stride = max(1, args.window_bars // 2)
        for i in range(args.max_windows):
            start = i * stride
            if start >= len(base):
                break
            window = base[start : start + args.window_bars]
            if len(window) < 50:
                break
            trade_count, events = _run_once(window, args, args.dry_run)
            if trade_count > 0:
                found = True
                print(f"[SMOKE] window_found index={i} start={start} bars={len(window)} trades={trade_count}")
                break
        if not found:
            raise SystemExit("ERROR: no se encontró ventana con trades")

    if trade_count < 1:
        raise SystemExit("ERROR: expected >=1 trade")
    if events["tg_entry"] < 1:
        raise SystemExit("ERROR: expected >=1 VIP Entrada")
    if events["tg_exit"] < 1:
        raise SystemExit("ERROR: expected >=1 VIP Salida")

    print(
        f"[SMOKE] PASS csv={csv_path} trades={trade_count} entry_msgs={events['tg_entry']} exit_msgs={events['tg_exit']} dry_run={args.dry_run}",
        flush=True,
    )


if __name__ == "__main__":
    main()
