# === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
from __future__ import annotations

import csv
import hashlib
import json
import os
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from scripts.solusdt_vwap_bot import compute_metrics, EQUITY_COLUMNS, TRADE_COLUMNS


class ShadowTradeSink:
    def __init__(self, outdir: str, max_dedupe: int = 5000) -> None:
        self.outdir = outdir
        os.makedirs(self.outdir, exist_ok=True)
        self.trades_path = os.path.join(self.outdir, "shadow_trades.csv")
        self.equity_path = os.path.join(self.outdir, "shadow_equity_curve.csv")
        self.metrics_path = os.path.join(self.outdir, "shadow_metrics.json")
        self.events_path = os.path.join(self.outdir, "shadow_events.jsonl")
        self.candles_path = os.path.join(self.outdir, "shadow_candles.csv")
        self.trade_events_path = os.path.join(self.outdir, "shadow_trade_events.csv")
        self._dedupe_keys: Deque[str] = deque(maxlen=max_dedupe)
        self._dedupe_set: set[str] = set()
        self.ensure_headers()

    def ensure_headers(self) -> None:
        self._ensure_header(self.trades_path, TRADE_COLUMNS)
        self._ensure_header(self.equity_path, EQUITY_COLUMNS)
        self._ensure_header(
            self.candles_path,
            [
                "event_type",
                "symbol",
                "interval",
                "open_ts",
                "close_ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "raw_schema",
            ],
        )
        self._ensure_header(
            self.trade_events_path,
            ["event_type", "symbol", "trade_id", "ts", "price", "qty", "side", "raw_schema"],
        )

    def append_raw_event(self, payload: Dict[str, Any]) -> bool:
        return self._append_jsonl(self.events_path, payload, self._dedupe_key("raw", payload))

    def append_candle(self, payload: Dict[str, Any]) -> bool:
        key = self._dedupe_key(
            "candle",
            (
                payload.get("symbol"),
                payload.get("interval"),
                payload.get("open_ts"),
                payload.get("close_ts"),
                payload.get("close"),
            ),
        )
        row = [
            "candle",
            payload.get("symbol"),
            payload.get("interval"),
            payload.get("open_ts"),
            payload.get("close_ts"),
            payload.get("open"),
            payload.get("high"),
            payload.get("low"),
            payload.get("close"),
            payload.get("volume"),
            payload.get("raw_schema"),
        ]
        return self._append_row(self.candles_path, row, key)

    def append_trade_event(self, payload: Dict[str, Any]) -> bool:
        key = self._dedupe_key(
            "trade",
            (
                payload.get("symbol"),
                payload.get("trade_id"),
                payload.get("ts"),
                payload.get("price"),
                payload.get("qty"),
                payload.get("side"),
            ),
        )
        row = [
            "trade",
            payload.get("symbol"),
            payload.get("trade_id"),
            payload.get("ts"),
            payload.get("price"),
            payload.get("qty"),
            payload.get("side"),
            payload.get("raw_schema"),
        ]
        return self._append_row(self.trade_events_path, row, key)

    def append_trade(self, trade_dict: Dict[str, Any]) -> None:
        try:
            self.ensure_headers()
            row = [self._format_value(trade_dict.get(col)) for col in TRADE_COLUMNS]
            self._append_row(self.trades_path, row, self._dedupe_key("shadow_trade", row))
        except Exception:
            return

    def append_equity(self, ts: Any, equity_usdt: Any, note: Any) -> None:
        try:
            self.ensure_headers()
            row = [self._format_value(ts), self._format_value(equity_usdt), self._format_value(note)]
            self._append_row(self.equity_path, row, self._dedupe_key("equity", row))
        except Exception:
            return

    def update_metrics(self, trades_csv_path: str, equity_csv_path: str) -> Optional[Dict[str, Any]]:
        if not os.path.exists(trades_csv_path) or not os.path.exists(equity_csv_path):
            return None
        trades_df = pd.read_csv(trades_csv_path)
        eq_df = pd.read_csv(equity_csv_path)
        if not eq_df.empty and "ts" in eq_df.columns:
            eq_df["ts"] = pd.to_datetime(eq_df["ts"], utc=True, errors="coerce")
        if not trades_df.empty and "exit_ts" in trades_df.columns:
            trades_df["exit_ts"] = pd.to_datetime(trades_df["exit_ts"], utc=True, errors="coerce")

        start_ts = None
        end_ts = None
        if not eq_df.empty and "ts" in eq_df.columns:
            start_ts = eq_df["ts"].min()
            end_ts = eq_df["ts"].max()
        elif not trades_df.empty and "exit_ts" in trades_df.columns:
            start_ts = trades_df["exit_ts"].min()
            end_ts = trades_df["exit_ts"].max()

        if not eq_df.empty and "equity_usdt" in eq_df.columns:
            initial_equity = float(eq_df["equity_usdt"].iloc[0])
            final_equity = float(eq_df["equity_usdt"].iloc[-1])
        else:
            initial_equity = 0.0
            final_equity = 0.0

        metrics = compute_metrics(trades_df, eq_df, initial_equity, final_equity, start_ts, end_ts)
        with open(self.metrics_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        return metrics

    def count_trades(self) -> int:
        return self._count_rows(self.trades_path)

    def count_equity(self) -> int:
        return self._count_rows(self.equity_path)

    def read_last_trade(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.trades_path):
            return None
        with open(self.trades_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        if len(lines) <= 1:
            return None
        last_line = lines[-1]
        if not last_line.strip():
            return None
        values = next(csv.reader([last_line]))
        if len(values) != len(TRADE_COLUMNS):
            return None
        return dict(zip(TRADE_COLUMNS, values))

    def _append_jsonl(self, path: str, payload: Dict[str, Any], dedupe_key: str) -> bool:
        if self._is_deduped(dedupe_key):
            return False
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            return True
        except Exception:
            return False

    def _append_row(self, path: str, row: Iterable[Any], dedupe_key: str) -> bool:
        if self._is_deduped(dedupe_key):
            return False
        try:
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([self._format_value(value) for value in row])
                f.flush()
                os.fsync(f.fileno())
            return True
        except Exception:
            return False

    def _is_deduped(self, key: str) -> bool:
        if key in self._dedupe_set:
            return True
        self._dedupe_set.add(key)
        self._dedupe_keys.append(key)
        if len(self._dedupe_set) > self._dedupe_keys.maxlen:
            while len(self._dedupe_set) > self._dedupe_keys.maxlen:
                old = self._dedupe_keys.popleft()
                self._dedupe_set.discard(old)
        return False

    @staticmethod
    def _dedupe_key(prefix: str, payload: Any) -> str:
        payload_str = json.dumps(payload, sort_keys=True, default=str)
        digest = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
        return f"{prefix}:{digest}"

    @staticmethod
    def _ensure_header(path: str, columns: List[str]) -> None:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            f.flush()
            os.fsync(f.fileno())

    @staticmethod
    def _format_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, pd.Timestamp):
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if pd.isna(ts):
                return ""
            return ts.isoformat(sep=" ")
        if isinstance(value, str):
            return value
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return value

    @staticmethod
    def _count_rows(path: str) -> int:
        if not os.path.exists(path):
            return 0
        with open(path, "r", encoding="utf-8") as f:
            count = sum(1 for _ in f)
        return max(0, count - 1)
# === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===
