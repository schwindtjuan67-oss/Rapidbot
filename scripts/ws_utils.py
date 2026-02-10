from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def _utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _trim_snippet(value: str, limit: int = 4096) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "...(truncated)"


def _safe_json_dumps(payload: Any) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return str(payload)


@dataclass
class SampleRecorder:
    path: str
    limit: int = 50
    count: int = 0

    def record(self, category: str, reason: str, payload: Any) -> None:
        if self.count >= self.limit:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        snippet = _safe_json_dumps(payload)
        entry = {
            "ts": _utc_iso(),
            "category": category,
            "reason": reason,
            "keys": sorted(payload.keys()) if isinstance(payload, dict) else None,
            "raw_snippet": _trim_snippet(snippet),
        }
        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self.count += 1
        except Exception:
            return


class MessageClassifier:
    def classify(self, msg: Any) -> Tuple[str, str]:
        if msg is None:
            return "unknown", "none"
        if not isinstance(msg, dict):
            return "unknown", "non_dict"
        channel = self._channel(msg)
        if channel in {"pong", "ping"} or (channel and channel.endswith("pong")):
            return "pong", "channel"
        if self._is_subscribe_ack(msg, channel):
            return "subscribe_ack", "subscribe_ack"
        if self._is_system(msg, channel):
            return "system", "system"
        if self._looks_like_kline(msg):
            return "kline", "shape"
        if self._looks_like_trade(msg):
            return "trade", "shape"
        if self._looks_like_ticker(msg):
            return "ticker", "shape"
        if self._looks_like_book(msg):
            return "book", "shape"
        return "unknown", "unmatched"

    @staticmethod
    def _channel(msg: Dict[str, Any]) -> str:
        return str(
            msg.get("channel") or msg.get("method") or msg.get("event") or msg.get("type") or ""
        ).lower()

    def _is_subscribe_ack(self, msg: Dict[str, Any], channel: str) -> bool:
        if ("id" in msg and msg.get("code") == 0) or msg.get("result") in {"success", "ok", True}:
            return True
        if str(msg.get("event", "")).lower() in {"subscribed", "subscribe"}:
            return True
        if channel and (channel.startswith("rs.sub") or ".sub" in channel or "subscribe" in channel):
            data = msg.get("data") or msg.get("result")
            if data in {"success", "ok", True}:
                return True
            if isinstance(data, str) and data.strip().lower() in {"success", "ok"}:
                return True
            if data is None and msg.get("code") in (None, 0, "0"):
                return True
        return False

    def _is_system(self, msg: Dict[str, Any], channel: str) -> bool:
        if "code" in msg and "msg" in msg and msg.get("code") not in (None, ""):
            return True
        if channel.startswith("rs.error") or channel.startswith("error"):
            return True
        if channel and "error" in channel:
            return True
        return False

    def _looks_like_kline(self, msg: Dict[str, Any]) -> bool:
        data = msg.get("data")
        if isinstance(data, dict) and isinstance(data.get("k"), dict):
            return True
        if isinstance(data, dict) and isinstance(data.get("kline"), dict):
            return True
        if isinstance(data, dict) and {"t", "o", "h", "l", "c"}.issubset(data.keys()):
            return True
        if isinstance(data, dict) and any(key in data for key in ("openTime", "closeTime", "isFinal")):
            return True
        if isinstance(msg.get("k"), dict):
            return True
        if isinstance(data, list) and len(data) >= 6:
            return True
        if isinstance(msg.get("d"), list) and len(msg.get("d")) >= 6:
            return True
        if any(key in msg for key in ("openTime", "closeTime", "isFinal")):
            return True
        channel = self._channel(msg)
        if channel and "kline" in channel:
            return True
        return False

    def _looks_like_trade(self, msg: Dict[str, Any]) -> bool:
        data = msg.get("data")
        if isinstance(data, dict) and any(key in data for key in ("p", "v", "t")):
            return True
        if any(key in msg for key in ("p", "v", "t")):
            return True
        return False

    def _looks_like_ticker(self, msg: Dict[str, Any]) -> bool:
        data = msg.get("data")
        if isinstance(data, dict) and any(key in data for key in ("last", "bid", "ask", "lastPrice")):
            return True
        return any(key in msg for key in ("last", "bid", "ask", "lastPrice"))

    def _looks_like_book(self, msg: Dict[str, Any]) -> bool:
        data = msg.get("data")
        if isinstance(data, dict) and any(key in data for key in ("asks", "bids")):
            return True
        return any(key in msg for key in ("asks", "bids"))


@dataclass
class CandleAggregator:
    symbol: str
    interval: str
    interval_ms: int
    current_bucket: Optional[int] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: float = 0.0

    def update_trade(self, trade: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ts = trade.get("ts")
        price = trade.get("price")
        qty = trade.get("qty")
        if ts is None or price is None or qty is None:
            return None
        bucket = int(ts) // self.interval_ms * self.interval_ms
        if self.current_bucket is None:
            self._start_bucket(bucket, price, qty)
            return None
        if bucket == self.current_bucket:
            self._update_bucket(price, qty)
            return None
        candle = self._close_bucket()
        self._start_bucket(bucket, price, qty)
        return candle

    def _start_bucket(self, bucket: int, price: float, qty: float) -> None:
        self.current_bucket = bucket
        self.open_price = price
        self.high_price = price
        self.low_price = price
        self.close_price = price
        self.volume = float(qty)

    def _update_bucket(self, price: float, qty: float) -> None:
        self.high_price = max(self.high_price, price) if self.high_price is not None else price
        self.low_price = min(self.low_price, price) if self.low_price is not None else price
        self.close_price = price
        self.volume += float(qty)

    def _close_bucket(self) -> Optional[Dict[str, Any]]:
        if self.current_bucket is None or self.close_price is None:
            return None
        open_ts = self.current_bucket
        close_ts = open_ts + self.interval_ms - 1
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "open_ts": open_ts,
            "close_ts": close_ts,
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "close": self.close_price,
            "volume": self.volume,
            "is_closed": True,
            "is_closed_explicit": True,
            "raw_schema": "local_agg",
        }
