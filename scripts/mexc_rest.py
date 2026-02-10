from __future__ import annotations

"""Lightweight MEXC Futures REST helpers (no auth).

Used for startup prefill: download last N candles so strategies can warm up indicators and
build higher-TF context BEFORE the websocket loop starts.

Endpoint documented by MEXC Futures market docs:
  GET /api/v1/contract/kline/{symbol}?interval=Min5&start=<sec>&end=<sec>
"""

import json
import os
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


_TF_TO_MEXC_INTERVAL = {
    "1m": "Min1",
    "5m": "Min5",
    "15m": "Min15",
    "30m": "Min30",
    "1h": "Min60",
    "60m": "Min60",
    "4h": "Hour4",
    "8h": "Hour8",
    "1d": "Day1",
    "1w": "Week1",
    "1M": "Month1",
}


def mexc_contract_symbol(sym: str) -> str:
    symbol = str(sym or "").strip().upper()
    if not symbol:
        return ""
    if "_" in symbol:
        return symbol
    if symbol.endswith("USDT") and len(symbol) > 4:
        return f"{symbol[:-4]}_USDT"
    return symbol


def mexc_interval_from_tf(tf: str) -> str:
    key = str(tf or "").strip()
    return _TF_TO_MEXC_INTERVAL.get(key, _TF_TO_MEXC_INTERVAL.get(key.lower(), "Min5"))


def tf_to_interval_sec(tf: str) -> int:
    s = str(tf or "").strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 60 * 60
    if s.endswith("d"):
        return int(s[:-1]) * 24 * 60 * 60
    return 300


def _normalize_ts_ms(ts_any: Any) -> Optional[int]:
    try:
        ts = int(ts_any)
    except Exception:
        return None
    # seconds -> ms
    if ts < 10**12:
        return ts * 1000
    return ts


def _http_get_json(url: str, timeout_sec: float) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "RetailBot/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def _fetch_kline_range(
    symbol_contract: str,
    interval: str,
    start_ts: int,
    end_ts: int,
    base: str,
    timeout_sec: float,
) -> List[Dict[str, Any]]:
    # Docs say start/end are seconds. Some examples floating around use ms, so we retry if empty.
    def _call(start_v: int, end_v: int) -> List[Dict[str, Any]]:
        q = urllib.parse.urlencode({"interval": interval, "start": start_v, "end": end_v})
        url = f"{base}/api/v1/contract/kline/{symbol_contract}?{q}"
        payload = _http_get_json(url, timeout_sec)
        if not isinstance(payload, dict) or payload.get("success") is not True:
            return []
        data = payload.get("data")
        if not isinstance(data, dict):
            return []
        times = data.get("time") or []
        opens = data.get("open") or []
        highs = data.get("high") or []
        lows = data.get("low") or []
        closes = data.get("close") or []
        vols = data.get("vol") or data.get("volume") or []
        if not isinstance(times, list):
            return []
        out: List[Dict[str, Any]] = []
        n = min(len(times), len(opens), len(highs), len(lows), len(closes), len(vols))
        for i in range(n):
            out.append(
                {
                    "t": times[i],
                    "o": opens[i],
                    "h": highs[i],
                    "l": lows[i],
                    "c": closes[i],
                    "v": vols[i],
                }
            )
        return out

    rows = _call(start_ts, end_ts)
    if rows:
        return rows
    # Retry with ms if empty
    rows = _call(start_ts * 1000, end_ts * 1000)
    return rows


def fetch_contract_klines(
    symbol: str,
    tf: str,
    limit: int,
    *,
    base: Optional[str] = None,
    timeout_sec: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Fetch last `limit` closed candles for a futures contract symbol (MEXC).

    Returns candles in Retail schema:
      {symbol, interval, open_ts, close_ts, open, high, low, close, volume, raw_schema}
    """
    if limit <= 0:
        return []

    base = (base or os.getenv("MEXC_REST_BASE", "https://api.mexc.com")).rstrip("/")
    timeout = float(timeout_sec if timeout_sec is not None else os.getenv("MEXC_REST_TIMEOUT_SEC", "6.0"))

    interval = mexc_interval_from_tf(tf)
    interval_sec = tf_to_interval_sec(tf)
    interval_ms = interval_sec * 1000

    symbol_contract = mexc_contract_symbol(symbol)
    if not symbol_contract:
        return []

    now_sec = int(time.time())
    # Align end to the previous complete candle boundary (avoid partial last candle)
    end_sec = now_sec - (now_sec % interval_sec)

    # Use a wider time window to avoid server-side caps (we'll trim to last `limit`).
    span_mult = float(os.getenv("PREFILL_SPAN_MULT", "2.0"))
    start_sec = max(0, end_sec - int(limit * interval_sec * span_mult))

    # Try 3 times expanding range if needed.
    rows: List[Dict[str, Any]] = []
    for attempt in range(3):
        rows = _fetch_kline_range(symbol_contract, interval, start_sec, end_sec, base, timeout)
        if rows and len(rows) >= max(10, limit // 2):
            break
        # expand further back
        start_sec = max(0, start_sec - int(limit * interval_sec * span_mult))

    if not rows:
        return []

    # Convert + sort by time
    def _t_ms(r: Dict[str, Any]) -> int:
        tms = _normalize_ts_ms(r.get("t"))
        return int(tms or 0)

    rows.sort(key=_t_ms)

    candles: List[Dict[str, Any]] = []
    now_ms = end_sec * 1000
    for r in rows:
        open_ts = _normalize_ts_ms(r.get("t"))
        if open_ts is None:
            continue
        close_ts = open_ts + interval_ms
        # keep only fully closed within aligned end
        if close_ts > now_ms:
            continue
        try:
            candles.append(
                {
                    "event_type": "candle",
                    "symbol": symbol,
                    "interval": tf,
                    "open_ts": int(open_ts),
                    "close_ts": int(close_ts),
                    "open": float(r.get("o")),
                    "high": float(r.get("h")),
                    "low": float(r.get("l")),
                    "close": float(r.get("c")),
                    "volume": float(r.get("v") or 0.0),
                    "raw_schema": "mexc:rest.kline",
                }
            )
        except Exception:
            continue

    if not candles:
        return []

    # Trim to last `limit`
    if len(candles) > limit:
        candles = candles[-limit:]
    return candles
