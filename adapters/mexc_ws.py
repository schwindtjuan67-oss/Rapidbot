# === CODEX_PATCH_BEGIN: MEXC_WS_KLINE_PARSER (2026-02-02) ===
"""
Robust parser for MEXC WebSocket kline messages.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple, List

DEBUG_PARSE = False


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(int(value))
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, float):
            return int(value)
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_ts(value: Any) -> Optional[int]:
    ts = _to_int(value)
    if ts is None:
        return None
    if ts < 10**12:
        return ts * 1000
    return ts


_VERBOSE_INTERVAL_TO_SHORT = {
    "min1": "1m",
    "min5": "5m",
    "min15": "15m",
    "min30": "30m",
    "min60": "1h",
    "hour4": "4h",
    "hour8": "8h",
    "day1": "1d",
    "week1": "1w",
    "month1": "1M",
}
_SHORT_INTERVAL_TO_VERBOSE = {value: key.title() for key, value in _VERBOSE_INTERVAL_TO_SHORT.items()}
_SHORT_INTERVAL_TO_VERBOSE["1M"] = "Month1"


def _interval_to_ms(interval: Optional[str]) -> Optional[int]:
    if not interval:
        return None
    interval = normalize_interval(interval, prefer_verbose=False) or interval
    match = re.match(r"^(\d+)([smhdw])$", interval, re.IGNORECASE)
    if not match:
        if interval == "1M":
            return 30 * 24 * 60 * 60 * 1000
        return None
    qty = int(match.group(1))
    unit = match.group(2).lower()
    multipliers = {
        "s": 1000,
        "m": 60 * 1000,
        "h": 60 * 60 * 1000,
        "d": 24 * 60 * 60 * 1000,
        "w": 7 * 24 * 60 * 60 * 1000,
    }
    return qty * multipliers[unit]


def _extract_interval(msg: Dict[str, Any], kline: Optional[Dict[str, Any]] = None) -> Optional[str]:
    candidates = []
    for source in (kline, msg):
        if not source or not isinstance(source, dict):
            continue
        for key in ("interval", "i", "kline_type", "period"):
            value = source.get(key)
            if isinstance(value, str):
                candidates.append(value)
    for key in ("stream", "topic", "channel", "c"):
        value = msg.get(key)
        if isinstance(value, str):
            candidates.append(value)
    for candidate in candidates:
        match = re.search(r"(\d+[smhdw])", candidate, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r"(min|hour|day|week|month)\d+", candidate, re.IGNORECASE)
        if match:
            return match.group(0)
    return None


def _extract_symbol(msg: Dict[str, Any], kline: Optional[Dict[str, Any]] = None) -> Optional[str]:
    for source in (kline, msg):
        if not source or not isinstance(source, dict):
            continue
        for key in ("symbol", "s", "sym", "pair"):
            value = source.get(key)
            if isinstance(value, str):
                return value
    return None


def _extract_is_closed(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _build_kline_payload(
    kline: Dict[str, Any],
    symbol: Optional[str],
    interval: Optional[str],
    raw_schema: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[List[str]]]:
    missing: List[str] = []
    open_ts = _normalize_ts(kline.get("t")) or _normalize_ts(kline.get("openTime")) or _normalize_ts(
        kline.get("open_time")
    )
    close_ts = _normalize_ts(kline.get("T")) or _normalize_ts(kline.get("closeTime")) or _normalize_ts(
        kline.get("close_time")
    )
    open_price = _to_float(kline.get("o") or kline.get("open"))
    high_price = _to_float(kline.get("h") or kline.get("high"))
    low_price = _to_float(kline.get("l") or kline.get("low"))
    close_price = _to_float(kline.get("c") or kline.get("close"))
    volume = _to_float(kline.get("v"))
    if volume is None:
        volume = _to_float(kline.get("a"))
    if volume is None:
        volume = _to_float(kline.get("q"))
    if volume is None:
        volume = _to_float(kline.get("vol") or kline.get("volume"))
    if volume is None:
        volume = 0.0
    is_closed_raw = _extract_is_closed(kline.get("x"))
    if is_closed_raw is None:
        is_closed_raw = _extract_is_closed(kline.get("isFinal"))
    if is_closed_raw is None:
        is_closed_raw = _extract_is_closed(kline.get("is_final"))
    if is_closed_raw is None:
        is_closed_raw = _extract_is_closed(kline.get("isClosed"))
    if is_closed_raw is None:
        is_closed_raw = _extract_is_closed(kline.get("is_closed"))
    is_closed_explicit = is_closed_raw is not None
    is_closed = bool(is_closed_raw) if is_closed_explicit else False

    if close_ts is None and open_ts is not None:
        interval_ms = _interval_to_ms(interval)
        if interval_ms is not None:
            close_ts = open_ts + interval_ms - 1

    if open_ts is None:
        missing.append("t")
    if open_price is None:
        missing.append("o")
    if high_price is None:
        missing.append("h")
    if low_price is None:
        missing.append("l")
    if close_price is None:
        missing.append("c")
    if volume is None:
        missing.append("v")
    if close_ts is None:
        missing.append("close_ts" if interval else "interval")
    if missing:
        return None, missing

    payload = {
        "symbol": symbol,
        "interval": interval or "",
        "open_ts": open_ts,
        "close_ts": close_ts,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
        "is_closed": is_closed,
        "is_closed_explicit": is_closed_explicit,
        "raw_schema": raw_schema,
    }
    if DEBUG_PARSE:
        payload["_debug"] = {"kline_keys": sorted(kline.keys())}
    return payload, None


def parse_mexc_kline_message_with_reason(
    msg: Dict[str, Any],
    expected_symbol: Optional[str] = None,
    expected_interval: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        if not isinstance(msg, dict):
            return None, "non_dict"

        data = msg.get("data") if isinstance(msg.get("data"), dict) else None
        kline = None
        raw_schema = ""
        channel = str(msg.get("channel") or "").lower()

        if channel == "push.kline" and data:
            kline = data
            raw_schema = "mexc:push.kline"
        elif data and isinstance(data.get("k"), dict):
            kline = data.get("k")
            raw_schema = "schema_a:data.k"
        elif data and isinstance(data.get("kline"), dict):
            kline = data.get("kline")
            raw_schema = "schema_a:data.kline"
        elif data and {"t", "o", "h", "l", "c"}.issubset(data.keys()):
            kline = data
            raw_schema = "schema_a:data"
        elif data and any(key in data for key in ("openTime", "closeTime", "isFinal")):
            kline = _parse_snake_case(data)
            raw_schema = "schema_d:snake_case"
        elif data and any(key in data for key in ("t", "o", "h", "l", "c", "interval", "symbol")):
            kline = data
            raw_schema = "schema_a:data_partial"
        elif isinstance(msg.get("k"), dict):
            kline = msg.get("k")
            raw_schema = "schema_c:k"
        elif isinstance(msg.get("data"), list):
            raw_schema = "schema_b:data_list"
            kline = _parse_list_schema(msg.get("data"))
        elif isinstance(msg.get("d"), list):
            raw_schema = "schema_b:d_list"
            kline = _parse_list_schema(msg.get("d"))
        elif any(key in msg for key in ("openTime", "closeTime", "isFinal")):
            kline = _parse_snake_case(msg)
            raw_schema = "schema_d:snake_case_root"

        if not kline:
            return None, "kline schema mismatch"

        interval = _extract_interval(msg, kline)
        symbol = _extract_symbol(msg, kline)
        if "ts" in msg and "t" not in kline:
            kline = {**kline, "t": msg.get("ts")}
        if expected_symbol:
            symbol = normalize_symbol(symbol or expected_symbol, prefer_underscore="_" in expected_symbol)
        else:
            symbol = normalize_symbol(symbol, prefer_underscore=None)
        interval = normalize_interval(interval or expected_interval, prefer_verbose=_prefers_verbose_interval(expected_interval))
        payload, missing = _build_kline_payload(kline, symbol, interval, raw_schema)
        if missing:
            return None, f"missing {', '.join(missing)}"
        if payload is None:
            return None, "kline schema mismatch"
        return payload, "ok"
    except Exception:
        return None, "exception"


def parse_mexc_kline_message(
    msg: Dict[str, Any],
    expected_symbol: Optional[str] = None,
    expected_interval: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Parse a WebSocket message into a normalized kline dict.
    Returns None if not a usable kline.
    """
    payload, _ = parse_mexc_kline_message_with_reason(msg, expected_symbol, expected_interval)
    return payload


def normalize_symbol(symbol: Optional[str], prefer_underscore: Optional[bool] = None) -> Optional[str]:
    if not symbol:
        return symbol
    if prefer_underscore is None:
        if "_" in symbol:
            return symbol
        if symbol.endswith("USDT") and len(symbol) > 4:
            return f"{symbol[:-4]}_USDT"
        return symbol
    if prefer_underscore:
        if "_" in symbol:
            return symbol
        if symbol.endswith("USDT") and len(symbol) > 4:
            return f"{symbol[:-4]}_USDT"
        return symbol
    return symbol.replace("_", "")


def _prefers_verbose_interval(interval: Optional[str]) -> bool:
    if not interval:
        return False
    lowered = interval.strip().lower()
    return lowered.startswith(("min", "hour", "day", "week", "month"))


def normalize_interval(interval: Optional[str], prefer_verbose: Optional[bool] = None) -> Optional[str]:
    if not interval:
        return interval
    raw = interval.strip()
    raw_key = raw.lower()
    short = _VERBOSE_INTERVAL_TO_SHORT.get(raw_key, raw)
    if isinstance(short, str) and re.match(r"^\d+[smhdw]$", short, re.IGNORECASE):
        short_norm = short.lower()
    else:
        short_norm = short
    if short_norm in _SHORT_INTERVAL_TO_VERBOSE:
        verbose = _SHORT_INTERVAL_TO_VERBOSE[short_norm]
    else:
        verbose = raw
    if prefer_verbose is None:
        if raw == "1M":
            return "Month1"
        if raw_key.endswith("m"):
            return f"Min{raw_key[:-1]}"
        if raw_key.endswith("h"):
            return f"Hour{raw_key[:-1]}"
        if raw_key.endswith("d"):
            return f"Day{raw_key[:-1]}"
        if raw_key.startswith(("min", "hour", "day", "week", "month")):
            return raw
        return raw
    return verbose if prefer_verbose else short_norm


def build_kline_subscribe_payload(symbol: str, interval: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    mexc_symbol = normalize_symbol(symbol, prefer_underscore=True)
    mexc_interval = normalize_interval(interval, prefer_verbose=True)
    mexc_payload = {"method": "sub.kline", "param": {"symbol": mexc_symbol, "interval": mexc_interval}, "id": 1}
    generic_stream = f"{symbol.lower()}@kline_{interval}"
    generic_payload = {"method": "SUBSCRIBE", "params": [generic_stream], "id": 1}
    return mexc_payload, generic_payload


def parse_mexc_trade_message(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if not isinstance(msg, dict):
            return None
        data = msg.get("data") if isinstance(msg.get("data"), dict) else None
        payload = None
        raw_schema = ""
        if data and any(key in data for key in ("p", "v", "t")):
            payload = data
            raw_schema = "schema_a:data"
        elif any(key in msg for key in ("p", "v", "t")):
            payload = msg
            raw_schema = "schema_b:root"
        if not payload:
            return None
        symbol = payload.get("symbol") or msg.get("symbol") or msg.get("s")
        trade_id = payload.get("tradeId") or payload.get("id")
        ts = _normalize_ts(payload.get("t") or payload.get("ts"))
        price = _to_float(payload.get("p") or payload.get("price"))
        qty = _to_float(payload.get("v") or payload.get("qty") or payload.get("quantity"))
        side = payload.get("side") or payload.get("S")
        if ts is None or price is None or qty is None:
            return None
        return {
            "symbol": symbol,
            "trade_id": trade_id,
            "ts": ts,
            "price": price,
            "qty": qty,
            "side": side,
            "raw_schema": raw_schema,
        }
    except Exception:
        return None


def _parse_list_schema(items: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(items, list):
        return None
    if len(items) < 6:
        return None
    open_ts = items[0]
    open_price = items[1] if len(items) > 1 else None
    high_price = items[2] if len(items) > 2 else None
    low_price = items[3] if len(items) > 3 else None
    close_price = items[4] if len(items) > 4 else None
    volume = items[5] if len(items) > 5 else None
    close_ts = items[6] if len(items) > 6 else None
    return {
        "t": open_ts,
        "o": open_price,
        "h": high_price,
        "l": low_price,
        "c": close_price,
        "v": volume,
        "T": close_ts,
    }


def _parse_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "t": data.get("openTime") or data.get("open_time"),
        "T": data.get("closeTime") or data.get("close_time"),
        "o": data.get("open"),
        "h": data.get("high"),
        "l": data.get("low"),
        "c": data.get("close"),
        "v": data.get("vol") or data.get("volume"),
        "x": data.get("isFinal") or data.get("is_final") or data.get("isClosed"),
    }


def is_kline_closed(k: Dict[str, Any], now_ms: Optional[int] = None) -> bool:
    if not isinstance(k, dict):
        return False
    if "is_closed" in k and k.get("is_closed_explicit", False):
        return bool(k.get("is_closed"))
    if now_ms is None:
        return False
    close_ts = k.get("close_ts")
    if close_ts is None:
        return False
    return int(now_ms) >= int(close_ts)


def should_accept_closed_kline(
    k: Dict[str, Any],
    last_close_ts: Optional[int],
    now_ms: int,
) -> bool:
    if not is_kline_closed(k, now_ms):
        return False
    close_ts = k.get("close_ts")
    if close_ts is None:
        return False
    if last_close_ts is not None and int(close_ts) <= int(last_close_ts):
        return False
    return True


if __name__ == "__main__":
    sample_a = {
        "data": {
            "k": {
                "t": 1700000000000,
                "T": 1700000299999,
                "o": "10",
                "h": "12",
                "l": "9",
                "c": "11",
                "v": "100",
                "x": True,
                "i": "5m",
            },
            "s": "SOLUSDT",
        }
    }
    sample_b = {"data": [1700000000, "10", "12", "9", "11", "100", 1700000299]}
    sample_c = {
        "k": {
            "t": 1700000000000,
            "T": 1700000599999,
            "o": 20,
            "h": 25,
            "l": 19,
            "c": 22,
            "v": 500,
            "x": "true",
        },
        "symbol": "SOLUSDT",
        "interval": "10m",
    }

    print(parse_mexc_kline_message(sample_a))
    print(parse_mexc_kline_message(sample_b))
    print(parse_mexc_kline_message(sample_c))
# === CODEX_PATCH_END: MEXC_WS_KLINE_PARSER (2026-02-02) ===
