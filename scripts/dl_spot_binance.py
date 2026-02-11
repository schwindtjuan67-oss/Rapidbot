from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

API_BASE_URL = "https://api.binance.com"
DEFAULT_LIMIT = 1000
USER_AGENT = "Rapidbot-BinanceSpotDownloader/1.0"


# PowerShell examples (Windows):
# python -m scripts.dl_spot_binance --exchange binance --symbol BTCUSDT --timeframe 5m --start 2017-01-01 --out .\Datasets\BINANCE\BTCUSDT\5m\BTCUSDT_5m_full.csv
# python -m scripts.dl_spot_binance --symbol ETHUSDT --timeframe 5m --start 2017-01-01 --out .\Datasets\BINANCE\ETHUSDT\5m\ETHUSDT_5m_full.csv
# python -m scripts.dl_spot_binance --symbol SOLUSDT --timeframe 5m --start 2017-01-01 --out .\Datasets\BINANCE\SOLUSDT\5m\SOLUSDT_5m_full.csv


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Download complete Binance Spot OHLCV history (CSV: ts/open/high/low/close/volume)."
    )
    ap.add_argument("--exchange", type=str, default="binance", help="Only 'binance' is supported in this script.")
    ap.add_argument("--symbol", type=str, required=True, help="Spot symbol, e.g. BTCUSDT")
    ap.add_argument("--timeframe", type=str, default="5m", help="Binance interval, default 5m")
    ap.add_argument("--start", type=str, default="2017-01-01", help="Requested start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default="", help="Optional end date YYYY-MM-DD")
    ap.add_argument("--out", type=str, default="", help="Optional output CSV path")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max klines per request (<=1000)")
    ap.add_argument("--sleep-ms", type=int, default=220, help="Sleep between successful requests")
    return ap.parse_args()


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").upper().replace("/", "").replace("-", "").replace("_", "").strip()


def _tf_to_ms(timeframe: str) -> int:
    tf = str(timeframe).strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60 * 1000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60 * 1000
    if tf.endswith("d"):
        return int(tf[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _ms_to_iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")


def _iso_to_ms(ts: str) -> Optional[int]:
    raw = str(ts or "").strip()
    if not raw:
        return None
    try:
        if raw.isdigit():
            return int(raw)
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _date_to_ms(date_s: str, *, end_of_day: bool = False) -> int:
    dt = datetime.strptime(date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
    return int(dt.timestamp() * 1000)


def _http_get_json(url: str, timeout_sec: float = 15.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_klines_page(symbol: str, timeframe: str, start_ms: int, end_ms: Optional[int], limit: int) -> list[list[Any]]:
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": timeframe,
        "startTime": int(start_ms),
        "limit": max(1, min(int(limit), 1000)),
    }
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    query = urllib.parse.urlencode(params)
    url = f"{API_BASE_URL}/api/v3/klines?{query}"

    backoff_sec = 0.8
    for attempt in range(1, 8):
        try:
            payload = _http_get_json(url)
            return payload if isinstance(payload, list) else []
        except Exception as exc:
            status = getattr(exc, "code", None)
            transient = status in {418, 429, 500, 502, 503, 504}
            if not transient and "timed out" not in str(exc).lower():
                raise
            if attempt >= 7:
                raise
            sleep_s = min(backoff_sec, 12.0)
            print(f"[backoff] attempt={attempt} status={status} sleeping={sleep_s:.2f}s")
            time.sleep(sleep_s)
            backoff_sec = min(backoff_sec * 2.0, 12.0)
    return []


def _iter_csv_rows(path: str) -> Iterable[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {k: str(v) for k, v in row.items()}


def _read_last_ts_ms(path: str) -> Optional[int]:
    if not os.path.exists(path):
        return None
    last_ms: Optional[int] = None
    for row in _iter_csv_rows(path):
        ts_ms = _iso_to_ms(row.get("ts", ""))
        if ts_ms is not None and (last_ms is None or ts_ms > last_ms):
            last_ms = ts_ms
    return last_ms


def _write_rows_append(path: str, rows: Iterable[dict[str, Any]]) -> int:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    exists = os.path.exists(path)
    wrote = 0
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ts", "open", "high", "low", "close", "volume"])
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
            wrote += 1
    return wrote


def _finalize_csv(path: str) -> int:
    dedup: dict[int, dict[str, Any]] = {}
    for row in _iter_csv_rows(path):
        ts_ms = _iso_to_ms(row.get("ts", ""))
        if ts_ms is None:
            continue
        dedup[ts_ms] = {
            "ts": _ms_to_iso_utc(ts_ms),
            "open": row.get("open", ""),
            "high": row.get("high", ""),
            "low": row.get("low", ""),
            "close": row.get("close", ""),
            "volume": row.get("volume", ""),
        }

    ordered = [dedup[ms] for ms in sorted(dedup.keys())]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ts", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(ordered)
    return len(ordered)


def _find_listing_start(symbol: str, timeframe: str, probe_from_ms: int, probe_to_ms: int, limit: int) -> int:
    # Probe por ventanas para evitar arrancar en una fecha incorrecta por endpoint/mercado equivocado.
    window_ms = 180 * 24 * 60 * 60 * 1000
    probe_ms = probe_from_ms
    while probe_ms <= probe_to_ms:
        rows = _fetch_klines_page(
            symbol=symbol,
            timeframe=timeframe,
            start_ms=probe_ms,
            end_ms=min(probe_ms + window_ms - 1, probe_to_ms),
            limit=limit,
        )
        if rows:
            first = int(rows[0][0])
            print(f"[probe] listing_start≈{_ms_to_iso_utc(first)}")
            return first
        print(f"[probe] empty window start={_ms_to_iso_utc(probe_ms)}")
        probe_ms += window_ms
    return probe_to_ms


def _default_out(symbol: str, timeframe: str) -> str:
    return os.path.join("Datasets", "BINANCE", symbol.upper(), timeframe, f"{symbol.upper()}_{timeframe}_full.csv")


def run_download(exchange: str, symbol: str, timeframe: str, start: str, end: str, out: str, limit: int, sleep_ms: int) -> None:
    if str(exchange).strip().lower() != "binance":
        raise ValueError("This downloader supports only --exchange binance")

    symbol_n = _normalize_symbol(symbol)
    tf_ms = _tf_to_ms(timeframe)
    now_ms = int(time.time() * 1000)
    end_ms = _date_to_ms(end, end_of_day=True) if end else now_ms
    requested_start_ms = _date_to_ms(start, end_of_day=False)

    out_path = out or _default_out(symbol_n, timeframe)
    resume_last = _read_last_ts_ms(out_path)

    listing_start_ms = _find_listing_start(
        symbol=symbol_n,
        timeframe=timeframe,
        probe_from_ms=requested_start_ms,
        probe_to_ms=end_ms,
        limit=limit,
    )
    cursor_ms = max(requested_start_ms, listing_start_ms)

    if resume_last is not None:
        cursor_ms = max(cursor_ms, resume_last + tf_ms)
        print(f"[resume] last_ts={_ms_to_iso_utc(resume_last)} -> next={_ms_to_iso_utc(cursor_ms)}")

    print(
        f"[start] exchange=binance symbol={symbol_n} timeframe={timeframe} "
        f"from={_ms_to_iso_utc(cursor_ms)} to={_ms_to_iso_utc(end_ms)} out={out_path}"
    )

    req_n = 0
    wrote_total = 0
    while cursor_ms <= end_ms:
        rows = _fetch_klines_page(symbol_n, timeframe, cursor_ms, end_ms, limit)
        req_n += 1

        if not rows:
            print(f"[stop] req={req_n} no rows at cursor={_ms_to_iso_utc(cursor_ms)}")
            break

        to_write: list[dict[str, Any]] = []
        for r in rows:
            try:
                open_ms = int(r[0])
                if open_ms > end_ms:
                    continue
                to_write.append(
                    {
                        "ts": _ms_to_iso_utc(open_ms),
                        "open": float(r[1]),
                        "high": float(r[2]),
                        "low": float(r[3]),
                        "close": float(r[4]),
                        "volume": float(r[5]),
                    }
                )
            except Exception:
                continue

        wrote = _write_rows_append(out_path, to_write)
        wrote_total += wrote

        first_ms = int(rows[0][0])
        last_ms = int(rows[-1][0])
        print(
            f"req#{req_n} fetched={len(rows)} wrote={wrote} "
            f"range=[{_ms_to_iso_utc(first_ms)} .. {_ms_to_iso_utc(last_ms)}]"
        )

        next_cursor = last_ms + tf_ms
        if next_cursor <= cursor_ms:
            next_cursor = cursor_ms + tf_ms
        cursor_ms = next_cursor

        if len(rows) < max(1, min(limit, 1000)) and cursor_ms > now_ms - tf_ms:
            break

        time.sleep(max(0, sleep_ms) / 1000.0)

    total_final = _finalize_csv(out_path)
    print(f"[done] requests={req_n} appended={wrote_total} total_candles={total_final} file={out_path}")


def main() -> None:
    args = _parse_args()
    run_download(
        exchange=args.exchange,
        symbol=args.symbol,
        timeframe=args.timeframe,
        start=args.start,
        end=args.end,
        out=args.out,
        limit=args.limit,
        sleep_ms=args.sleep_ms,
    )


if __name__ == "__main__":
    main()
