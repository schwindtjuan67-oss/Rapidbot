from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, Optional


DEFAULT_BASE_URLS = {
    "mexc": "https://api.mexc.com",
    "binance": "https://api.binance.com",
}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Download spot OHLCV candles to CSV (ts/open/high/low/close/volume).")
    ap.add_argument("--exchange", type=str, default="mexc")
    ap.add_argument("--symbol", type=str, required=True, help="e.g. BTC/USDC or BTCUSDC")
    ap.add_argument("--timeframe", type=str, default="5m")
    ap.add_argument("--since", type=str, default="", help="YYYY-MM-DD (optional)")
    ap.add_argument("--until", type=str, default="", help="YYYY-MM-DD (optional)")
    ap.add_argument("--out", type=str, required=True, help="Output CSV path")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--sleep_ms", type=int, default=200)
    return ap.parse_args()


def _normalize_symbol(symbol: str, exchange: str) -> str:
    sym = str(symbol or "").strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if not sym:
        raise ValueError("Empty symbol")
    if exchange.lower() == "mexc":
        return sym
    return sym


def _tf_to_ms(timeframe: str) -> int:
    tf = str(timeframe or "").strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60 * 1000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60 * 1000
    if tf.endswith("d"):
        return int(tf[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _date_to_ms(date_s: str, *, end_of_day: bool = False) -> int:
    dt = datetime.strptime(date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
    return int(dt.timestamp() * 1000)


def _ms_to_iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")


def _iso_to_dt(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _http_get_json(url: str, timeout_sec: float = 12.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "Rapidbot-DatasetDownloader/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def _fetch_klines_page(
    *,
    base_url: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: Optional[int],
    limit: int,
    max_retries: int = 6,
) -> list[list[Any]]:
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": timeframe,
        "startTime": int(start_ms),
        "limit": int(limit),
    }
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    query = urllib.parse.urlencode(params)
    # Binance Spot and MEXC Spot both expose /api/v3/klines.
    # If the wrong exchange/symbol is used (e.g. MEXC BTCUSDC), historical start may appear much newer.
    url = f"{base_url.rstrip('/')}/api/v3/klines?{query}"

    delay = 0.8
    for attempt in range(1, max_retries + 1):
        try:
            payload = _http_get_json(url)
            if isinstance(payload, list):
                return payload
            return []
        except Exception as exc:
            status = getattr(exc, "code", None)
            transient = status in {429, 500, 502, 503, 504} or "timed out" in str(exc).lower()
            if not transient or attempt == max_retries:
                raise
            time.sleep(delay)
            delay = min(delay * 2.0, 10.0)
    return []


def _write_rows_append(path: str, rows: Iterable[dict[str, Any]]) -> int:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    exists = os.path.exists(path)
    count = 0
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ts", "open", "high", "low", "close", "volume"])
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def _finalize_csv(path: str) -> None:
    dedup: dict[str, dict[str, str]] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = str(row.get("ts", "")).strip()
            dt = _iso_to_dt(ts)
            if dt is None:
                continue
            ts_norm = dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
            dedup[ts_norm] = {
                "ts": ts_norm,
                "open": str(row.get("open", "")),
                "high": str(row.get("high", "")),
                "low": str(row.get("low", "")),
                "close": str(row.get("close", "")),
                "volume": str(row.get("volume", "")),
            }

    rows_sorted = [dedup[k] for k in sorted(dedup.keys())]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ts", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows_sorted)


def _find_first_available_start_ms(
    *,
    base_url: str,
    symbol: str,
    timeframe: str,
    probe_from_ms: int,
    probe_to_ms: int,
    limit: int,
) -> int:
    step_ms = 180 * 24 * 60 * 60 * 1000
    probe = probe_from_ms
    while probe <= probe_to_ms:
        rows = _fetch_klines_page(
            base_url=base_url,
            symbol=symbol,
            timeframe=timeframe,
            start_ms=probe,
            end_ms=min(probe + step_ms - 1, probe_to_ms),
            limit=limit,
        )
        if rows:
            first_open_ms = int(rows[0][0])
            print(f"[probe] data found starting around {_ms_to_iso_utc(first_open_ms)}")
            return first_open_ms
        print(f"[probe] no data in window starting {_ms_to_iso_utc(probe)}")
        probe += step_ms
    return probe_to_ms


def run_download(
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    since: str,
    until: str,
    out: str,
    limit: int,
    sleep_ms: int,
) -> None:
    ex = exchange.lower().strip()
    if ex not in DEFAULT_BASE_URLS:
        raise ValueError(f"Unsupported exchange: {exchange}")
    base_url = DEFAULT_BASE_URLS[ex]
    norm_symbol = _normalize_symbol(symbol, ex)
    tf_ms = _tf_to_ms(timeframe)

    now_ms = int(time.time() * 1000)
    until_ms = _date_to_ms(until, end_of_day=True) if until else now_ms

    if since:
        start_ms = _date_to_ms(since, end_of_day=False)
    else:
        start_ms = _find_first_available_start_ms(
            base_url=base_url,
            symbol=norm_symbol,
            timeframe=timeframe,
            probe_from_ms=_date_to_ms("2017-01-01", end_of_day=False),
            probe_to_ms=until_ms,
            limit=limit,
        )

    total_rows = 0
    request_count = 0
    cursor_ms = start_ms

    print(
        f"Starting download exchange={ex} symbol={norm_symbol} timeframe={timeframe} "
        f"from={_ms_to_iso_utc(cursor_ms)} to={_ms_to_iso_utc(until_ms)} out={out}"
    )

    while cursor_ms <= until_ms:
        rows = _fetch_klines_page(
            base_url=base_url,
            symbol=norm_symbol,
            timeframe=timeframe,
            start_ms=cursor_ms,
            end_ms=until_ms,
            limit=limit,
        )
        request_count += 1

        if not rows:
            print(f"No more rows at cursor={_ms_to_iso_utc(cursor_ms)} (requests={request_count}).")
            break

        out_rows = []
        for r in rows:
            try:
                open_ms = int(r[0])
                if open_ms > until_ms:
                    continue
                out_rows.append(
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

        wrote = _write_rows_append(out, out_rows)
        total_rows += wrote

        first_ms = int(rows[0][0])
        last_ms = int(rows[-1][0])
        print(
            f"req={request_count} fetched={len(rows)} wrote={wrote} total={total_rows} "
            f"range=[{_ms_to_iso_utc(first_ms)} .. {_ms_to_iso_utc(last_ms)}]"
        )

        next_cursor = last_ms + tf_ms
        if next_cursor <= cursor_ms:
            next_cursor = cursor_ms + tf_ms
        cursor_ms = next_cursor

        if len(rows) < limit and cursor_ms > now_ms - tf_ms:
            break

        time.sleep(max(0, sleep_ms) / 1000.0)

    _finalize_csv(out)
    print(f"Done. requests={request_count} rows_written={total_rows} file={out}")


def main() -> None:
    args = _parse_args()
    run_download(
        exchange=args.exchange,
        symbol=args.symbol,
        timeframe=args.timeframe,
        since=args.since,
        until=args.until,
        out=args.out,
        limit=args.limit,
        sleep_ms=args.sleep_ms,
    )


if __name__ == "__main__":
    main()
