import argparse
import datetime as dt
import io
import zipfile
from pathlib import Path

import pandas as pd
import requests

BASE = "https://data.binance.vision/data/spot/monthly/klines"
INTERVAL = "5m"

# Binance kline columns in vision zip (no header)
RAW_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

def month_range(start_ym: str, end_ym: str):
    ys, ms = map(int, start_ym.split("-"))
    ye, me = map(int, end_ym.split("-"))
    cur = dt.date(ys, ms, 1)
    end = dt.date(ye, me, 1)
    out = []
    while cur <= end:
        out.append(cur.strftime("%Y-%m"))
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return out

def fetch_month_zip(symbol: str, ym: str) -> bytes:
    url = f"{BASE}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{ym}.zip"
    r = requests.get(url, timeout=60)
    if r.status_code == 404:
        return b""
    r.raise_for_status()
    return r.content

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, help="Ej: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, BNBUSDC, LTCUSDC")
    ap.add_argument("--start_ym", default="2017-01")
    ap.add_argument("--end_ym", default=dt.date.today().strftime("%Y-%m"))
    ap.add_argument("--out", required=True, help="CSV salida")
    args = ap.parse_args()

    symbol = args.symbol.upper()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    hit = 0
    miss = 0
    for ym in month_range(args.start_ym, args.end_ym):
        zbytes = fetch_month_zip(symbol, ym)
        if not zbytes:
            miss += 1
            continue
        hit += 1
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                df = pd.read_csv(f, header=None, names=RAW_COLS)

        # Keep only needed columns and convert ts -> UTC string
        # open_time is ms since epoch
        ts = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        out_df = pd.DataFrame({
            "ts": ts.dt.strftime("%Y-%m-%d %H:%M:%S+00:00"),
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["close"],
            "volume": df["volume"],
        })
        frames.append(out_df)
        print(f"[OK] {symbol} {ym} rows={len(out_df)}")

    if not frames:
        raise SystemExit(f"No se encontró data para {symbol} en {args.start_ym}..{args.end_ym} (hit=0, miss={miss}).")

    data = pd.concat(frames, ignore_index=True)

    # Dedup safety (shouldn't happen, but protects you)
    data = data.drop_duplicates(subset=["ts"], keep="last")

    data.to_csv(out_path, index=False)
    print(f"\n[SAVED] {out_path} rows={len(data)} | months_hit={hit} months_miss={miss}")

if __name__ == "__main__":
    main()
