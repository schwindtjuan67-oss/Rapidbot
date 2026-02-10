"""
binance_vision_solusdt_5m.py

Downloader SOLUSDT 5m desde Binance Data Collection (data.binance.vision).
Default: USDⓈ-M futures (futures/um).

Instalar:
  pip install requests pandas

Ejemplos:
  # FUTURES (um) 5m:
  python binance_vision_solusdt_5m.py --start 2022-01-01 --end 2026-02-01 --out datasets/SOLUSDT/5m/SOLUSDT_5m_um.csv

  # SPOT 5m:
  python binance_vision_solusdt_5m.py --asset spot --start 2022-01-01 --end 2026-02-01 --out datasets/SOLUSDT/5m/SOLUSDT_5m_spot.csv

Notas:
- URLs siguen el patrón documentado por Binance Public Data:
  https://data.binance.vision/data/<asset>/<monthly|daily>/klines/<SYMBOL>/<TF>/<SYMBOL>-<TF>-<YYYY>-<MM>(-<DD>).zip
  y su correspondiente .CHECKSUM. (ver repo binance-public-data)
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import os
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import requests


BASE = "https://data.binance.vision/data"


# Klines columns (Binance official order)
KLINES_USDM_COLS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

KLINES_SPOT_COLS = KLINES_USDM_COLS  # mismo orden; ojo: spot desde 2025 puede venir en microsegundos


@dataclass
class DLConfig:
    asset: str = "futures/um"     # "futures/um" (USD-M), "futures/cm" (COIN-M), "spot"
    symbol: str = "SOLUSDT"
    timeframe: str = "5m"
    start: date = date(2022, 1, 1)
    end: date = date(2026, 2, 1)
    out_csv: str = "datasets/SOLUSDT/5m/SOLUSDT_5m_um.csv"
    cache_dir: str = "datasets/_cache_binance_vision"
    verify_checksum: bool = True
    keep_zips: bool = True
    timeout_sec: int = 60
    sleep_sec: float = 0.15
    mode: str = "auto"            # "auto" (monthly si existe, si no daily), o "daily", o "monthly"
    strict: bool = False          # si True, aborta ante ZIP/CSV corrupto


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _daterange(d0: date, d1: date) -> Iterable[date]:
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def _month_start_end(y: int, m: int) -> Tuple[date, date]:
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last_day)


def _iter_months(d0: date, d1: date) -> Iterable[Tuple[int, int]]:
    y, m = d0.year, d0.month
    while (y, m) <= (d1.year, d1.month):
        yield y, m
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1


def _url_zip(cfg: DLConfig, granularity: str, y: int, m: int, d: Optional[int] = None) -> str:
    # granularity: "monthly" o "daily"
    if granularity == "monthly":
        fname = f"{cfg.symbol}-{cfg.timeframe}-{y:04d}-{m:02d}.zip"
        return f"{BASE}/{cfg.asset}/monthly/klines/{cfg.symbol}/{cfg.timeframe}/{fname}"
    if granularity == "daily":
        assert d is not None
        fname = f"{cfg.symbol}-{cfg.timeframe}-{y:04d}-{m:02d}-{d:02d}.zip"
        return f"{BASE}/{cfg.asset}/daily/klines/{cfg.symbol}/{cfg.timeframe}/{fname}"
    raise ValueError("granularity must be monthly|daily")


def _url_checksum(zip_url: str) -> str:
    return zip_url + ".CHECKSUM"


def _head_exists(url: str, timeout: int) -> bool:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def _download_file(url: str, out_path: str, timeout: int) -> bool:
    _ensure_dir(out_path)
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            if r.status_code == 404:
                return False
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", "0") or "0")
            done = 0
            t0 = time.time()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    done += len(chunk)
                    if total > 0:
                        pct = 100.0 * done / total
                        if done == len(chunk) or (time.time() - t0) > 1.0:
                            t0 = time.time()
                            print(f"  -> {os.path.basename(out_path)} {pct:6.2f}% ({done/1e6:.1f}/{total/1e6:.1f} MB)")
        return True
    except Exception as e:
        print(f"[ERR] download failed url={url} err={e}")
        return False


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def _verify_checksum(zip_path: str, checksum_url: str, timeout: int) -> bool:
    # Descarga .CHECKSUM y compara
    try:
        r = requests.get(checksum_url, timeout=timeout)
        if r.status_code != 200:
            print(f"[WARN] checksum not found: {checksum_url} (status={r.status_code}) -> skip verify")
            return True
        text = r.text.strip()
        # formato típico: "<sha256>  <filename>"
        parts = text.split()
        if not parts:
            print("[WARN] checksum file empty -> skip verify")
            return True
        expected = parts[0].lower()
        actual = _sha256_file(zip_path)
        ok = (expected == actual)
        if not ok:
            print(f"[ERR] checksum mismatch!\n  expected={expected}\n  actual  ={actual}\n  file={zip_path}")
        return ok
    except Exception as e:
        print(f"[WARN] checksum verify failed: {e} -> skip verify")
        return True


def _extract_zip_to_df(zip_path: str, cfg: DLConfig) -> pd.DataFrame:
    cols = KLINES_SPOT_COLS if cfg.asset == "spot" else KLINES_USDM_COLS
    with zipfile.ZipFile(zip_path, "r") as z:
        # usualmente hay 1 CSV adentro
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            msg = f"No CSV inside zip: {zip_path}"
            if cfg.strict:
                raise RuntimeError(msg)
            print(f"[WARN] {msg}")
            return pd.DataFrame(columns=cols)
        # si hay más de uno, concatenamos
        dfs = []
        for name in names:
            with z.open(name) as f:
                df = pd.read_csv(f, header=None, dtype=str)
                # asignar columnas
                if df.shape[1] != len(cols):
                    msg = f"Unexpected columns in {name}: got {df.shape[1]}, expected {len(cols)}"
                    if cfg.strict:
                        raise RuntimeError(msg)
                    print(f"[WARN] {msg} -> skip CSV")
                    continue
                df.columns = cols
                dfs.append(df)
        if not dfs:
            return pd.DataFrame(columns=cols)
        out = pd.concat(dfs, ignore_index=True)
        return out


def _open_time_to_ts(open_time_series: pd.Series, asset: str) -> pd.Series:
    # FUTURES: ms
    # SPOT: desde 2025-01-01 puede venir en microseconds -> detectamos por magnitud
    x = pd.to_numeric(open_time_series, errors="coerce")
    # si es micro (>= 1e14) usamos unit='us'
    x_non_null = x.dropna()
    is_micro = (not x_non_null.empty) and (x_non_null.max() >= 100_000_000_000_000)
    unit = "us" if (asset == "spot" and is_micro) else "ms"
    # pd.to_datetime maneja NaN -> NaT si algo quedó inválido
    return pd.to_datetime(x, unit=unit, utc=True, errors="coerce")


def print_summary(raw_before: int, raw_after: int, out_df: pd.DataFrame) -> None:
    dropped = raw_before - raw_after
    print(f"[SUMMARY] dropped_open_time_invalid={dropped} raw_before={raw_before} raw_after={raw_after}")
    if out_df.empty:
        print("[SUMMARY] output is empty after cleaning.")
    else:
        print(
            f"[SUMMARY] output_rows={len(out_df)} "
            f"from={out_df['ts'].iloc[0]} to={out_df['ts'].iloc[-1]}"
        )


def build_dataset(cfg: DLConfig) -> None:
    _ensure_dir(cfg.out_csv)
    os.makedirs(cfg.cache_dir, exist_ok=True)

    # recolectar zips a usar
    zip_paths: List[str] = []

    for y, m in _iter_months(cfg.start, cfg.end):
        m_start, m_end = _month_start_end(y, m)
        use_start = max(cfg.start, m_start)
        use_end = min(cfg.end, m_end)

        if cfg.mode == "daily":
            monthly_ok = False
        elif cfg.mode == "monthly":
            monthly_ok = True
        else:
            # auto: monthly si existe
            monthly_url = _url_zip(cfg, "monthly", y, m)
            monthly_ok = _head_exists(monthly_url, cfg.timeout_sec)

        if monthly_ok:
            url = _url_zip(cfg, "monthly", y, m)
            fname = os.path.basename(url)
            zpath = os.path.join(cfg.cache_dir, fname)
            if not os.path.exists(zpath):
                print(f"[DL][MONTHLY] {url}")
                ok = _download_file(url, zpath, cfg.timeout_sec)
                if not ok:
                    print("  monthly missing -> fallback daily")
                    monthly_ok = False
                else:
                    if cfg.verify_checksum:
                        c_ok = _verify_checksum(zpath, _url_checksum(url), cfg.timeout_sec)
                        if not c_ok:
                            raise RuntimeError("Checksum mismatch (monthly).")
                time.sleep(cfg.sleep_sec)

            if monthly_ok:
                zip_paths.append(zpath)
                continue  # no bajamos daily para este mes

        # fallback daily para el rango del mes
        for d in _daterange(use_start, use_end):
            url = _url_zip(cfg, "daily", d.year, d.month, d.day)
            fname = os.path.basename(url)
            zpath = os.path.join(cfg.cache_dir, fname)
            if not os.path.exists(zpath):
                print(f"[DL][DAILY] {url}")
                ok = _download_file(url, zpath, cfg.timeout_sec)
                if not ok:
                    print("  -> 404 (no existe), skip")
                    continue
                if cfg.verify_checksum:
                    c_ok = _verify_checksum(zpath, _url_checksum(url), cfg.timeout_sec)
                    if not c_ok:
                        raise RuntimeError("Checksum mismatch (daily).")
                time.sleep(cfg.sleep_sec)
            zip_paths.append(zpath)

    if not zip_paths:
        raise RuntimeError("No zip files downloaded/found for the given range.")

    # cargar + concatenar
    frames = []
    for zp in zip_paths:
        try:
            df = _extract_zip_to_df(zp, cfg)
            if df.empty:
                print(f"[WARN] empty dataframe from {zp}")
                continue
            frames.append(df)
        except Exception as e:
            if cfg.strict:
                raise
            print(f"[WARN] failed reading {zp}: {e}")

    if not frames:
        raise RuntimeError("All downloads failed to parse.")

    raw = pd.concat(frames, ignore_index=True)
    if raw.empty:
        raise RuntimeError("All parsed CSVs were empty. Nothing to build.")

    raw_before = len(raw)
    # Normalizar open_time y dropear filas inválidas antes de convertir a datetime.
    raw["open_time_num"] = pd.to_numeric(raw["open_time"], errors="coerce")
    raw = raw.dropna(subset=["open_time_num"])
    raw_after = len(raw)
    if raw_after != raw_before:
        print(f"[INFO] dropped rows with invalid open_time: {raw_before - raw_after}")

    # convertir ts + filtrar por rango exacto
    raw["ts"] = _open_time_to_ts(raw["open_time_num"], cfg.asset)
    start_ts = pd.Timestamp(cfg.start, tz="UTC")
    end_ts = pd.Timestamp(cfg.end + timedelta(days=1), tz="UTC")  # end inclusive (día completo)
    raw = raw[(raw["ts"] >= start_ts) & (raw["ts"] < end_ts)]

    # normalizar columnas para tu backtest: ts, open, high, low, close, volume
    out = raw[["ts", "open", "high", "low", "close", "volume"]].copy()
    for c in ["open", "high", "low", "close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["ts", "open", "high", "low", "close", "volume"])
    out = out.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    print_summary(raw_before, raw_after, out)
    if out.empty:
        raise RuntimeError("No valid rows after cleaning. Check source data range/quality.")

    # guardar
    out.to_csv(cfg.out_csv, index=False)
    print(f"[OK] Saved dataset: {cfg.out_csv} | rows={len(out)} | from={out['ts'].iloc[0]} | to={out['ts'].iloc[-1]}")

    if not cfg.keep_zips:
        for zp in set(zip_paths):
            try:
                os.remove(zp)
            except Exception:
                pass


def parse_args() -> DLConfig:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asset", type=str, default="futures/um", choices=["futures/um", "futures/cm", "spot"])
    ap.add_argument("--symbol", type=str, default="SOLUSDT")
    ap.add_argument("--timeframe", type=str, default="5m")
    ap.add_argument("--start", type=str, required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--end", type=str, required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--cache", type=str, default="datasets/_cache_binance_vision")
    ap.add_argument("--no-checksum", action="store_true")
    ap.add_argument("--no-keep-zips", action="store_true")
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--sleep", type=float, default=0.15)
    ap.add_argument("--mode", type=str, default="auto", choices=["auto", "daily", "monthly"])
    ap.add_argument("--strict", action="store_true", help="Abort on corrupt zip/CSV instead of skipping.")
    args = ap.parse_args()

    def to_date(s: str) -> date:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()

    return DLConfig(
        asset=args.asset,
        symbol=args.symbol,
        timeframe=args.timeframe,
        start=to_date(args.start),
        end=to_date(args.end),
        out_csv=args.out,
        cache_dir=args.cache,
        verify_checksum=(not args.no_checksum),
        keep_zips=(not args.no_keep_zips),
        timeout_sec=args.timeout,
        sleep_sec=args.sleep,
        mode=args.mode,
        strict=args.strict,
    )


if __name__ == "__main__":
    cfg = parse_args()
    build_dataset(cfg)
