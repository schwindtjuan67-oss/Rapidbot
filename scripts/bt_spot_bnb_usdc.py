from __future__ import annotations

"""
Adapter Binance SPOT 5m downloader for BNBUSDC.

Usage (Windows PowerShell):
  python -m scripts.bt_spot_bnb_usdc --start 2017-01-01 --end 2026-02-13
"""

import argparse
from pathlib import Path

from scripts.dl_spot_binance import run_download

SYMBOL = "BNBUSDC"
INTERVAL = "5m"
DATASETS_ROOT = Path("Datasets")


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Download Binance SPOT BNBUSDC 5m candles into the repo dataset layout.")
    ap.add_argument("--start", type=str, default="2017-01-01", help="Requested start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default="", help="Optional end date YYYY-MM-DD")
    ap.add_argument("--out", type=str, default="", help="Optional output CSV path")
    ap.add_argument("--limit", type=int, default=1000, help="Max klines per request (<=1000)")
    ap.add_argument("--sleep-ms", type=int, default=220, help="Sleep between successful requests")
    return ap


def _default_out() -> Path:
    return DATASETS_ROOT / SYMBOL / INTERVAL / f"{SYMBOL}_{INTERVAL}_spot.csv"


def main() -> None:
    args = _build_parser().parse_args()

    out_path = Path(args.out) if args.out else _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"[adapter:start] symbol={SYMBOL} interval={INTERVAL} start={args.start} "
        f"end={args.end or '<now>'} out={out_path}"
    )

    run_download(
        exchange="binance",
        symbol=SYMBOL,
        timeframe=INTERVAL,
        start=args.start,
        end=args.end,
        out=str(out_path),
        limit=args.limit,
        sleep_ms=args.sleep_ms,
    )

    print(f"[adapter:done] output={out_path}")


if __name__ == "__main__":
    main()
