from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone


def _parse_ts(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect OHLCV CSV integrity")
    ap.add_argument("--csv", type=str, required=True)
    args = ap.parse_args()

    rows = 0
    bad_ts_count = 0
    min_ts: datetime | None = None
    max_ts: datetime | None = None

    with open(args.csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows += 1
            ts = _parse_ts(str(row.get("ts", "")))
            if ts is None:
                bad_ts_count += 1
                continue
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts

    print(f"rows={rows}")
    print(f"min_ts={min_ts.isoformat(sep=' ') if min_ts else None}")
    print(f"max_ts={max_ts.isoformat(sep=' ') if max_ts else None}")
    print(f"bad_ts_count={bad_ts_count}")


if __name__ == "__main__":
    main()
