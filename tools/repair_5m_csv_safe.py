import argparse
from pathlib import Path
import pandas as pd

REQ_COLS = ["ts", "open", "high", "low", "close", "volume"]

def parse_ts_to_ms(series: pd.Series) -> pd.Series:
    # Try numeric first (ms)
    s_num = pd.to_numeric(series, errors="coerce")
    if s_num.notna().mean() > 0.95:
        return s_num.astype("int64")

    # Otherwise parse as datetime string -> ms
    s_dt = pd.to_datetime(series, utc=True, errors="coerce")
    if s_dt.notna().mean() < 0.95:
        bad = series[s_dt.isna()].head(5).tolist()
        raise SystemExit(f"ts parse failed for many rows. Examples: {bad}")

    return (s_dt.view("int64") // 1_000_000).astype("int64")  # ns -> ms

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="CSV input")
    ap.add_argument("--out", dest="out", default="", help="CSV output (default: <input>.repaired.csv)")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    inp = Path(args.inp)
    out = Path(args.out) if args.out else inp.with_suffix(inp.suffix + ".repaired.csv")

    df = pd.read_csv(inp)

    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

    rows0 = len(df)

    ts_ms = parse_ts_to_ms(df["ts"])
    df = df.copy()
    df["ts"] = ts_ms

    df = df.sort_values("ts", kind="mergesort")
    dup_count = int(df.duplicated(subset=["ts"]).sum())
    df = df.drop_duplicates(subset=["ts"], keep="last")

    deltas = df["ts"].diff().dropna()
    non5m = deltas[deltas != 300000]
    non5m_count = int(len(non5m))

    if args.report:
        print(f"[REPAIR] file={inp}")
        print(f" rows_in={rows0} rows_out={len(df)} removed_dups={dup_count}")
        if len(df):
            print(f" ts_min={df['ts'].iloc[0]} ts_max={df['ts'].iloc[-1]}")
        print(f" non5m_deltas_count={non5m_count}")
        if non5m_count:
            print(" sample non-5m deltas (ms):", non5m.head(10).astype(int).tolist())

    df.to_csv(out, index=False)
    print(f"[SAVED] {out} rows={len(df)}")

if __name__ == "__main__":
    main()
