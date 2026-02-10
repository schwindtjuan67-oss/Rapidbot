from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from scripts.solusdt_vwap_bot_spot import (
    BTConfig,
    StrategyConfig,
    backtest_from_signals,
    compute_atr_pct_proxy,
    compute_metrics,
    generate_signals,
    load_ohlcv_csv,
)


def _fmt_pct(value: float) -> str:
    return f"{value:.4f}%"


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Backtest SPOT ETH/USDC long-only using CSV 5m data")
    ap.add_argument("--bt", action="store_true", help="Run backtest mode")
    ap.add_argument("--csv", type=str, required=True, help="Historical 5m CSV with ts,open,high,low,close,volume (expected: ETHUSDC_5m_fixed.csv)")
    ap.add_argument("--symbol", type=str, default="ETH/USDC")
    ap.add_argument("--fee_maker", type=float, default=0.0)
    ap.add_argument("--fee_taker", type=float, default=0.0)
    ap.add_argument("--enable_funding_cost", type=int, default=0)
    ap.add_argument("--slip_bps_entry", type=float, default=2.0)
    ap.add_argument("--slip_bps_stop", type=float, default=6.0)
    ap.add_argument("--tp_bidask_half_spread_bps", type=float, default=1.0)
    ap.add_argument("--risk_per_trade", type=float, default=0.03)
    ap.add_argument("--rr_min", type=float, default=1.0)
    ap.add_argument("--start_equity", type=float, default=5000.0)
    ap.add_argument("--outdir", type=str, default="")
    return ap


def _resolve_outdir(outdir: str) -> str:
    if outdir:
        return outdir
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return os.path.join("results", "bt_spot_eth_usdc", stamp)


def _print_trade_logs(trades_df: pd.DataFrame, slip_entry_bps: float, slip_stop_bps: float, tp_half_spread_bps: float) -> None:
    if trades_df.empty:
        print("No closed trades.")
        return

    for i, trade in trades_df.reset_index(drop=True).iterrows():
        entry = float(trade["entry"])
        exit_px = float(trade["exit"])
        qty = float(trade["qty"])
        side = str(trade["side"]).upper()
        exit_reason = str(trade["exit_reason"])

        pl_pct = ((exit_px - entry) / entry) * 100.0 if side == "LONG" else ((entry - exit_px) / entry) * 100.0
        exit_slip = slip_stop_bps if "STOP" in exit_reason.upper() else tp_half_spread_bps

        print(
            f"TRADE #{i + 1}: entry={entry:.6f} exit={exit_px:.6f} qty={qty:.8f} "
            f"pl%={pl_pct:.4f} slippage_bps(entry={slip_entry_bps:.2f},exit={exit_slip:.2f}) "
            f"reason={exit_reason}"
        )


def main() -> None:
    args = _build_parser().parse_args()

    if not args.bt:
        raise SystemExit("This entrypoint is backtest-only. Pass --bt.")

    if args.symbol != "ETH/USDC":
        raise SystemExit("Only --symbol ETH/USDC is supported by this entrypoint.")

    outdir = _resolve_outdir(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    print(
        "BT SPOT ETH/USDC — fees: "
        f"{args.fee_maker} / {args.fee_taker} — risk_per_trade: {args.risk_per_trade} "
        f"— slip: {args.slip_bps_entry}/{args.slip_bps_stop} — tp spread: {args.tp_bidask_half_spread_bps}"
    )

    df = load_ohlcv_csv(args.csv, strict=True)
    strat_cfg = StrategyConfig(
        symbol="ETHUSDC",
        enable_funding_cost=bool(args.enable_funding_cost),
        enable_tp_bidask_model=True,
        tp_bidask_half_spread_bps=float(args.tp_bidask_half_spread_bps),
    )

    sigdf = generate_signals(df, strat_cfg)
    # SPOT ETH/USDC entrypoint is long-only.
    sigdf.loc[sigdf["signal"] == "SHORT", "signal"] = ""

    atr_pct_proxy = compute_atr_pct_proxy(df, n=strat_cfg.daily_atr_len)

    bt = BTConfig(
        fee_taker=float(args.fee_taker),
        fee_maker=float(args.fee_maker),
        slippage_bps_entry=float(args.slip_bps_entry),
        slippage_bps_tp=0.0,
        slippage_bps_stop=float(args.slip_bps_stop),
        slip_model="fixed",
        initial_equity_usdt=float(args.start_equity),
        risk_pct_per_trade=float(args.risk_per_trade),
        max_notional_usdt=0.0,
        max_notional_mult=0.0,
        conservative_intrabar=True,
        enable_funding_cost=bool(args.enable_funding_cost),
        enable_tp_bidask_model=True,
        tp_bidask_half_spread_bps=float(args.tp_bidask_half_spread_bps),
    )

    trades_df, eq_df, final_equity, *_ = backtest_from_signals(sigdf, bt, strat_cfg, atr_pct_proxy=atr_pct_proxy)

    _print_trade_logs(
        trades_df,
        slip_entry_bps=float(args.slip_bps_entry),
        slip_stop_bps=float(args.slip_bps_stop),
        tp_half_spread_bps=float(args.tp_bidask_half_spread_bps),
    )

    start_ts = df["ts"].min() if not df.empty else None
    end_ts = df["ts"].max() if not df.empty else None
    metrics: dict[str, Any] = compute_metrics(trades_df, eq_df, float(args.start_equity), float(final_equity), start_ts, end_ts)

    total_pnl_usd = float(final_equity) - float(args.start_equity)
    win_rate_pct = float(metrics.get("winrate", 0.0)) * 100.0
    avg_trade_pnl = float(trades_df["pnl_net_usdt"].mean()) if not trades_df.empty else 0.0
    expectancy = avg_trade_pnl
    total_fees = float(trades_df["fees_usdt"].sum()) if not trades_df.empty and "fees_usdt" in trades_df.columns else 0.0

    trades_path = os.path.join(outdir, "trades.csv")
    eq_path = os.path.join(outdir, "equity_curve.csv")
    trades_df.to_csv(trades_path, index=False)
    eq_df.to_csv(eq_path, index=False)

    print(f"final equity: {final_equity:.6f}")
    print(f"total profit / loss in USD: {total_pnl_usd:.6f}")
    print(f"total % return: {_fmt_pct(float(metrics.get('return_pct', 0.0)))}")
    print(f"max drawdown (%): {_fmt_pct(float(metrics.get('max_drawdown_pct', 0.0) or 0.0))}")
    print(f"expectancy: {expectancy:.6f}")
    print(f"win rate: {_fmt_pct(win_rate_pct)}")
    print(f"average trade PnL: {avg_trade_pnl:.6f}")
    print(f"total trades: {int(metrics.get('trades', 0))}")
    print(f"realized fees: {total_fees:.6f}")
    print(f"equity curve file path: {eq_path}")
    print(f"trades report file path: {trades_path}")


if __name__ == "__main__":
    main()
