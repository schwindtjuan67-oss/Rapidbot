from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from scripts.solusdt_vwap_bot_spot import (
    BTConfig,
    StrategyConfig,
    TRADE_COLUMNS,
    _apply_slip,
    _daily_key,
    _fee_cost,
    _tp_fill_price,
    compute_atr_pct_proxy,
    compute_metrics,
    compute_risk_usd,
    generate_signals,
    load_ohlcv_csv,
)


@dataclass
class SymbolState:
    symbol: str
    sigdf: pd.DataFrame
    index_by_ts: Dict[pd.Timestamp, int]
    pending: Optional[Dict[str, Any]] = None
    pending_age: int = 0
    pos: Optional[Dict[str, Any]] = None


DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "BNBUSDC",
    "LTCUSDC",
]


def _resolve_symbol_csv(symbol: str) -> Path:
    symbol_dir = Path("Rapidbot") / "Datasets" / "BINANCE" / symbol
    if not symbol_dir.exists():
        # Repo-local fallback when cwd is already Rapidbot.
        symbol_dir = Path("Datasets") / "BINANCE" / symbol
    if not symbol_dir.exists():
        raise ValueError(f"Missing dataset directory for {symbol}: {symbol_dir}")

    candidates = sorted(p for p in symbol_dir.rglob("*.csv") if "5m" in p.name.lower())
    if not candidates:
        raise ValueError(f"No 5m CSV found for {symbol} under {symbol_dir}")
    return candidates[0]


def _validate_loaded_df(symbol: str, df: pd.DataFrame) -> None:
    required_cols = {"ts", "open", "high", "low", "close", "volume"}
    if set(df.columns) != required_cols:
        raise ValueError(f"{symbol}: invalid columns {list(df.columns)}; expected {sorted(required_cols)}")

    ohlcv = df[["open", "high", "low", "close", "volume"]]
    if not all(pd.api.types.is_numeric_dtype(ohlcv[c]) for c in ohlcv.columns):
        raise ValueError(f"{symbol}: OHLCV columns must be numeric")
    if not df["ts"].is_monotonic_increasing:
        raise ValueError(f"{symbol}: ts is not sorted ascending")
    if df["ts"].duplicated().any():
        raise ValueError(f"{symbol}: duplicated ts found after loading")


def validate_5m_ts(df: pd.DataFrame, symbol: str, expected_minutes: int = 5, tol_ms: int = 1) -> None:
    if "ts" not in df.columns:
        return

    ts = df["ts"]
    total_deltas = max(0, len(ts) - 1)
    if total_deltas == 0:
        return

    expected_ms = int(expected_minutes * 60 * 1000)
    tol_ms = int(max(0, tol_ms))

    if pd.api.types.is_datetime64_any_dtype(ts):
        deltas_ns = ts.diff().dropna().astype("timedelta64[ns]").astype("int64")
        expected_delta = expected_ms * 1_000_000
        tol = tol_ms * 1_000_000
        deltas_compare = deltas_ns.to_numpy(dtype=np.int64)
        deltas_ms = deltas_ns / 1_000_000.0
        unit_label = "datetime64[ns]"
    else:
        ts_num = pd.to_numeric(ts, errors="coerce")
        if ts_num.isna().any():
            bad_rows = int(ts_num.isna().sum())
            raise ValueError(f"{symbol}: ts contains {bad_rows} non-numeric values")

        ts_i64 = ts_num.astype("int64")
        abs_max = int(np.max(np.abs(ts_i64.to_numpy(dtype=np.int64)))) if len(ts_i64) else 0
        # Epoch seconds are ~1e9-1e10; epoch milliseconds are ~1e12-1e13.
        unit_scale_to_ms = 1000 if abs_max < 100_000_000_000 else 1
        detected_unit = "seconds" if unit_scale_to_ms == 1000 else "milliseconds"

        deltas_raw = ts_i64.diff().dropna().astype("int64")
        deltas_compare = deltas_raw.to_numpy(dtype=np.int64)
        expected_delta = expected_ms // unit_scale_to_ms
        tol = max(1, tol_ms // unit_scale_to_ms) if unit_scale_to_ms > 1 else tol_ms
        deltas_ms = deltas_raw.astype("float64") * float(unit_scale_to_ms)
        unit_label = f"numeric({detected_unit})"

    if deltas_compare.size == 0:
        return

    bad_mask = ~np.isclose(deltas_compare, expected_delta, atol=tol)
    bad_count = int(np.sum(bad_mask))
    if bad_count <= 0:
        return

    min_delta_sec = float(np.min(deltas_ms) / 1000.0)
    max_delta_sec = float(np.max(deltas_ms) / 1000.0)

    top_counts = pd.Series(deltas_ms).round(3).value_counts().head(5)
    top_human = [f"{float(delta_ms) / 1000.0:g}s x{int(cnt)}" for delta_ms, cnt in top_counts.items()]

    print(
        f"WARNING: {symbol}: detected non-{expected_minutes}m delta in ts "
        f"(expected {expected_ms} ms, unit={unit_label}) | bad={bad_count}/{total_deltas} | "
        f"min_delta={min_delta_sec:g}s | max_delta={max_delta_sec:g}s | "
        f"top_deltas={top_human}"
    )


def _open_risk_usd(states: Dict[str, SymbolState]) -> float:
    total = 0.0
    for st in states.values():
        if st.pos is None:
            continue
        qty = float(st.pos.get("qty_remaining", 0.0))
        if qty <= 0:
            continue
        entry = float(st.pos.get("entry_fill") or st.pos.get("entry") or 0.0)
        stop = float(st.pos.get("stop_active") or st.pos.get("stop") or 0.0)
        total += max(0.0, (entry - stop) * qty)
    return float(total)


def _size_qty(entry: float, stop: float, equity: float, bt: BTConfig, states: Dict[str, SymbolState]) -> Dict[str, float]:
    dist = abs(entry - stop)
    intended = compute_risk_usd(equity, bt.risk_pct_per_trade)
    if (not np.isfinite(dist)) or dist <= 0:
        return {"qty": 0.0, "intended": intended, "effective": 0.0, "eff_pct": 0.0, "budget_hit": 0}

    budget_total = max(0.0, float(bt.max_portfolio_risk_pct) * float(equity))
    rem_budget = max(0.0, budget_total - _open_risk_usd(states))
    effective = min(intended, rem_budget)
    budget_hit = int(rem_budget < intended)
    qty = (effective / dist) if dist > 0 else 0.0

    notional_cap = float(bt.legacy_fixed_notional_usd) if float(bt.legacy_fixed_notional_usd) > 0 else float(bt.max_notional_usdt)
    if bt.max_notional_mult > 0:
        notional_cap = max(notional_cap, float(equity) * float(bt.max_notional_mult))
    if bt.max_notional_pct_equity > 0:
        notional_cap = max(notional_cap, float(equity) * float(bt.max_notional_pct_equity))
    if notional_cap > 0:
        qty = min(qty, notional_cap / entry)

    min_forced = 0
    if qty * entry < bt.min_notional_usd:
        qty = bt.min_notional_usd / entry
        min_forced = 1
    if qty * entry > equity:
        qty = max(0.0, equity / entry)

    eff = abs(entry - stop) * qty
    eff_pct = (eff / equity) if equity > 0 else 0.0
    return {
        "qty": float(max(0.0, qty)),
        "intended": float(intended),
        "effective": float(eff),
        "eff_pct": float(eff_pct),
        "budget_hit": int(budget_hit),
        "min_forced": int(min_forced),
    }


def run_portfolio_backtest(args: argparse.Namespace) -> None:
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise ValueError("No symbols configured")

    if args.csvs:
        csvs = [c.strip() for c in args.csvs.split(",") if c.strip()]
        if len(symbols) != len(csvs):
            raise ValueError("--symbols and --csvs must have same length")
    else:
        csvs = [str(_resolve_symbol_csv(symbol)) for symbol in symbols]

    strat_cfg = StrategyConfig(
        enable_tp_bidask_model=bool(args.enable_tp_bidask),
        tp_bidask_half_spread_bps=args.tp_bidask_half_spread_bps,
        enable_daily_dd_kill=bool(args.daily_dd_kill),
        enable_max_stops_kill=bool(args.max_stops_kill),
    )

    bt = BTConfig(
        initial_equity_usdt=args.equity,
        risk_pct_per_trade=args.risk_per_trade,
        max_positions=args.max_positions,
        max_portfolio_risk_pct=args.max_portfolio_risk_pct,
        min_notional_usd=args.min_notional_usd,
        slippage_bps_entry=args.slip_bps_entry,
        slippage_bps_stop=args.slip_bps_stop,
        fee_taker=args.fee_taker,
        fee_maker=args.fee_maker,
        enable_tp_bidask_model=bool(args.enable_tp_bidask),
        tp_bidask_half_spread_bps=args.tp_bidask_half_spread_bps,
        exec_policy=args.exec_policy,
    )

    states: Dict[str, SymbolState] = {}
    all_ts: set[pd.Timestamp] = set()
    start_ts: Optional[pd.Timestamp] = None
    end_ts: Optional[pd.Timestamp] = None

    for symbol, csv_path in zip(symbols, csvs):
        df = load_ohlcv_csv(csv_path, strict=False)
        _validate_loaded_df(symbol, df)
        if bool(args.validate_5m):
            validate_5m_ts(df, symbol)
        if df.empty:
            continue
        print(f"Loaded {symbol} | rows={len(df)} | from={df['ts'].min()} | to={df['ts'].max()}")
        atr_proxy = compute_atr_pct_proxy(df, n=strat_cfg.daily_atr_len)
        if len(atr_proxy) == len(df):
            df = df.copy()
            df["atr_pct_proxy"] = atr_proxy.values
        sigdf = generate_signals(df, strat_cfg).reset_index(drop=True)
        idx_map = {pd.Timestamp(ts): i for i, ts in enumerate(sigdf["ts"].tolist())}
        states[symbol] = SymbolState(symbol=symbol, sigdf=sigdf, index_by_ts=idx_map)
        all_ts.update(idx_map.keys())
        s0, s1 = sigdf["ts"].min(), sigdf["ts"].max()
        start_ts = s0 if start_ts is None else min(start_ts, s0)
        end_ts = s1 if end_ts is None else max(end_ts, s1)

    if not states:
        raise ValueError("No data loaded")

    eq_curve: List[Dict[str, Any]] = []
    trades: List[Dict[str, Any]] = []
    equity = float(bt.initial_equity_usdt)
    day_start_eq = equity
    day_key: Optional[str] = None
    stops_today = 0
    total_bars = 0
    bars_in_market = 0
    sum_open_positions_count = 0
    open_positions_hist: defaultdict[int, int] = defaultdict(int)
    sum_exposure_pct = 0.0
    max_exposure_pct = 0.0
    bars_cap_binded = 0
    eligible_entries = 0
    blocked_entries = 0

    def record_eq(ts: pd.Timestamp, note: str = "") -> None:
        eq_curve.append({"ts": ts, "equity_usdt": equity, "note": note})

    for timeline_i, ts in enumerate(sorted(all_ts)):
        if day_key != _daily_key(ts, "UTC"):
            day_key = _daily_key(ts, "UTC")
            day_start_eq = equity
            stops_today = 0

        dd_hit = bool(args.daily_dd_kill) and day_start_eq > 0 and ((equity / day_start_eq) - 1.0) <= -abs(args.daily_dd_limit)
        max_stops_hit = bool(args.max_stops_kill) and stops_today >= max(1, int(args.max_stops_per_day))

        for sym, st in states.items():
            i = st.index_by_ts.get(ts)
            if i is None or i < 1:
                continue
            prev = st.sigdf.iloc[i - 1]
            row = st.sigdf.iloc[i]

            if st.pos is None and st.pending is None and (not dd_hit) and (not max_stops_hit):
                if str(prev.get("signal", "")).upper().strip() == "LONG":
                    eligible_entries += 1
                    open_positions = sum(1 for s in states.values() if s.pos is not None)
                    blocked_by_max_positions = open_positions >= int(bt.max_positions)
                    blocked_by_risk_cap = False
                    if not blocked_by_max_positions:
                        entry = float(prev["entry_long"])
                        stop = float(prev["stop_long"])
                        tp1 = float(prev["tp1_long"])
                        tp2 = float(prev["tp2_long"])
                        if np.isfinite([entry, stop, tp1, tp2]).all():
                            sz = _size_qty(entry, stop, equity, bt, states)
                            if sz["qty"] > 0:
                                st.pending = {
                                    "side": "LONG", "signal_ts": prev["ts"], "entry": entry, "stop": stop,
                                    "tp1": tp1, "tp2": tp2, "qty": sz["qty"], "qty_remaining": sz["qty"],
                                    "took_tp1": False, "realized_pnl": 0.0, "fees_usdt": 0.0, "tp1_fee_usdt": 0.0,
                                    "tp1_pnl_gross_usdt": 0.0, "risk_usd": sz["effective"], "intended_risk_usd": sz["intended"],
                                    "effective_risk_usd": sz["effective"], "effective_risk_pct": sz["eff_pct"],
                                    "portfolio_risk_budget_hit": sz["budget_hit"], "position_notional": sz["qty"] * entry,
                                }
                                st.pending_age = 0
                            else:
                                budget_total = max(0.0, float(bt.max_portfolio_risk_pct) * float(equity))
                                rem_budget = max(0.0, budget_total - _open_risk_usd(states))
                                blocked_by_risk_cap = rem_budget <= 0.0
                    if blocked_by_max_positions or blocked_by_risk_cap:
                        blocked_entries += 1

            if st.pending is not None and st.pos is None:
                st.pending_age += 1
                if st.pending_age > bt.entry_timeout_bars:
                    st.pending = None
                    st.pending_age = 0
                elif float(row["high"]) >= float(st.pending["entry"]):
                    fill = _apply_slip(float(st.pending["entry"]), "LONG", True, bt.slippage_bps_entry)
                    qty = float(st.pending["qty"])
                    fee = _fee_cost(qty * fill, bt.fee_taker)
                    equity -= fee
                    st.pending["fees_usdt"] = fee
                    st.pending["entry_fill"] = fill
                    st.pending["entry_ts"] = ts
                    st.pending["entry_i"] = timeline_i
                    st.pending["stop_active"] = float(st.pending["stop"])
                    st.pos = st.pending
                    st.pending = None
                    st.pending_age = 0

            if st.pos is not None:
                pos = st.pos
                entry_fill = float(pos["entry_fill"])
                stop_lvl = float(pos["stop_active"])
                qty_total = float(pos["qty"])
                qty_rem = float(pos["qty_remaining"])
                entry_i = int(pos.get("entry_i", timeline_i))
                bars_held = int(max(0, timeline_i - entry_i))
                holding_hours = float((pd.Timestamp(ts) - pd.Timestamp(pos["entry_ts"])).total_seconds() / 3600.0)

                hit_time_stop = False
                if int(args.max_hold_bars) > 0 and bars_held >= int(args.max_hold_bars):
                    hit_time_stop = True
                elif float(args.max_hold_hours) > 0 and holding_hours >= float(args.max_hold_hours):
                    hit_time_stop = True

                if hit_time_stop:
                    base_exit_px = _tp_fill_price(float(row["close"]), "LONG", bt, tp_half_spread_bps=bt.tp_bidask_half_spread_bps)
                    exit_px = _apply_slip(base_exit_px, "LONG", False, bt.slippage_bps_stop)
                    exit_fee = _fee_cost(qty_rem * exit_px, bt.fee_taker)
                    equity -= exit_fee
                    pnl = (exit_px - entry_fill) * qty_rem
                    equity += pnl
                    total_pnl = float(pos["realized_pnl"]) + pnl
                    total_fees = float(pos["fees_usdt"]) + float(pos["tp1_fee_usdt"]) + exit_fee
                    pnl_net = total_pnl - total_fees
                    pnl_r = total_pnl / ((entry_fill - float(pos["stop"])) * qty_total) if qty_total > 0 else 0.0
                    rec = {c: None for c in TRADE_COLUMNS}
                    rec.update({
                        "symbol": sym, "side": "LONG", "signal_ts": pos["signal_ts"], "entry_ts": pos["entry_ts"], "exit_ts": ts,
                        "entry": entry_fill, "exit": exit_px, "stop": pos["stop"], "tp1": pos["tp1"], "tp2": pos["tp2"],
                        "qty": qty_total, "pnl_gross_usdt": total_pnl, "pnl_net_usdt": pnl_net, "fees_usdt": total_fees,
                        "tp1_pnl_gross_usdt": pos["tp1_pnl_gross_usdt"], "tp1_fee_usdt": pos["tp1_fee_usdt"], "pnl_R": pnl_r,
                        "exit_reason": "TIME_STOP", "took_tp1": pos["took_tp1"], "risk_usd": pos["risk_usd"],
                        "intended_risk_usd": pos["intended_risk_usd"], "effective_risk_usd": pos["effective_risk_usd"],
                        "effective_risk_pct": pos["effective_risk_pct"], "position_notional": pos["position_notional"],
                        "portfolio_risk_budget_hit": pos["portfolio_risk_budget_hit"],
                        "bars_held": bars_held, "holding_hours": holding_hours,
                    })
                    trades.append(rec)
                    st.pos = None
                    record_eq(ts, "TIME_STOP")
                    continue

                hit_stop = float(row["low"]) <= stop_lvl
                hit_tp1 = (not bool(pos["took_tp1"])) and (float(row["high"]) >= float(pos["tp1"]))
                hit_tp2 = float(row["high"]) >= float(pos["tp2"])
                if bt.conservative_intrabar and hit_stop and (hit_tp1 or hit_tp2):
                    hit_tp1 = False
                    hit_tp2 = False

                if hit_stop:
                    exit_px = _apply_slip(stop_lvl, "LONG", False, bt.slippage_bps_stop)
                    exit_fee = _fee_cost(qty_rem * exit_px, bt.fee_taker)
                    equity -= exit_fee
                    pnl = (exit_px - entry_fill) * qty_rem
                    equity += pnl
                    total_pnl = float(pos["realized_pnl"]) + pnl
                    total_fees = float(pos["fees_usdt"]) + float(pos["tp1_fee_usdt"]) + exit_fee
                    pnl_net = total_pnl - total_fees
                    pnl_r = total_pnl / ((entry_fill - float(pos["stop"])) * qty_total) if qty_total > 0 else 0.0
                    rec = {c: None for c in TRADE_COLUMNS}
                    rec.update({
                        "symbol": sym, "side": "LONG", "signal_ts": pos["signal_ts"], "entry_ts": pos["entry_ts"], "exit_ts": ts,
                        "entry": entry_fill, "exit": exit_px, "stop": pos["stop"], "tp1": pos["tp1"], "tp2": pos["tp2"],
                        "qty": qty_total, "pnl_gross_usdt": total_pnl, "pnl_net_usdt": pnl_net, "fees_usdt": total_fees,
                        "tp1_pnl_gross_usdt": pos["tp1_pnl_gross_usdt"], "tp1_fee_usdt": pos["tp1_fee_usdt"], "pnl_R": pnl_r,
                        "exit_reason": "STOP", "took_tp1": pos["took_tp1"], "risk_usd": pos["risk_usd"],
                        "intended_risk_usd": pos["intended_risk_usd"], "effective_risk_usd": pos["effective_risk_usd"],
                        "effective_risk_pct": pos["effective_risk_pct"], "position_notional": pos["position_notional"],
                        "portfolio_risk_budget_hit": pos["portfolio_risk_budget_hit"],
                        "bars_held": bars_held, "holding_hours": holding_hours,
                    })
                    trades.append(rec)
                    st.pos = None
                    stops_today += 1
                    record_eq(ts, "STOP")
                    continue

                if hit_tp1:
                    close_qty = min(qty_rem, qty_total * 0.5)
                    exit_px = _tp_fill_price(float(pos["tp1"]), "LONG", bt, tp_half_spread_bps=bt.tp_bidask_half_spread_bps)
                    tp1_fee = _fee_cost(close_qty * exit_px, bt.fee_maker)
                    equity -= tp1_fee
                    pnl = (exit_px - entry_fill) * close_qty
                    equity += pnl
                    pos["realized_pnl"] = float(pos["realized_pnl"]) + pnl
                    pos["tp1_fee_usdt"] = float(pos["tp1_fee_usdt"]) + tp1_fee
                    pos["tp1_pnl_gross_usdt"] = float(pos["tp1_pnl_gross_usdt"]) + pnl
                    pos["qty_remaining"] = qty_rem - close_qty
                    pos["stop_active"] = entry_fill
                    pos["took_tp1"] = True

                if hit_tp2 and st.pos is not None:
                    qty_rem = float(pos["qty_remaining"])
                    exit_px = _tp_fill_price(float(pos["tp2"]), "LONG", bt, tp_half_spread_bps=bt.tp_bidask_half_spread_bps)
                    exit_fee = _fee_cost(qty_rem * exit_px, bt.fee_maker)
                    equity -= exit_fee
                    pnl = (exit_px - entry_fill) * qty_rem
                    equity += pnl
                    total_pnl = float(pos["realized_pnl"]) + pnl
                    total_fees = float(pos["fees_usdt"]) + float(pos["tp1_fee_usdt"]) + exit_fee
                    pnl_net = total_pnl - total_fees
                    pnl_r = total_pnl / ((entry_fill - float(pos["stop"])) * qty_total) if qty_total > 0 else 0.0
                    rec = {c: None for c in TRADE_COLUMNS}
                    rec.update({
                        "symbol": sym, "side": "LONG", "signal_ts": pos["signal_ts"], "entry_ts": pos["entry_ts"], "exit_ts": ts,
                        "entry": entry_fill, "exit": exit_px, "stop": pos["stop"], "tp1": pos["tp1"], "tp2": pos["tp2"],
                        "qty": qty_total, "pnl_gross_usdt": total_pnl, "pnl_net_usdt": pnl_net, "fees_usdt": total_fees,
                        "tp1_pnl_gross_usdt": pos["tp1_pnl_gross_usdt"], "tp1_fee_usdt": pos["tp1_fee_usdt"], "pnl_R": pnl_r,
                        "exit_reason": "TP2", "took_tp1": pos["took_tp1"], "risk_usd": pos["risk_usd"],
                        "intended_risk_usd": pos["intended_risk_usd"], "effective_risk_usd": pos["effective_risk_usd"],
                        "effective_risk_pct": pos["effective_risk_pct"], "position_notional": pos["position_notional"],
                        "portfolio_risk_budget_hit": pos["portfolio_risk_budget_hit"],
                        "bars_held": bars_held, "holding_hours": holding_hours,
                    })
                    trades.append(rec)
                    st.pos = None
                    record_eq(ts, "TP2")

        open_positions_count = sum(1 for s in states.values() if s.pos is not None)
        bars_in_market += int(open_positions_count > 0)
        sum_open_positions_count += open_positions_count
        open_positions_hist[int(open_positions_count)] += 1

        total_notional = 0.0
        for s in states.values():
            if s.pos is None:
                continue
            qty_rem = float(s.pos.get("qty_remaining", 0.0))
            entry_fill = float(s.pos.get("entry_fill", 0.0))
            total_notional += max(0.0, qty_rem * entry_fill)
        exposure_pct = (100.0 * total_notional / equity) if equity > 0 else 0.0
        sum_exposure_pct += exposure_pct
        max_exposure_pct = max(max_exposure_pct, exposure_pct)

        rem_budget = max(0.0, float(bt.max_portfolio_risk_pct) * float(equity) - _open_risk_usd(states))
        is_cap_binded = (open_positions_count >= int(bt.max_positions)) or (rem_budget <= 0.0)
        bars_cap_binded += int(is_cap_binded)
        total_bars += 1

        record_eq(ts, "")

    trades_df = pd.DataFrame(trades)
    if not trades_df.empty:
        cols = ["symbol"] + TRADE_COLUMNS + ["bars_held"]
        for c in cols:
            if c not in trades_df.columns:
                trades_df[c] = np.nan
        trades_df = trades_df[cols]

    eq_df = pd.DataFrame(eq_curve)
    metrics = compute_metrics(trades_df, eq_df, float(bt.initial_equity_usdt), float(equity), start_ts, end_ts)

    time_in_market_pct = (100.0 * bars_in_market / total_bars) if total_bars > 0 else 0.0
    avg_open_positions = (sum_open_positions_count / total_bars) if total_bars > 0 else 0.0
    avg_exposure_pct = (sum_exposure_pct / total_bars) if total_bars > 0 else 0.0
    cap_bind_time_pct = (100.0 * bars_cap_binded / total_bars) if total_bars > 0 else 0.0
    cap_hit_rate = (sum(1 for k, v in open_positions_hist.items() if int(k) >= int(bt.max_positions) for _ in range(v)) / total_bars) if total_bars > 0 else 0.0
    cap_block_rate = (blocked_entries / eligible_entries) if eligible_entries > 0 else 0.0

    backtest_days = 0
    if start_ts is not None and end_ts is not None:
        backtest_days = int((pd.Timestamp(end_ts) - pd.Timestamp(start_ts)).days + 1)
    trades_per_day = (float(len(trades_df)) / float(backtest_days)) if backtest_days > 0 else 0.0

    cagr_pct = None
    calmar_ratio = None
    if start_ts is not None and end_ts is not None and float(bt.initial_equity_usdt) > 0:
        total_days = max(0.0, (pd.Timestamp(end_ts) - pd.Timestamp(start_ts)).total_seconds() / 86400.0)
        years = total_days / 365.25
        if years > 0:
            cagr = (float(equity) / float(bt.initial_equity_usdt)) ** (1.0 / years) - 1.0
            cagr_pct = float(cagr * 100.0)
            max_dd_pct = metrics.get("max_drawdown_pct")
            if max_dd_pct is not None and float(max_dd_pct) > 0:
                calmar_ratio = float(cagr_pct / float(max_dd_pct))

    metrics.update(
        {
            "time_in_market_pct": float(time_in_market_pct),
            "avg_open_positions": float(avg_open_positions),
            "open_positions_hist": {int(k): int(v) for k, v in sorted(open_positions_hist.items())},
            "avg_exposure_pct": float(avg_exposure_pct),
            "max_exposure_pct": float(max_exposure_pct),
            "cap_hit_rate": float(cap_hit_rate),
            "cap_block_rate": float(cap_block_rate),
            "cap_bind_time_pct": float(cap_bind_time_pct),
            "eligible_entries": int(eligible_entries),
            "blocked_entries": int(blocked_entries),
            "trades_per_day": float(trades_per_day),
            "cagr_pct": cagr_pct,
            "calmar_ratio": calmar_ratio,
            "exit_reason_counts": {str(k): int(v) for k, v in trades_df["exit_reason"].value_counts().to_dict().items()} if (not trades_df.empty and "exit_reason" in trades_df.columns) else {},
        }
    )

    per_symbol: Dict[str, Any] = {}
    for sym in symbols:
        t = trades_df[trades_df["symbol"] == sym] if not trades_df.empty else pd.DataFrame()
        gp = float(t.loc[t["pnl_net_usdt"] > 0, "pnl_net_usdt"].sum()) if not t.empty else 0.0
        gl = float(-t.loc[t["pnl_net_usdt"] < 0, "pnl_net_usdt"].sum()) if not t.empty else 0.0
        pf = (gp / gl) if gl > 0 else None
        per_symbol[sym] = {
            "trades": int(len(t)),
            "profit_factor": float(pf) if pf is not None else None,
            "avg_R": float(t["pnl_R"].mean()) if not t.empty else 0.0,
            "sum_PnL": float(t["pnl_net_usdt"].sum()) if not t.empty else 0.0,
        }
    metrics["per_symbol"] = per_symbol

    outdir = os.path.join("results", args.outdir)
    os.makedirs(outdir, exist_ok=True)
    trades_path = os.path.join(outdir, "portfolio_trades.csv")
    eq_path = os.path.join(outdir, "portfolio_equity_curve.csv")
    met_path = os.path.join(outdir, "portfolio_metrics.json")
    trades_df.to_csv(trades_path, index=False)
    eq_df.to_csv(eq_path, index=False)
    with open(met_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    print("=== PORTFOLIO SUMMARY ===")
    print(f"final_equity={metrics.get('final_equity_usdt', equity):.6f}")
    print(f"return_pct={metrics.get('return_pct', 0.0):.4f}")
    print(f"max_dd_pct={metrics.get('max_drawdown_pct', 0.0)}")
    print(f"trades_total={metrics.get('trades', 0)}")
    print(f"avg_R={metrics.get('avg_R', 0.0):.6f}")
    print(f"PF={metrics.get('profit_factor')}")
    print(f"time_in_market_pct={metrics.get('time_in_market_pct', 0.0):.4f}")
    print(f"avg_open_positions={metrics.get('avg_open_positions', 0.0):.6f}")
    print(f"avg_exposure_pct={metrics.get('avg_exposure_pct', 0.0):.4f}")
    print(f"max_exposure_pct={metrics.get('max_exposure_pct', 0.0):.4f}")
    print(f"cap_hit_rate={metrics.get('cap_hit_rate', 0.0):.4f}")
    print(f"cap_bind_time_pct={metrics.get('cap_bind_time_pct', 0.0):.4f}")
    print(f"trades_per_day={metrics.get('trades_per_day', 0.0):.6f}")
    print(f"cagr_pct={metrics.get('cagr_pct')}")
    print(f"calmar_ratio={metrics.get('calmar_ratio')}")
    print("per_symbol_trades=" + ", ".join([f"{k}:{v['trades']}" for k, v in per_symbol.items()]))
    print(f"Saved: {trades_path}, {eq_path}, {met_path}")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bt", action="store_true")
    ap.add_argument("--symbols", type=str, default=",".join(DEFAULT_SYMBOLS))
    ap.add_argument("--csvs", type=str, default="")
    ap.add_argument("--outdir", type=str, default="portfolio")
    ap.add_argument("--equity", type=float, default=1000.0)
    ap.add_argument("--risk_per_trade", type=float, default=0.03)
    ap.add_argument("--max_positions", type=int, default=2)
    ap.add_argument("--max_portfolio_risk_pct", type=float, default=0.06)
    ap.add_argument("--min_notional_usd", type=float, default=10.0)
    ap.add_argument("--enable_tp_bidask", type=int, default=1)
    ap.add_argument("--tp_bidask_half_spread_bps", type=float, default=0.5)
    ap.add_argument("--slip_bps_entry", type=float, default=1.0)
    ap.add_argument("--slip_bps_stop", type=float, default=2.0)
    ap.add_argument("--daily_dd_kill", type=int, default=0)
    ap.add_argument("--daily_dd_limit", type=float, default=0.02)
    ap.add_argument("--max_stops_kill", type=int, default=0)
    ap.add_argument("--max_stops_per_day", type=int, default=3)
    ap.add_argument("--exec_policy", type=str, default="legacy", choices=["legacy", "maker_first_fast"])
    ap.add_argument("--fee_taker", type=float, default=0.0)
    ap.add_argument("--fee_maker", type=float, default=0.0)
    ap.add_argument("--validate_5m", type=int, default=1)
    ap.add_argument("--max_hold_bars", type=int, default=0)
    ap.add_argument("--max_hold_hours", type=float, default=0.0)
    return ap


if __name__ == "__main__":
    parser = build_parser()
    ns = parser.parse_args()
    if not ns.bt:
        raise SystemExit("Use --bt")
    if ns.exec_policy != "legacy":
        raise SystemExit("portfolio_aggregator_spot supports legacy execution only")
    try:
        run_portfolio_backtest(ns)
    except ValueError as exc:
        raise SystemExit(f"[INPUT_ERROR] {exc}")
