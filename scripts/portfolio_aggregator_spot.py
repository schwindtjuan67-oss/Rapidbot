from __future__ import annotations

import argparse
import json
import os
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


@dataclass
class ExposureConcurrencyMetrics:
    max_positions: int
    cap_tolerance_abs: float = 0.001
    bars_total: int = 0
    exposure_positive_bars: int = 0
    cap_bind_bars: int = 0
    cap_hit_count: int = 0
    entry_attempts: int = 0
    exposure_sum_pct: float = 0.0
    exposure_max_pct: float = 0.0
    open_positions_sum: float = 0.0
    open_positions_max: int = 0
    open_positions_hist: Dict[int, int] = None
    symbol_open_bars: Dict[str, int] = None
    symbol_exposure_sum_pct: Dict[str, float] = None

    def __post_init__(self) -> None:
        self.open_positions_hist = {}
        self.symbol_open_bars = {}
        self.symbol_exposure_sum_pct = {}

    def note_entry_attempt(self, cap_hit: bool) -> None:
        self.entry_attempts += 1
        if cap_hit:
            self.cap_hit_count += 1

    def on_bar(
        self,
        equity: float,
        open_positions_count: int,
        exposure_risk_usd: float,
        cap_pct: float,
        symbol_exposure_risk_usd: Dict[str, float],
    ) -> None:
        self.bars_total += 1
        self.open_positions_sum += float(open_positions_count)
        self.open_positions_max = max(self.open_positions_max, int(open_positions_count))

        hist_key = int(open_positions_count)
        self.open_positions_hist[hist_key] = self.open_positions_hist.get(hist_key, 0) + 1

        exposure_pct = (float(exposure_risk_usd) / float(equity)) if equity > 0 else 0.0
        self.exposure_sum_pct += exposure_pct
        self.exposure_max_pct = max(self.exposure_max_pct, exposure_pct)
        if exposure_pct > 0:
            self.exposure_positive_bars += 1
        if cap_pct > 0 and exposure_pct >= max(0.0, cap_pct - self.cap_tolerance_abs):
            self.cap_bind_bars += 1

        for sym, sym_risk in symbol_exposure_risk_usd.items():
            if sym_risk > 0:
                self.symbol_open_bars[sym] = self.symbol_open_bars.get(sym, 0) + 1
            sym_exp_pct = (float(sym_risk) / float(equity)) if equity > 0 else 0.0
            self.symbol_exposure_sum_pct[sym] = self.symbol_exposure_sum_pct.get(sym, 0.0) + sym_exp_pct

    def finalize(self, symbols: List[str], cap_pct: float) -> Dict[str, Any]:
        bars = max(1, self.bars_total)
        max_bucket = max(int(self.max_positions), self.open_positions_max)
        histogram_pct = {
            f"pct_time_positions_{k}": 100.0 * self.open_positions_hist.get(k, 0) / bars
            for k in range(0, max_bucket + 1)
        }
        symbol_stats = {
            sym: {
                "bars_in_position": int(self.symbol_open_bars.get(sym, 0)),
                "avg_exposure_pct": 100.0 * float(self.symbol_exposure_sum_pct.get(sym, 0.0)) / bars,
            }
            for sym in symbols
        }
        return {
            "bars_total": int(self.bars_total),
            "avg_open_positions": float(self.open_positions_sum / bars),
            "max_open_positions": int(self.open_positions_max),
            "p95_open_positions": float(np.percentile(list(self.open_positions_hist_expand()), 95)) if self.bars_total > 0 else 0.0,
            **histogram_pct,
            "avg_exposure_pct": 100.0 * float(self.exposure_sum_pct / bars),
            "max_exposure_pct": 100.0 * float(self.exposure_max_pct),
            "time_in_market_pct": 100.0 * float(self.exposure_positive_bars / bars),
            "pct_time_exposure_ge_cap": 100.0 * float(self.cap_bind_bars / bars) if cap_pct > 0 else 0.0,
            "cap_hit_count": int(self.cap_hit_count),
            "entry_attempts": int(self.entry_attempts),
            "cap_hit_rate": float(self.cap_hit_count / self.entry_attempts) if self.entry_attempts > 0 else 0.0,
            "cap_bind_time_pct": 100.0 * float(self.cap_bind_bars / bars),
            "symbol_concurrency_exposure": symbol_stats,
        }

    def open_positions_hist_expand(self) -> List[int]:
        expanded: List[int] = []
        for k, cnt in self.open_positions_hist.items():
            expanded.extend([int(k)] * int(cnt))
        return expanded


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

    ts_ms = (df["ts"].astype("int64") // 1_000_000).diff().dropna()
    if not ts_ms.empty and (ts_ms != 300000).any():
        print(f"WARNING: {symbol}: detected non-5m delta in ts (expected 300000 ms)")


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


def _position_risk_usd(pos: Optional[Dict[str, Any]]) -> float:
    if pos is None:
        return 0.0
    qty = float(pos.get("qty_remaining", 0.0))
    if qty <= 0:
        return 0.0
    entry = float(pos.get("entry_fill") or pos.get("entry") or 0.0)
    stop = float(pos.get("stop_active") or pos.get("stop") or 0.0)
    return float(max(0.0, (entry - stop) * qty))


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
    ec_metrics = ExposureConcurrencyMetrics(max_positions=int(bt.max_positions))

    def record_eq(ts: pd.Timestamp, note: str = "") -> None:
        eq_curve.append({"ts": ts, "equity_usdt": equity, "note": note})

    for ts in sorted(all_ts):
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
                    open_positions = sum(1 for s in states.values() if s.pos is not None)
                    if open_positions < int(bt.max_positions):
                        entry = float(prev["entry_long"])
                        stop = float(prev["stop_long"])
                        tp1 = float(prev["tp1_long"])
                        tp2 = float(prev["tp2_long"])
                        if np.isfinite([entry, stop, tp1, tp2]).all():
                            sz = _size_qty(entry, stop, equity, bt, states)
                            ec_metrics.note_entry_attempt(cap_hit=bool(sz["budget_hit"]))
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
                    })
                    trades.append(rec)
                    st.pos = None
                    record_eq(ts, "TP2")

        symbol_exposure_risk_usd = {sym: _position_risk_usd(st.pos) for sym, st in states.items()}
        open_positions_count = sum(1 for st in states.values() if st.pos is not None)
        exposure_risk_usd = float(sum(symbol_exposure_risk_usd.values()))
        ec_metrics.on_bar(
            equity=equity,
            open_positions_count=open_positions_count,
            exposure_risk_usd=exposure_risk_usd,
            cap_pct=float(bt.max_portfolio_risk_pct),
            symbol_exposure_risk_usd=symbol_exposure_risk_usd,
        )

        record_eq(ts, "")

    trades_df = pd.DataFrame(trades)
    if not trades_df.empty:
        cols = ["symbol"] + TRADE_COLUMNS
        for c in cols:
            if c not in trades_df.columns:
                trades_df[c] = np.nan
        trades_df = trades_df[cols]

    eq_df = pd.DataFrame(eq_curve)
    metrics = compute_metrics(trades_df, eq_df, float(bt.initial_equity_usdt), float(equity), start_ts, end_ts)

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
    exposure_metrics = ec_metrics.finalize(symbols=symbols, cap_pct=float(bt.max_portfolio_risk_pct))
    metrics["portfolio_exposure_concurrency"] = exposure_metrics

    outdir = os.path.join("results", args.outdir)
    os.makedirs(outdir, exist_ok=True)
    trades_path = os.path.join(outdir, "portfolio_trades.csv")
    eq_path = os.path.join(outdir, "portfolio_equity_curve.csv")
    met_path = os.path.join(outdir, "portfolio_metrics.json")
    exp_path = os.path.join(outdir, "portfolio_exposure_metrics.json")
    trades_df.to_csv(trades_path, index=False)
    eq_df.to_csv(eq_path, index=False)
    with open(met_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(exp_path, "w", encoding="utf-8") as f:
        json.dump(exposure_metrics, f, ensure_ascii=False, indent=2)

    print("=== PORTFOLIO SUMMARY ===")
    print(f"final_equity={metrics.get('final_equity_usdt', equity):.6f}")
    print(f"return_pct={metrics.get('return_pct', 0.0):.4f}")
    print(f"max_dd_pct={metrics.get('max_drawdown_pct', 0.0)}")
    print(f"trades_total={metrics.get('trades', 0)}")
    print(f"avg_R={metrics.get('avg_R', 0.0):.6f}")
    print(f"PF={metrics.get('profit_factor')}")
    print("per_symbol_trades=" + ", ".join([f"{k}:{v['trades']}" for k, v in per_symbol.items()]))
    print("=== PORTFOLIO EXPOSURE & CONCURRENCY ===")
    print(f"time_in_market_pct={exposure_metrics.get('time_in_market_pct', 0.0):.4f}")
    print(f"avg_open_positions={exposure_metrics.get('avg_open_positions', 0.0):.6f}")
    print(f"max_open_positions={exposure_metrics.get('max_open_positions', 0)}")
    print(f"p95_open_positions={exposure_metrics.get('p95_open_positions', 0.0):.4f}")
    print(
        "open_positions_histogram="
        + ", ".join(
            [
                f"{k.replace('pct_time_positions_', 'positions_')}:{v:.4f}%"
                for k, v in exposure_metrics.items()
                if k.startswith("pct_time_positions_")
            ]
        )
    )
    print(f"avg_exposure_pct={exposure_metrics.get('avg_exposure_pct', 0.0):.6f}")
    print(f"max_exposure_pct={exposure_metrics.get('max_exposure_pct', 0.0):.6f}")
    print(f"pct_time_exposure_ge_cap={exposure_metrics.get('pct_time_exposure_ge_cap', 0.0):.4f}")
    print(f"cap_hit_rate={exposure_metrics.get('cap_hit_rate', 0.0):.6f}")
    print(f"cap_bind_time_pct={exposure_metrics.get('cap_bind_time_pct', 0.0):.4f}")
    print(f"Saved: {trades_path}, {eq_path}, {met_path}, {exp_path}")


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
