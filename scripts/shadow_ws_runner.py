"""
shadow_ws_runner.py

Robusto runner WebSocket para consumir velas cerradas y generar señales.
No envía órdenes. Prioriza supervivencia y reconexión infinita.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

# === CODEX_PATCH_BEGIN: SYS_PATH_BOOTSTRAP_REPO_ROOT (2026-02-02) ===
_file_dir = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.abspath(os.path.join(_file_dir, ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
if _file_dir not in sys.path:
    sys.path.insert(0, _file_dir)
# === CODEX_PATCH_END: SYS_PATH_BOOTSTRAP_REPO_ROOT (2026-02-02) ===

import pandas as pd
import websockets

from adapters.mexc_ws import (
    build_kline_subscribe_payload,
    is_kline_closed,
    parse_mexc_kline_message_with_reason,
    parse_mexc_trade_message,
    should_accept_closed_kline,
)
from scripts.ws_utils import CandleAggregator, MessageClassifier, SampleRecorder
# === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
# === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
from scripts.shadow_trade_sink import ShadowTradeSink
from scripts.solusdt_vwap_bot import (
    BTConfig,
    StrategyConfig,
    backtest_from_signals,
    compute_atr_pct_proxy,
    generate_signals,
    get_last_signal_snapshot,
)
# === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
# === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===
# === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
from datetime import datetime

from scripts.solusdt_vwap_bot import TRADE_COLUMNS
from telegram_notifier import TelegramNotifier
# === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===

_LOG_FILE: Optional[Any] = None


@dataclass
class RunnerConfig:
    ws_url: str
    symbol: str = "SOLUSDT"
    interval: str = "5m"
    shadow_outdir: str = "./results/shadow"
    log_path: str = "./logs/shadow_ws.log"
    stopfile: str = "./results/STOP_SHADOW.txt"
    pidfile: Optional[str] = None
    idle_timeout_sec: int = 60
    recv_timeout_sec: int = 60
    base_delay_sec: int = 1
    max_delay_sec: int = 60
    ping_every_sec: int = 20
    ping_timeout_sec: int = 10
    warmup_bars: int = 800
    enable_catchup: int = 1
    ping_sec: int = 15
    require_pong: int = 1
    sub_ticker: int = 0
    min_bars: int = 60
    console_verbosity: int = 1
    # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
    log_every_sec: int = 15
    log_first_klines: int = 3
    log_every_closed_kline: int = 50
    data_stall_sec: int = 180
    data_stall_max_attempts: int = 2
    kline_fallback_after_sec: int = 90
    unknown_samples_path: str = "./logs/ws_unknown_samples.jsonl"
    parse_fail_samples_path: str = "./logs/ws_parse_fail_samples.jsonl"
    samples_limit: int = 50
    status_path: str = "./results/shadow/status.json"
    # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    equity_start: float = 5000.0
    state_file: str = "./results/shadow/state.json"
    tg_enable: int = 0
    tg_token_env: str = "TELEGRAM_BOT_TOKEN"
    tg_chat_id_env: str = "TELEGRAM_CHAT_ID"
    # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===


def _now_ms() -> int:
    return int(time.time() * 1000)


def _interval_ms(interval: str) -> int:
    if not interval:
        return 0
    interval = interval.strip().lower()
    try:
        if interval.endswith("m"):
            return int(interval[:-1]) * 60_000
        if interval.endswith("h"):
            return int(interval[:-1]) * 60 * 60_000
        if interval.endswith("d"):
            return int(interval[:-1]) * 24 * 60 * 60_000
    except Exception:
        return 0
    return 0


def _log(msg: str) -> None:
    print(msg, flush=True)
    if _LOG_FILE is not None:
        try:
            _LOG_FILE.write(msg + "\n")
            _LOG_FILE.flush()
            os.fsync(_LOG_FILE.fileno())
        except Exception:
            pass


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _init_logging(path: str) -> None:
    global _LOG_FILE
    if _LOG_FILE is not None:
        return
    try:
        _ensure_dir(os.path.dirname(path) or ".")
        _LOG_FILE = open(path, "a", encoding="utf-8")
    except Exception:
        _LOG_FILE = None


def _close_logging() -> None:
    global _LOG_FILE
    if _LOG_FILE is None:
        return
    try:
        _LOG_FILE.close()
    except Exception:
        pass
    _LOG_FILE = None


def _write_pidfile(pidfile: str) -> None:
    try:
        with open(pidfile, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
            f.flush()
            os.fsync(f.fileno())
    except Exception:
        pass


def _cleanup_pidfile(pidfile: Optional[str]) -> None:
    if not pidfile:
        return
    try:
        if os.path.exists(pidfile):
            os.remove(pidfile)
    except Exception:
        pass


# === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
def _read_state(state_path: str) -> Dict[str, Any]:
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_state(state_path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
    tmp_path = f"{state_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, state_path)


def _utc_now_str() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _build_state_payload(
    equity_start: float,
    equity_current: float,
    last_close_ts: Optional[int],
) -> Dict[str, Any]:
    return {
        "equity_start": equity_start,
        "equity_current": equity_current,
        "last_close_ts": last_close_ts,
        "updated_utc": _utc_now_str(),
    }
# === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===


def _write_health(health_path: str, payload: Dict[str, Any]) -> None:
    tmp_path = f"{health_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, health_path)


def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _stop_requested(stopfile: str) -> bool:
    return bool(stopfile) and os.path.exists(stopfile)


def _clear_stale_stopfile(stopfile: str) -> None:
    if not stopfile or not os.path.exists(stopfile):
        return
    try:
        os.remove(stopfile)
        _log(f"[WS] WARN removed stale stopfile: {stopfile}")
    except Exception:
        _log(f"[WS] WARN could not remove stale stopfile: {stopfile}")


def _write_status(status_path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(status_path) or ".", exist_ok=True)
    tmp_path = f"{status_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, status_path)


def _build_subscribe_payload(symbol: str, interval: str) -> Dict[str, Any]:
    mexc_payload, fallback_payload = build_kline_subscribe_payload(symbol, interval)
    return {"mexc": mexc_payload, "fallback": fallback_payload}


def _kline_to_bar(kline: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ts": pd.to_datetime(int(kline["close_ts"]), unit="ms", utc=True),
        "open": float(kline["open"]),
        "high": float(kline["high"]),
        "low": float(kline["low"]),
        "close": float(kline["close"]),
        "volume": float(kline["volume"]),
        "close_ts": int(kline["close_ts"]),
    }


def _maybe_generate_signals(
    bars: List[Dict[str, Any]],
    runner_cfg: Optional[RunnerConfig],
    cfg: Optional[Any],
    signals_path: str,
) -> Dict[str, Any]:
    if generate_signals is None or cfg is None or runner_cfg is None:
        return {}
    if len(bars) < runner_cfg.min_bars:
        last_log_ts = getattr(_maybe_generate_signals, "_last_warmup_log_ts", 0)
        now_ts = _now_ms()
        if now_ts - last_log_ts >= 60_000:
            _log(f"[WARMUP] bars={len(bars)}/{runner_cfg.min_bars} (no signals yet)")
            setattr(_maybe_generate_signals, "_last_warmup_log_ts", now_ts)
        return {}
    df = pd.DataFrame(bars)
    df = df[["ts", "open", "high", "low", "close", "volume"]]
    signals = generate_signals(df, cfg)
    if signals.empty:
        return {}
    snapshot = get_last_signal_snapshot(signals)
    latest = signals.iloc[-1]
    signal = latest.get("signal", "")
    if not signal:
        return snapshot
    payload = {
        "ts": latest["ts"].isoformat(),
        "signal": signal,
        "entry_long": _safe_float(latest.get("entry_long")),
        "entry_short": _safe_float(latest.get("entry_short")),
        "stop_long": _safe_float(latest.get("stop_long")),
        "stop_short": _safe_float(latest.get("stop_short")),
        "tp1_long": _safe_float(latest.get("tp1_long")),
        "tp2_long": _safe_float(latest.get("tp2_long")),
        "tp1_short": _safe_float(latest.get("tp1_short")),
        "tp2_short": _safe_float(latest.get("tp2_short")),
    }
    _append_jsonl(signals_path, payload)
    return snapshot


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _fmt_float(value: Any, decimals: int) -> str:
    fv = _safe_float(value)
    if fv is None:
        return "NA"
    return f"{fv:.{decimals}f}"


def _pick_first(row: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def _format_bar_line(interval: str, bar: Dict[str, Any], n_bars: int) -> str:
    return (
        "[BAR] "
        f"tf={interval} "
        f"ts={bar.get('close_ts')} "
        f"o={_fmt_float(bar.get('open'), 6)} "
        f"h={_fmt_float(bar.get('high'), 6)} "
        f"l={_fmt_float(bar.get('low'), 6)} "
        f"c={_fmt_float(bar.get('close'), 6)} "
        f"v={_fmt_float(bar.get('volume'), 2)} "
        f"n_bars={n_bars}"
    )


def _format_sig_line(snapshot: Dict[str, Any], last_close_ts: Any, state: Any, verbosity: int) -> str:
    close_ts = snapshot.get("close_ts") or snapshot.get("timestamp") or last_close_ts
    signal = snapshot.get("signal") or ""
    pos = state if state not in (None, "") else "NA"
    line = (
        "[SIG] "
        f"ts={close_ts} "
        f"signal={signal} "
        f"pos={pos} "
        f"last_close_ts={last_close_ts}"
    )
    if verbosity >= 2:
        extras = []
        for key in ("close", "bias_1h", "struct_15m", "ok_vol", "ok_chop", "vol_ok", "atr_pct"):
            if key in snapshot and snapshot.get(key) is not None:
                val = snapshot.get(key)
                if key in {"close", "atr_pct"}:
                    val = _fmt_float(val, 6)
                extras.append(f"{key}={val}")
        if extras:
            line += " " + " ".join(extras)
    return line


# === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
def _format_ts(value: Any) -> Optional[str]:
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.isoformat(sep=" ")


# === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
def _format_utc(value: Any) -> str:
    if value is None:
        return "UTC -"
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return "UTC -"
    return f"UTC {ts.strftime('%Y-%m-%d %H:%M:%S')}"


def _format_symbol(symbol: str) -> str:
    if "_" in symbol:
        return symbol
    if symbol.endswith("USDT") and len(symbol) > 4:
        return f"{symbol[:-4]}_USDT"
    return symbol


def _fmt_price(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value)


def _fmt_signed(value: Any) -> str:
    try:
        return f"{float(value):+.2f}"
    except Exception:
        return str(value)
# === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===


# === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
def _fmt_qty(value: Any) -> str:
    try:
        return f"{float(value):.4f}"
    except Exception:
        return str(value)


def _normalize_ts(value: Any) -> str:
    if value is None:
        return ""
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.isoformat()


def _load_tg_cache(path: str) -> Set[str]:
    if not path or not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            items = data.get("sent_ids", [])
        else:
            items = data
        if isinstance(items, list):
            return {str(item) for item in items}
    except Exception:
        return set()
    return set()


def _save_tg_cache(path: str, sent_ids: Set[str], max_items: int = 1000) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        trimmed = list(sent_ids)[-max_items:]
        payload = {"sent_ids": trimmed}
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        return


def _trade_dedupe_id(event_type: str, symbol: str, trade: Dict[str, Any]) -> str:
    trade_id = trade.get("trade_id") or trade.get("id")
    ts_value = trade.get("entry_ts") if event_type == "OPEN" else trade.get("exit_ts")
    ts_norm = _normalize_ts(ts_value)
    if trade_id:
        return f"{symbol}:{event_type}:{trade_id}:{ts_norm or '-'}"
    side = trade.get("side")
    entry = trade.get("entry")
    exit_val = trade.get("exit")
    qty = trade.get("qty")
    raw = "|".join(
        str(part or "")
        for part in (
            symbol,
            event_type,
            side,
            entry,
            exit_val,
            qty,
            ts_norm,
        )
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"{symbol}:{event_type}:{digest}"


def _fmt_local_time(value: Any) -> str:
    if value is None:
        return "N/A"
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return "N/A"
    local_ts = ts.tz_convert(datetime.now().astimezone().tzinfo)
    return local_ts.strftime("%Y-%m-%d %H:%M:%S (%z)")


def _build_trade_message(event_type: str, symbol: str, timeframe: str, trade: Dict[str, Any], dedupe_key: str) -> str:
    side = str(trade.get("side") or "N/A").upper()
    side_emoji = "🟢" if side == "LONG" else "🔴" if side == "SHORT" else "⚪"
    entry = trade.get("entry")
    exit_val = trade.get("exit")
    qty = trade.get("qty")
    trade_id = trade.get("trade_id") or trade.get("id")
    ts_value = trade.get("entry_ts") if event_type == "OPEN" else trade.get("exit_ts")

    lines = []
    if event_type == "OPEN":
        lines.append("🚨 SEÑAL VIP — ENTRADA")
    else:
        lines.append("✅ SEÑAL VIP — CIERRE")
    lines.append(f"📌 Par: {symbol} | TF: {timeframe}")
    lines.append(f"{side_emoji} Lado: {side}")

    if event_type == "OPEN":
        lines.append(f"🎯 Entry: {_fmt_price(entry) if entry is not None else 'N/A'}")
    else:
        lines.append(
            f"📍 Entry: {_fmt_price(entry) if entry is not None else 'N/A'} → Exit: {_fmt_price(exit_val) if exit_val is not None else 'N/A'}"
        )

    if qty is not None:
        lines.append(f"🧮 Qty: {_fmt_qty(qty)}")

    sl_keys = ["sl", "stop_loss"]
    tp_keys = ["tp", "tp1", "tp2", "tp3", "take_profit"]
    for key in sl_keys:
        if trade.get(key) is not None:
            lines.append(f"🛡 SL: {_fmt_price(trade.get(key))}")
    for key in tp_keys:
        if trade.get(key) is not None:
            label = key.upper() if key.upper().startswith("TP") else "TP"
            lines.append(f"🏁 {label}: {_fmt_price(trade.get(key))}")

    if event_type == "CLOSE":
        pnl_pct = trade.get("pnl_pct")
        pnl_abs = trade.get("pnl_net_usdt")
        if pnl_pct is not None or pnl_abs is not None:
            pnl_pct_txt = f"{_fmt_signed(pnl_pct)}%" if pnl_pct is not None else "N/A"
            pnl_abs_txt = f"{_fmt_signed(pnl_abs)} USDT" if pnl_abs is not None else "N/A"
            lines.append(f"💰 PnL: {pnl_pct_txt} ( {pnl_abs_txt} )")

    eq_before = trade.get("equity_before")
    eq_after = trade.get("equity_after")
    if eq_before is not None or eq_after is not None:
        lines.append(
            f"📊 Equity: {_fmt_price(eq_before) if eq_before is not None else 'N/A'} → {_fmt_price(eq_after) if eq_after is not None else 'N/A'}"
        )

    lines.append(f"🕒 Hora: {_fmt_local_time(ts_value)}")
    if ts_value is not None:
        lines.append(f"🧾 TS raw: {ts_value}")
    lines.append(f"🔎 Ref: {trade_id if trade_id is not None else dedupe_key}")
    return "\n".join(lines)


def _notify_trade(
    notifier: TelegramNotifier,
    sent_ids: Set[str],
    cache_path: str,
    event_type: str,
    symbol: str,
    timeframe: str,
    trade: Dict[str, Any],
) -> None:
    dedupe_id = _trade_dedupe_id(event_type, symbol, trade)
    if dedupe_id in sent_ids:
        return
    msg = _build_trade_message(event_type, symbol, timeframe, trade, dedupe_id)
    preview = msg if len(msg) <= 300 else f"{msg[:300]}..."
    _log(f"[TG] OUT {dedupe_id} <len={len(msg)}> {preview}")
    response = notifier.send_vip_trade_message(msg, dedupe_key=dedupe_id)
    if response.get("ok"):
        sent_ids.add(dedupe_id)
        _save_tg_cache(cache_path, sent_ids)
    else:
        _log(
            f"[TG] send fail dedupe={dedupe_id} dry_run={response.get('dry_run')} error={response.get('error') or '-'}"
        )
# === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===


def _run_shadow_backtest(
    bars: List[Dict[str, Any]],
    strat_cfg: StrategyConfig,
) -> Optional[Dict[str, Any]]:
    if not bars:
        return None
    df = pd.DataFrame(bars)
    if df.empty:
        return None
    df = df[["ts", "open", "high", "low", "close", "volume"]]
    atr_pct_proxy = compute_atr_pct_proxy(df, n=strat_cfg.daily_atr_len) if not df.empty else pd.Series()
    if not df.empty and len(atr_pct_proxy) == len(df):
        df = df.copy()
        df["atr_pct_proxy"] = atr_pct_proxy.values

    sigdf = generate_signals(df, strat_cfg) if not df.empty else df
    bt = BTConfig(
        tp_is_maker=True,
        slippage_bps_tp=0.0,
        entry_timeout_bars=strat_cfg.entry_timeout_bars,
        enable_funding_cost=strat_cfg.enable_funding_cost,
        funding_rate_8h=strat_cfg.funding_rate_8h,
        funding_interval_hours=strat_cfg.funding_interval_hours,
        enable_tp_bidask_model=strat_cfg.enable_tp_bidask_model,
        tp_bidask_half_spread_bps=strat_cfg.tp_bidask_half_spread_bps,
    )
    # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    trades_df, eq_df, final_equity, _, _, _, state = backtest_from_signals(
        sigdf,
        bt,
        strat_cfg,
        atr_pct_proxy=atr_pct_proxy,
        return_state=True,
    )
    return {
        "trades_df": trades_df,
        "eq_df": eq_df,
        "final_equity": final_equity,
        "initial_equity": float(bt.initial_equity_usdt),
        "state": state,
    }
    # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
# === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===


async def _subscribe(ws: websockets.WebSocketClientProtocol, symbol: str, interval: str) -> None:
    payloads = _build_subscribe_payload(symbol, interval)
    await ws.send(json.dumps(payloads["mexc"]))
    await ws.send(json.dumps(payloads["fallback"]))


async def _subscribe_ticker(ws: websockets.WebSocketClientProtocol) -> None:
    payload = {"method": "sub.ticker", "param": {"symbol": "SOL_USDT"}, "gzip": False}
    await ws.send(json.dumps(payload))


async def _wait_for_subscribe_ack(
    ws: websockets.WebSocketClientProtocol,
    timeout_sec: float = 5.0,
) -> Optional[Dict[str, Any]]:
    try:
        raw_msg = await asyncio.wait_for(ws.recv(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        return None
    try:
        return json.loads(raw_msg)
    except Exception:
        return None


# === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
def _is_subscribe_ack(msg: Optional[Dict[str, Any]]) -> bool:
    if not msg or not isinstance(msg, dict):
        return False
    if msg.get("id") is None:
        return False
    return "result" in msg or msg.get("code") == 0
# === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===


async def run_shadow_runner(cfg: RunnerConfig) -> None:
    _ensure_dir(cfg.shadow_outdir)
    _ensure_dir(os.path.dirname(cfg.log_path) or ".")
    _ensure_dir(os.path.dirname(cfg.stopfile) or ".")
    if cfg.pidfile is None:
        cfg.pidfile = os.path.join(cfg.shadow_outdir, "shadow.pid")
    _ensure_dir(os.path.dirname(cfg.pidfile) or ".")
    _write_pidfile(cfg.pidfile)
    _log(
        "[WS] START "
        f"ws_url={cfg.ws_url} "
        f"symbol={cfg.symbol} "
        f"tf={cfg.interval} "
        f"out={cfg.shadow_outdir} "
        f"log={cfg.log_path} "
        f"stopfile={cfg.stopfile} "
        f"pidfile={cfg.pidfile}"
    )
    # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    state_path = cfg.state_file
    # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    health_path = os.path.join(cfg.shadow_outdir, "ws_health.json")
    signals_path = os.path.join(cfg.shadow_outdir, "shadow_signals.jsonl")
    # === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
    trade_sink = ShadowTradeSink(cfg.shadow_outdir)
    last_trade_count = trade_sink.count_trades()
    last_equity_count = trade_sink.count_equity()
    last_trade_summary = trade_sink.read_last_trade()
    trades_closed_total = last_trade_count
    last_trade_exit_ts = last_trade_summary.get("exit_ts") if last_trade_summary else None
    last_pnl_net = (
        float(last_trade_summary.get("pnl_net_usdt"))
        if last_trade_summary and last_trade_summary.get("pnl_net_usdt")
        else None
    )
    last_R = (
        float(last_trade_summary.get("pnl_R"))
        if last_trade_summary and last_trade_summary.get("pnl_R")
        else None
    )
    # === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    state_payload = _read_state(state_path)
    state_exists = os.path.exists(state_path)
    if state_exists:
        equity_start = state_payload.get("equity_start")
        equity_current = state_payload.get("equity_current")
        last_kline_close_ts = (
            int(state_payload.get("last_close_ts")) if state_payload.get("last_close_ts") else None
        )
    else:
        equity_start = float(cfg.equity_start)
        equity_current = equity_start
        last_kline_close_ts = None
        _write_state(state_path, _build_state_payload(equity_start, equity_current, last_kline_close_ts))
    equity_start_state = equity_start if state_exists else float(cfg.equity_start)
    if equity_start is None:
        equity_start = float(cfg.equity_start)
    if equity_current is None:
        equity_current = float(equity_start)
    open_position = 0
    last_entry_key = None
    tg_cache_path = os.path.join(cfg.shadow_outdir, ".tg_sent_cache.json")
    sent_trade_ids = _load_tg_cache(tg_cache_path)
    tg_env_enabled = os.getenv("TELEGRAM_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    tg_has_creds = bool(os.getenv(cfg.tg_token_env)) and bool(os.getenv(cfg.tg_chat_id_env))
    tg_enabled = (bool(cfg.tg_enable) or tg_env_enabled) and tg_has_creds
    notifier = TelegramNotifier(
        enabled=tg_enabled,
        token_env=cfg.tg_token_env,
        chat_id_env=cfg.tg_chat_id_env,
    )
    # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    bars: List[Dict[str, Any]] = []
    reconnections_count = 0
    attempt = 0
    last_msg_monotonic = time.monotonic()
    last_frame_monotonic = last_msg_monotonic
    last_ok_ts = None
    last_pong_ts = None
    last_ping_ts = None
    last_data_ts = None
    last_kline_rx_ts = None
    last_error: Optional[str] = None
    current_candle_close_ts: Optional[int] = None
    current_open_ts: Optional[int] = None
    current_kline_snapshot: Optional[Dict[str, Any]] = None
    last_emitted_open_ts: Optional[int] = None
    current_state = "INIT"
    msgs_total = 0
    klines_total = 0
    last_kline_t = None
    connected = False
    stop_requested = False
    # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
    rx_total = 0
    pong_total = 0
    ping_total = 0
    kline_total = 0
    kline_closed_total = 0
    kline_skipped_intrabar = 0
    kline_deduped = 0
    parse_fail_total = 0
    unknown_total = 0
    trade_total = 0
    last_rx_ts = None
    last_ping_log_ts = None
    last_pong_log_ts = None
    # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
    classifier = MessageClassifier()
    unknown_samples = SampleRecorder(cfg.unknown_samples_path, cfg.samples_limit)
    parse_fail_samples = SampleRecorder(cfg.parse_fail_samples_path, cfg.samples_limit)
    data_stall_attempts = 0
    data_stall_triggered = False
    fallback_enabled = False
    trade_aggregator = CandleAggregator(cfg.symbol, cfg.interval, _interval_ms(cfg.interval))

    strategy_cfg = StrategyConfig(symbol=cfg.symbol)

    while True:
        if _stop_requested(cfg.stopfile):
            _log("[WS] STOP_SHADOW detected, exiting")
            stop_requested = True
            break

        delay = min(cfg.max_delay_sec, cfg.base_delay_sec * (2 ** attempt))
        jitter = random.uniform(0, 0.5 * delay)
        wait_sec = delay + jitter if attempt > 0 else 0
        if wait_sec > 0:
            _log(f"[WS] RECONNECT in {wait_sec:.2f}s")
            await asyncio.sleep(wait_sec)

        current_state = "CONNECT"
        _write_health(
            health_path,
            {
                "connected": connected,
                "last_msg_ts": last_ok_ts,
                "last_pong_ts": last_pong_ts,
                "last_ping_ts": last_ping_ts,
                "msgs_total": msgs_total,
                "klines_total": klines_total,
                "last_kline_t": last_kline_t,
                "last_error": last_error,
                "current_state": current_state,
                # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                "rx_total": rx_total,
                "pong_total": pong_total,
                "ping_total": ping_total,
                "kline_total": kline_total,
                "kline_closed_total": kline_closed_total,
                "kline_skipped_intrabar": kline_skipped_intrabar,
                "kline_deduped": kline_deduped,
                "parse_fail_total": parse_fail_total,
                "unknown_total": unknown_total,
                "trade_total": trade_total,
                "last_rx_ts": last_rx_ts,
                "last_pong_age_ms": _now_ms() - last_pong_ts if last_pong_ts is not None else None,
                "last_kline_close_ts": last_kline_close_ts,
                # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                # === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
                "trades_closed_total": trades_closed_total,
                "last_trade_exit_ts": last_trade_exit_ts,
                "last_pnl_net": last_pnl_net,
                "last_R": last_R,
                # === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===
                # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                "equity_start": equity_start,
                "equity_current": equity_current,
                "open_position": open_position,
                # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
            },
        )

        ws = None
        ping_task = None
        health_task = None
        # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
        heartbeat_task = None
        # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
        stable_reset = False
        connected_since = None
        # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
        first_kline_remaining = cfg.log_first_klines
        # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
        try:
            _log("[WS] CONNECT")
            ws = await websockets.connect(
                cfg.ws_url,
                ping_interval=None,
                ping_timeout=None,
                max_queue=None,
            )
            connected = True
            connected_since = time.monotonic()
            last_pong_ts = _now_ms()
            data_stall_triggered = False
            last_data_ts = None
            await _subscribe(ws, cfg.symbol, cfg.interval)
            # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
            _log("[WS] SUB_SENT")
            # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
            if cfg.sub_ticker:
                await _subscribe_ticker(ws)
                _log("[WS] SUBSCRIBED TICKER")
            preload_msgs: List[Dict[str, Any]] = []
            ack_msg = await _wait_for_subscribe_ack(ws)
            if ack_msg is not None:
                preload_msgs.append(ack_msg)
                # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                if _is_subscribe_ack(ack_msg):
                    _log("[WS] SUB_ACK")
                # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
            current_state = "SUBSCRIBED"

            async def _ping_loop() -> None:
                nonlocal ping_total, last_ping_ts, last_ping_log_ts
                while True:
                    await ws.send(json.dumps({"method": "ping"}))
                    now_ms = _now_ms()
                    ping_total += 1
                    last_ping_ts = now_ms
                    if last_ping_log_ts is None or (now_ms - last_ping_log_ts) >= 60_000:
                        _log(f"[WS] PING_SENT n={ping_total}")
                        last_ping_log_ts = now_ms
                    await asyncio.sleep(cfg.ping_sec)

            async def _health_loop() -> None:
                while True:
                    await asyncio.sleep(5)
                    _write_health(
                        health_path,
                        {
                            "connected": connected,
                            "last_msg_ts": last_ok_ts,
                            "last_pong_ts": last_pong_ts,
                            "last_ping_ts": last_ping_ts,
                            "msgs_total": msgs_total,
                            "klines_total": klines_total,
                            "last_kline_t": last_kline_t,
                            # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                            "rx_total": rx_total,
                            "pong_total": pong_total,
                            "ping_total": ping_total,
                            "kline_total": kline_total,
                            "kline_closed_total": kline_closed_total,
                            "kline_skipped_intrabar": kline_skipped_intrabar,
                            "kline_deduped": kline_deduped,
                            "parse_fail_total": parse_fail_total,
                            "unknown_total": unknown_total,
                            "trade_total": trade_total,
                            "last_rx_ts": last_rx_ts,
                            "last_pong_age_ms": _now_ms() - last_pong_ts if last_pong_ts is not None else None,
                            "last_kline_close_ts": last_kline_close_ts,
                            # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                            # === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
                            "trades_closed_total": trades_closed_total,
                            "last_trade_exit_ts": last_trade_exit_ts,
                            "last_pnl_net": last_pnl_net,
                            "last_R": last_R,
                            # === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===
                            # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                            "equity_start": equity_start,
                            "equity_current": equity_current,
                            "open_position": open_position,
                            # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                        },
                    )

            # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
            async def _heartbeat_loop() -> None:
                while True:
                    await asyncio.sleep(cfg.log_every_sec)
                    up_sec = 0.0
                    if connected_since is not None:
                        up_sec = max(0.0, time.monotonic() - connected_since)
                    if current_candle_close_ts is None:
                        next_close_info = "None"
                    else:
                        next_close_info = f"{(current_candle_close_ts - _now_ms()) / 1000:.1f}"
                    _log(
                        "[WS] HEALTH "
                        f"up={up_sec:.1f}s "
                        f"rx={rx_total} "
                        f"ping={ping_total} "
                        f"pong={pong_total} "
                        f"last_pong_age_ms={_now_ms() - last_pong_ts if last_pong_ts is not None else None} "
                        f"kline={kline_total} "
                        f"closed={kline_closed_total} "
                        f"intrabar_skip={kline_skipped_intrabar} "
                        f"dedupe={kline_deduped} "
                        f"parse_fail={parse_fail_total} "
                        f"unknown={unknown_total} "
                        f"last_close_ts={last_kline_close_ts} "
                        f"bars_count={len(bars)} "
                        f"next_close_in_sec={next_close_info} "
                        # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                        f"eq={equity_current}/{equity_start} "
                        f"open_pos={open_position} "
                        f"trades_closed={trades_closed_total} "
                        f"last_pnl_net={last_pnl_net} "
                        f"last_R={last_R}"
                        # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                    )
            # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===

            ping_task = asyncio.create_task(_ping_loop())
            health_task = asyncio.create_task(_health_loop())
            # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
            heartbeat_task = asyncio.create_task(_heartbeat_loop())
            # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===

            while True:
                if _stop_requested(cfg.stopfile):
                    _log("[WS] STOP_SHADOW detected, closing")
                    stop_requested = True
                    break
                if ping_task and ping_task.done():
                    ping_exc = ping_task.exception()
                    if ping_exc:
                        raise RuntimeError(f"Ping loop failed: {ping_exc}") from ping_exc
                    raise RuntimeError("Ping loop stopped")

                preload_used = False
                now_monotonic = time.monotonic()
                if now_monotonic - last_frame_monotonic > cfg.idle_timeout_sec:
                    _log("[WS] IDLE_TIMEOUT")
                    raise asyncio.TimeoutError("Idle timeout")

                if cfg.require_pong and last_pong_ts is not None:
                    pong_age_ms = _now_ms() - last_pong_ts
                    rx_age_ms = _now_ms() - last_rx_ts if last_rx_ts is not None else None
                    if pong_age_ms > 65_000 and (rx_age_ms is None or rx_age_ms > 65_000):
                        _log("[WS] PONG_TIMEOUT")
                        raise asyncio.TimeoutError("Pong timeout")

                if (
                    not data_stall_triggered
                    and (
                        (
                            (last_kline_rx_ts is not None or last_data_ts is not None)
                            and (_now_ms() - (last_kline_rx_ts or last_data_ts)) > cfg.data_stall_sec * 1000
                        )
                        or (
                            last_kline_rx_ts is None
                            and last_data_ts is None
                            and rx_total > 0
                            and connected_since is not None
                            and (time.monotonic() - connected_since) > cfg.data_stall_sec
                        )
                    )
                ):
                    data_stall_triggered = True
                    payload = {
                        "reason": "data_stall",
                        "now_ms": _now_ms(),
                        "last_data_ts": last_data_ts,
                        "last_kline_rx_ts": last_kline_rx_ts,
                        "last_rx_ts": last_rx_ts,
                        "rx_total": rx_total,
                        "kline_total": kline_total,
                        "parse_fail_total": parse_fail_total,
                    }
                    _write_status(cfg.status_path, payload)
                    _log(
                        "[WS] DATA_STALL parsing_broken=1 "
                        f"now_ms={payload['now_ms']} "
                        f"last_data_ts={last_data_ts} "
                        f"last_kline_rx_ts={last_kline_rx_ts} "
                        f"last_rx_ts={last_rx_ts} "
                        f"rx={rx_total} "
                        f"kline_total={kline_total} "
                        f"parse_fail={parse_fail_total}"
                    )
                    data_stall_attempts += 1
                    if data_stall_attempts <= cfg.data_stall_max_attempts:
                        raise asyncio.TimeoutError("Data stall")
                    stop_requested = True
                    break

                try:
                    if preload_msgs:
                        msg = preload_msgs.pop(0)
                        preload_used = True
                    else:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=cfg.recv_timeout_sec)
                        last_msg_monotonic = time.monotonic()
                        last_frame_monotonic = last_msg_monotonic
                        last_ok_ts = _now_ms()
                        # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                        last_rx_ts = last_ok_ts
                        rx_total += 1
                        # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                        msgs_total += 1
                        try:
                            msg = json.loads(raw_msg)
                        except Exception:
                            msg = None
                            # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                            unknown_total += 1
                            unknown_samples.record("unknown", "json_decode", {"raw": str(raw_msg)[:2048]})
                            # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                        preload_used = False
                except asyncio.TimeoutError:
                    if time.monotonic() - last_msg_monotonic > cfg.idle_timeout_sec:
                        _log("[WS] IDLE_TIMEOUT")
                        raise
                    continue
                except Exception:
                    continue

                if preload_used:
                    last_msg_monotonic = time.monotonic()
                    last_frame_monotonic = last_msg_monotonic
                    last_ok_ts = _now_ms()
                    # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    last_rx_ts = last_ok_ts
                    rx_total += 1
                    # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    msgs_total += 1
                current_state = "MSG"
                category, reason = classifier.classify(msg)
                if category == "pong":
                    last_pong_ts = _now_ms()
                    # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    pong_total += 1
                    if last_pong_log_ts is None or (_now_ms() - last_pong_log_ts) >= 60_000:
                        _log(f"[WS] PONG_RX n={pong_total}")
                        last_pong_log_ts = _now_ms()
                    # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    continue
                if msg is None:
                    continue
                if category in {"subscribe_ack", "system"}:
                    continue

                if category == "trade":
                    trade_event = parse_mexc_trade_message(msg)
                    if not trade_event:
                        parse_fail_total += 1
                        parse_fail_samples.record("trade", "parse_fail", msg)
                        continue
                    trade_total += 1
                    last_data_ts = trade_event.get("ts") or _now_ms()
                    trade_sink.append_raw_event({"event_type": "trade", "payload": trade_event})
                    trade_sink.append_trade_event(trade_event)
                    if (
                        kline_closed_total == 0
                        and not fallback_enabled
                        and connected_since is not None
                        and (time.monotonic() - connected_since) >= cfg.kline_fallback_after_sec
                    ):
                        fallback_enabled = True
                        _log("[WS] FALLBACK_AGG enabled (trade-based candles)")
                    if fallback_enabled and trade_aggregator.interval_ms > 0:
                        agg_candle = trade_aggregator.update_trade(trade_event)
                        if agg_candle:
                            kline = agg_candle
                        else:
                            continue
                    else:
                        continue
                elif category == "kline":
                    kline, reason = parse_mexc_kline_message_with_reason(
                        msg, expected_symbol=cfg.symbol, expected_interval=cfg.interval
                    )
                    if not kline:
                        parse_fail_total += 1
                        parse_fail_samples.record("kline", reason or "parse_fail", msg)
                        continue
                    last_kline_rx_ts = _now_ms()
                    last_data_ts = last_kline_rx_ts
                    fallback_enabled = False
                else:
                    unknown_total += 1
                    unknown_samples.record(category, reason, msg)
                    continue

                # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                rollover_kline = None
                open_ts_value = kline.get("open_ts")
                open_ts_int = int(open_ts_value) if open_ts_value is not None else None
                if open_ts_int is not None:
                    if current_open_ts is None:
                        current_open_ts = open_ts_int
                        current_kline_snapshot = dict(kline)
                    elif open_ts_int != current_open_ts:
                        rollover_kline = current_kline_snapshot
                        current_open_ts = open_ts_int
                        current_kline_snapshot = dict(kline)
                    else:
                        current_kline_snapshot = dict(kline)
                kline_total += 1
                close_ts_value = kline.get("close_ts")
                current_candle_close_ts = int(close_ts_value) if close_ts_value is not None else None
                kline_is_closed = is_kline_closed(kline, _now_ms())
                if first_kline_remaining > 0:
                    _log(
                        "[WS] KLINE "
                        f"raw_schema={kline.get('raw_schema')} "
                        f"open_ts={kline.get('open_ts')} "
                        f"close_ts={kline.get('close_ts')} "
                        f"is_closed={kline_is_closed} "
                        f"ohlc={kline.get('open')}/{kline.get('high')}/{kline.get('low')}/{kline.get('close')}"
                    )
                    first_kline_remaining -= 1
                pending_klines = []
                if rollover_kline is not None:
                    pending_klines.append((rollover_kline, True))
                pending_klines.append((kline, False))
                # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===

                for emit_kline, force_closed in pending_klines:
                    emit_open_ts = emit_kline.get("open_ts")
                    emit_open_ts_int = int(emit_open_ts) if emit_open_ts is not None else None
                    emit_is_closed = True if force_closed else is_kline_closed(emit_kline, _now_ms())
                    if not emit_is_closed:
                        kline_skipped_intrabar += 1
                        continue
                    if emit_open_ts_int is not None and emit_open_ts_int == last_emitted_open_ts:
                        kline_deduped += 1
                        continue
                    if not should_accept_closed_kline(emit_kline, last_kline_close_ts, _now_ms()):
                        kline_deduped += 1
                        continue

                    trade_sink.append_raw_event({"event_type": "kline", "payload": emit_kline})
                    trade_sink.append_candle(emit_kline)

                    bar = _kline_to_bar(emit_kline)

                    bars.append(bar)
                    if cfg.console_verbosity >= 1:
                        _log(_format_bar_line(cfg.interval, bar, len(bars)))
                    last_kline_close_ts = bar["close_ts"]
                    last_kline_t = last_kline_close_ts
                    last_data_ts = last_kline_rx_ts or last_kline_close_ts
                    klines_total += 1
                    last_emitted_open_ts = emit_open_ts_int
                    # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    kline_closed_total += 1
                    if cfg.log_every_closed_kline > 0 and kline_closed_total % cfg.log_every_closed_kline == 0:
                        _log(
                            "[WS] KLINE_CLOSED "
                            f"accepted count={kline_closed_total} "
                            f"close_ts={bar['close_ts']}"
                        )
                    # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                    _write_state(
                        state_path,
                        _build_state_payload(equity_start_state, equity_current, last_kline_close_ts),
                    )
                    # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                    if cfg.console_verbosity >= 2:
                        _log(f"[WS] MSG {bar['close_ts']}")

                    signal_snapshot = _maybe_generate_signals(bars, cfg, strategy_cfg, signals_path)
                    if cfg.console_verbosity >= 1:
                        _log(_format_sig_line(signal_snapshot, bar.get("close_ts"), open_position, cfg.console_verbosity))
                # === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
                if len(bars) >= max(60, cfg.warmup_bars):
                    shadow_result = _run_shadow_backtest(bars, strategy_cfg)
                    if shadow_result is not None:
                        trades_df = shadow_result["trades_df"]
                        eq_df = shadow_result["eq_df"]
                        metrics_dirty = False
                        # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                        state = shadow_result.get("state") or {}
                        open_position = int(state.get("open_position") or 0)
                        open_position_info = state.get("open_position_info")
                        if open_position_info:
                            entry_ts = open_position_info.get("entry_ts")
                            entry_key = f"{entry_ts}|{open_position_info.get('side')}"
                            if entry_key != last_entry_key:
                                symbol = _format_symbol(cfg.symbol)
                                trade_payload = {
                                    "side": open_position_info.get("side"),
                                    "entry": open_position_info.get("entry"),
                                    "entry_ts": entry_ts,
                                    "qty": open_position_info.get("qty"),
                                    "sl": open_position_info.get("sl"),
                                    "tp": open_position_info.get("tp"),
                                    "tp1": open_position_info.get("tp1"),
                                    "tp2": open_position_info.get("tp2"),
                                    "trade_id": open_position_info.get("trade_id"),
                                    "equity_before": open_position_info.get("equity_before"),
                                    "equity_after": open_position_info.get("equity_after"),
                                }
                                _notify_trade(
                                    notifier,
                                    sent_trade_ids,
                                    tg_cache_path,
                                    "OPEN",
                                    symbol,
                                    cfg.interval,
                                    trade_payload,
                                )
                                last_entry_key = entry_key
                        else:
                            last_entry_key = None
                        # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                        # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                        if trades_df is not None and len(trades_df) > last_trade_count:
                            new_trades = trades_df.iloc[last_trade_count:]
                            for _, trade_row in new_trades.iterrows():
                                trade_dict = {col: trade_row.get(col) for col in TRADE_COLUMNS}
                                trade_sink.append_trade(trade_dict)
                                trades_closed_total += 1
                                last_trade_exit_ts = _format_ts(trade_dict.get("exit_ts"))
                                last_pnl_net = _safe_float(trade_dict.get("pnl_net_usdt"))
                                last_R = _safe_float(trade_dict.get("pnl_R"))
                                entry_ts = _pick_first(trade_dict, ["entry_ts", "ts_entry", "open_ts"])
                                exit_ts = _pick_first(trade_dict, ["exit_ts", "ts_exit", "close_ts"])
                                _log(
                                    "[TRADE] "
                                    f"side={trade_dict.get('side')} "
                                    f"entry={_fmt_float(trade_dict.get('entry'), 6)} "
                                    f"exit={_fmt_float(trade_dict.get('exit'), 6)} "
                                    f"pnl={_fmt_float(trade_dict.get('pnl_net_usdt'), 2)} "
                                    f"R={_fmt_float(trade_dict.get('pnl_R'), 3)} "
                                    f"entry_ts={entry_ts} "
                                    f"exit_ts={exit_ts} "
                                    f"reason={trade_dict.get('exit_reason')}"
                                )
                                symbol = _format_symbol(cfg.symbol)
                                _notify_trade(
                                    notifier,
                                    sent_trade_ids,
                                    tg_cache_path,
                                    "CLOSE",
                                    symbol,
                                    cfg.interval,
                                    trade_dict,
                                )
                                metrics_dirty = True
                            last_trade_count = len(trades_df)

                        if eq_df is not None and len(eq_df) > last_equity_count:
                            new_eq = eq_df.iloc[last_equity_count:]
                            for _, eq_row in new_eq.iterrows():
                                note = eq_row.get("note")
                                trade_sink.append_equity(eq_row.get("ts"), eq_row.get("equity_usdt"), note)
                                if note in {"TP1", "TP2", "STOP"}:
                                    eq_before = equity_current
                                    eq_val = _safe_float(eq_row.get("equity_usdt"))
                                    if eq_val is not None:
                                        equity_current = float(eq_val)
                                        _write_state(
                                            state_path,
                                            _build_state_payload(
                                                equity_start_state, equity_current, last_kline_close_ts
                                            ),
                                        )
                                dd_val = _pick_first(eq_row, ["dd", "drawdown", "drawdown_pct", "dd_pct"])
                                eq_ts = _pick_first(eq_row, ["ts", "timestamp", "close_ts"])
                                _log(
                                    "[EQUITY] "
                                    f"ts={eq_ts} "
                                    f"equity={_fmt_float(eq_row.get('equity_usdt'), 2)} "
                                    f"dd={_fmt_float(dd_val, 4)}"
                                )
                                metrics_dirty = True
                            last_equity_count = len(eq_df)
                        # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                        if metrics_dirty:
                            trade_sink.update_metrics(trade_sink.trades_path, trade_sink.equity_path)
                # === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===

                if connected_since and time.monotonic() - connected_since > 60:
                    attempt = 0
                    stable_reset = True

        except asyncio.CancelledError:
            _log("[WS] CANCELLED")
            raise
        except Exception as exc:
            last_error = str(exc)
            _log(f"[WS] ERROR {exc}")
        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass
            connected = False
            if ping_task:
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task
            if health_task:
                health_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await health_task
            # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
            if heartbeat_task:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
            # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
            reconnections_count += 1
            if not stable_reset:
                attempt += 1
            current_state = "RECONNECT"
            _write_health(
                health_path,
                {
                    "connected": connected,
                    "last_msg_ts": last_ok_ts,
                    "last_pong_ts": last_pong_ts,
                    "msgs_total": msgs_total,
                    "klines_total": klines_total,
                    "last_kline_t": last_kline_t,
                    "last_error": last_error,
                    "current_state": current_state,
                    # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    "rx_total": rx_total,
                    "pong_total": pong_total,
                    "kline_total": kline_total,
                    "kline_closed_total": kline_closed_total,
                    "kline_skipped_intrabar": kline_skipped_intrabar,
                    "kline_deduped": kline_deduped,
                    "parse_fail_total": parse_fail_total,
                    "unknown_total": unknown_total,
                    "trade_total": trade_total,
                    "last_rx_ts": last_rx_ts,
                    "last_kline_close_ts": last_kline_close_ts,
                    # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
                    # === CODEX_PATCH_BEGIN: SHADOW_TRADES_LOGGING (2026-02-02) ===
                    "trades_closed_total": trades_closed_total,
                    "last_trade_exit_ts": last_trade_exit_ts,
                    "last_pnl_net": last_pnl_net,
                    "last_R": last_R,
                    # === CODEX_PATCH_END: SHADOW_TRADES_LOGGING (2026-02-02) ===
                    # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                    "equity_start": equity_start,
                    "equity_current": equity_current,
                    "open_position": open_position,
                    # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
                },
            )
        if stop_requested:
            break
    _cleanup_pidfile(cfg.pidfile)


def parse_args() -> RunnerConfig:
    parser = argparse.ArgumentParser(description="Shadow WS runner (no trading)")
    parser.add_argument("--ws_url", default=_env("WS_URL", "wss://contract.mexc.com/edge"))
    parser.add_argument("--symbol", default=_env("SYMBOL", "SOLUSDT"))
    parser.add_argument(
        "--tf",
        "--interval",
        dest="interval",
        default=_env("TF", _env("INTERVAL", "5m")),
    )
    parser.add_argument(
        "--out",
        "--outdir",
        "--shadow_outdir",
        dest="shadow_outdir",
        default=_env("OUT", _env("OUTDIR", "./results/shadow")),
    )
    parser.add_argument("--log", dest="log_path", default=_env("LOG", "./logs/shadow_ws.log"))
    parser.add_argument("--stopfile", default=_env("STOPFILE", "./results/STOP_SHADOW.txt"))
    parser.add_argument("--pidfile", default=_env("PIDFILE", ""))
    parser.add_argument("--idle_timeout_sec", type=int, default=int(_env("IDLE_TIMEOUT", "60")))
    parser.add_argument("--recv_timeout_sec", type=int, default=int(_env("RECV_TIMEOUT", "60")))
    parser.add_argument("--base_delay_sec", type=int, default=int(_env("BASE_DELAY", "1")))
    parser.add_argument("--max_delay_sec", type=int, default=int(_env("MAX_DELAY", "60")))
    parser.add_argument("--ping_sec", type=int, default=int(_env("PING_SEC", "15")))
    parser.add_argument("--require_pong", type=int, default=1)
    parser.add_argument("--sub_ticker", type=int, default=0)
    parser.add_argument("--min_bars", type=int, default=int(_env("SHADOW_MIN_BARS", _env("MIN_BARS", "60"))))
    parser.add_argument("--console_verbosity", type=int, default=int(_env("SHADOW_CONSOLE_VERBOSITY", "1")))
    # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
    parser.add_argument("--log_every_sec", type=int, default=15)
    parser.add_argument("--log_first_klines", type=int, default=3)
    parser.add_argument("--log_every_closed_kline", type=int, default=50)
    parser.add_argument("--data_stall_sec", type=int, default=int(_env("DATA_STALL_SEC", "180")))
    parser.add_argument("--data_stall_max_attempts", type=int, default=int(_env("DATA_STALL_MAX", "2")))
    parser.add_argument("--kline_fallback_after_sec", type=int, default=int(_env("KLINE_FALLBACK_SEC", "90")))
    parser.add_argument(
        "--unknown_samples_path",
        default=_env("WS_UNKNOWN_SAMPLES", "./logs/ws_unknown_samples.jsonl"),
    )
    parser.add_argument(
        "--parse_fail_samples_path",
        default=_env("WS_PARSE_FAIL_SAMPLES", "./logs/ws_parse_fail_samples.jsonl"),
    )
    parser.add_argument("--samples_limit", type=int, default=int(_env("WS_SAMPLES_LIMIT", "50")))
    parser.add_argument("--status_path", default=_env("STATUS_PATH", "./results/shadow/status.json"))
    # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: SHADOW_ARGS_WARMUP_CATCHUP (2026-02-02) ===
    parser.add_argument("--warmup_bars", type=int, default=int(_env("WARMUP_BARS", "800")))
    parser.add_argument("--enable_catchup", type=int, default=int(_env("ENABLE_CATCHUP", "1")))
    # === CODEX_PATCH_END: SHADOW_ARGS_WARMUP_CATCHUP (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    parser.add_argument("--equity_start", type=float, default=float(_env("EQUITY_START", "5000.0")))
    parser.add_argument("--state_file", default=_env("STATE_FILE", "./results/shadow/state.json"))
    parser.add_argument("--tg_enable", type=int, default=int(_env("TG_ENABLE", "0")))
    parser.add_argument("--tg_token_env", default=_env("TG_TOKEN_ENV", "TELEGRAM_BOT_TOKEN"))
    parser.add_argument("--tg_chat_id_env", default=_env("TG_CHAT_ID_ENV", "TELEGRAM_CHAT_ID"))
    # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    args = parser.parse_args()
    log_path = args.log_path
    pidfile = args.pidfile or None
    _init_logging(log_path)
    _clear_stale_stopfile(args.stopfile)
    return RunnerConfig(
        ws_url=args.ws_url,
        symbol=args.symbol,
        interval=args.interval,
        shadow_outdir=args.shadow_outdir,
        log_path=log_path,
        stopfile=args.stopfile,
        pidfile=pidfile,
        idle_timeout_sec=args.idle_timeout_sec,
        recv_timeout_sec=args.recv_timeout_sec,
        base_delay_sec=args.base_delay_sec,
        max_delay_sec=args.max_delay_sec,
        ping_sec=args.ping_sec,
        require_pong=args.require_pong,
        sub_ticker=args.sub_ticker,
        min_bars=args.min_bars,
        console_verbosity=max(0, min(2, int(args.console_verbosity))),
        # === CODEX_PATCH_BEGIN: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
        log_every_sec=args.log_every_sec,
        log_first_klines=args.log_first_klines,
        log_every_closed_kline=args.log_every_closed_kline,
        data_stall_sec=args.data_stall_sec,
        data_stall_max_attempts=args.data_stall_max_attempts,
        kline_fallback_after_sec=args.kline_fallback_after_sec,
        unknown_samples_path=args.unknown_samples_path,
        parse_fail_samples_path=args.parse_fail_samples_path,
        samples_limit=args.samples_limit,
        status_path=args.status_path,
        # === CODEX_PATCH_END: WS_CONSOLE_OBSERVABILITY (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: SHADOW_ARGS_WARMUP_CATCHUP (2026-02-02) ===
        warmup_bars=args.warmup_bars,
        enable_catchup=args.enable_catchup,
        # === CODEX_PATCH_END: SHADOW_ARGS_WARMUP_CATCHUP (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
        equity_start=args.equity_start,
        state_file=args.state_file,
        tg_enable=args.tg_enable,
        tg_token_env=args.tg_token_env,
        tg_chat_id_env=args.tg_chat_id_env,
        # === CODEX_PATCH_END: SHADOW_EQUITY_TRADES_TG (2026-02-02) ===
    )


def main() -> None:
    cfg = parse_args()
    try:
        asyncio.run(run_shadow_runner(cfg))
    except KeyboardInterrupt:
        _log("[WS] KeyboardInterrupt")
    finally:
        _close_logging()


if __name__ == "__main__":
    main()
