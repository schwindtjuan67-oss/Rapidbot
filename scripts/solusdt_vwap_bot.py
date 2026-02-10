"""
solusdt_vwap_bot.py

✅ Plug-and-play: estrategia + order router + order manager (state machine) en UN SOLO ARCHIVO.
Objetivo: SOLUSDT futures, entrada 5m, contexto 1H, estructura 15m, VWAP Session + EMA20 + ATR.

Incluye:
1) Estrategia algorítmica (generate_signals) -> produce señal LONG/SHORT + niveles entry/stop/tp1/tp2
2) OrderRouter (ccxt Binance futures) -> primitives create/cancel + sizing por riesgo
3) OrderManager (máquina de estados) -> coloca entrada, espera fill, coloca SL/TP, TP1->BE, finaliza y limpia
4) Runner:
   - Modo backtest CSV (para ver señales / probar integración)
   - Modo live skeleton (loop) listo para conectar tu feed 5m

Dependencias:
  pip install pandas numpy ccxt

⚠️ Importante:
- No metas API keys en el repo. Usá variables de entorno:
    BINANCE_KEY, BINANCE_SECRET
- Símbolo en ccxt futures suele ser "SOL/USDT:USDT" (por defecto).
- Esto NO es HFT: ejecuta por cierres de vela y polling de estado.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, Literal, List, Tuple
import os
import time
import math
import uuid
import json
import tempfile
import urllib.request

import numpy as np
import pandas as pd

from scripts.time_utils import ensure_utc


# =============================================================================
# 0) TIPOS Y UTILIDADES
# =============================================================================

Side = Literal["LONG", "SHORT"]
OrderType = Literal["MARKET", "LIMIT", "STOP_MARKET"]
MgrStatus = Literal["IDLE", "ENTRY_PLACED", "IN_POSITION", "EXITING", "DONE", "CANCELLED", "ERROR"]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _log_line(path: str, obj: Dict[str, Any]) -> None:
    _ensure_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _round_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step


def _round_price(x: float, tick: float) -> float:
    if tick <= 0:
        return x
    return round(math.floor(x / tick) * tick, 10)


def _mk_client_id(prefix: str, tag: str) -> str:
    base = tag if tag else uuid.uuid4().hex[:10]
    return f"{prefix}_{base}"[:32]


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


PRESET_PARAMS: Dict[str, Dict[str, Any]] = {
    "max_throughput": {
        "cooldown_sec": 0,
        "max_trades_day": 5000,
        "rr_min": 0.0,
        "use_time_filter": 0,
        "delta_threshold": 0,
        "delta_rolling_sec": 1,
    },
    "strict": {
        "cooldown_sec": 120,
        "max_trades_day": 10,
        "rr_min": 2.0,
        "use_time_filter": 1,
        "hour_start": 4,
        "hour_end": 20,
        "delta_threshold": 100,
        "delta_rolling_sec": 60,
    },
}


def apply_overrides(obj: Any, params: Dict[str, Any]) -> None:
    for key, value in params.items():
        if hasattr(obj, key):
            setattr(obj, key, value)
            print(f"[PARAM-APPLY] {type(obj).__name__}.{key}={value} (snake_case)")
            continue
        upper_key = key.upper()
        if hasattr(obj, upper_key):
            setattr(obj, upper_key, value)
            print(f"[PARAM-APPLY] {type(obj).__name__}.{upper_key}={value} (UPPERCASE)")
            continue
        print(f"[PARAM-APPLY] skipped {type(obj).__name__}.{key}={value} (attr_missing)")


def resolve_effective_params(args: Any) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if args.preset:
        params.update(PRESET_PARAMS[args.preset])

    explicit_overrides = {
        "cooldown_sec": args.cooldown_sec,
        "max_trades_day": args.max_trades_day,
        "rr_min": args.rr_min,
        "use_time_filter": args.use_time_filter,
        "hour_start": args.hour_start,
        "hour_end": args.hour_end,
        "delta_threshold": args.delta_threshold,
        "delta_rolling_sec": args.delta_rolling_sec,
        "vwap_range_filter": args.vwap_range_filter,
    }
    for key, value in explicit_overrides.items():
        if value is not None:
            params[key] = value

    print(f"[PARAM-APPLY] effective_params={json.dumps(params, ensure_ascii=False, sort_keys=True)}")
    return params


# =============================================================================
# 1) ESTRATEGIA (SOLUSDT) - VWAP Trend Pullback (1H/15m/5m)
# =============================================================================

@dataclass
class StrategyConfig:
    symbol: str = "SOLUSDT"

    # Indicators
    ema_len: int = 20
    atr_len: int = 14

    # Pullback zone thresholds (in ATR units on 5m)
    touch_atr_mult: float = 0.25

    # Stop padding and minimum stop distance (ATR units on 5m)
    stop_atr_pad: float = 0.25
    min_stop_atr: float = 0.8

    # TP logic
    tp1_R: float = 1.0
    tp2_R: float = 2.0

    # Anti-chop
    max_vwap_crosses_60m: int = 6
    min_atr_pct_5m: float = 0.0012  # ATR/price >= 0.12% (tune)

    # Context chop filter (1H)
    ctx_min_dist_ema_atr_1h: float = 0.6

    # Pivot detection (15m)
    pivot_n: int = 2

    # Entry timeout (bars in 5m)
    entry_timeout_bars: int = 3
    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    # Funding costs (perpetual futures)
    enable_funding_cost: bool = True
    funding_rate_8h: float = 0.0001
    funding_interval_hours: int = 8

    # TP bid/ask model
    enable_tp_bidask_model: bool = True
    tp_bidask_half_spread_bps: float = 1.0
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # Daily drawdown kill switch
    enable_daily_dd_kill: bool = True
    daily_dd_limit_pct: float = 0.02  # 2% default
    daily_dd_mode: str = "soft"  # "soft" or "hard"
    daily_dd_timezone: str = "UTC"  # daily reset timezone
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    # Max stops per day kill switch
    enable_max_stops_kill: bool = True
    max_stops_per_day: int = 3
    max_stops_mode: str = "soft"  # "soft" or "hard"
    max_stops_timezone: str = "UTC"  # daily reset timezone
    # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    # Monthly volatility filter (daily ATR%)
    enable_monthly_vol_filter: bool = True
    daily_atr_len: int = 14
    monthly_atr_pct_quantile: float = 0.25
    monthly_atr_pct_floor: float = 0.0
    # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    # Runtime tunables (CLI overrides)
    cooldown_sec: float = 0.0
    max_trades_day: int = 10_000
    rr_min: float = 0.0
    use_time_filter: int = 0
    hour_start: int = 0
    hour_end: int = 24
    delta_threshold: float = 0.0
    delta_rolling_sec: int = 1


# ---- Indicators

def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def atr(df: pd.DataFrame, n: int) -> pd.Series:
    return true_range(df).ewm(span=n, adjust=False).mean()


# === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
def compute_daily_atr_pct(df_5m: pd.DataFrame, n: int = 14) -> pd.Series:
    df = df_5m.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    else:
        if df["ts"].dt.tz is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")

    d = df.set_index("ts")
    daily = pd.DataFrame({
        "open": d["open"].resample("1D").first(),
        "high": d["high"].resample("1D").max(),
        "low": d["low"].resample("1D").min(),
        "close": d["close"].resample("1D").last(),
        "volume": d["volume"].resample("1D").sum(),
    }).dropna()
    if daily.empty:
        return pd.Series(dtype=float)

    daily_atr = atr(daily, n)
    atr_pct = daily_atr / daily["close"].replace(0, np.nan)
    atr_pct.name = "atr_pct_daily"
    return atr_pct


def compute_monthly_vol_ok(
    df_5m: pd.DataFrame,
    atr_pct_daily: pd.Series,
    q: float = 0.25,
    floor: float = 0.0,
) -> pd.Series:
    if atr_pct_daily.empty:
        return pd.Series(False, index=df_5m.index)

    # Avoid pandas warning: converting tz-aware DatetimeIndex to Period drops tz info.
    _idx = atr_pct_daily.index
    try:
        if getattr(_idx, "tz", None) is not None:
            _idx = _idx.tz_convert(None)
    except Exception:
        pass
    daily_period = _idx.to_period("M")
    monthly_threshold = atr_pct_daily.groupby(daily_period).quantile(q)
    daily_threshold = daily_period.map(monthly_threshold).astype(float)
    daily_floor = np.maximum(daily_threshold, floor)
    daily_floor_series = pd.Series(daily_floor, index=atr_pct_daily.index)

    ts_floor = df_5m["ts"].dt.floor("D")
    atr_today = ts_floor.map(atr_pct_daily)
    thr_today = ts_floor.map(daily_floor_series)
    vol_ok = atr_today >= thr_today
    return vol_ok.fillna(False)
# === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===


# === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
def compute_atr_pct_proxy(df_5m: pd.DataFrame, n: int = 14) -> pd.Series:
    df = df_5m.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    else:
        if df["ts"].dt.tz is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")

    atr_pct_daily = compute_daily_atr_pct(df, n)
    if not atr_pct_daily.empty:
        ts_floor = df["ts"].dt.floor("D")
        atr_proxy = ts_floor.map(atr_pct_daily)
        return atr_proxy.fillna(0.0).astype(float)

    # Fallback proxy: 1h ATR% resample
    d = df.set_index("ts")
    hourly = pd.DataFrame({
        "open": d["open"].resample("1h").first(),
        "high": d["high"].resample("1h").max(),
        "low": d["low"].resample("1h").min(),
        "close": d["close"].resample("1h").last(),
        "volume": d["volume"].resample("1h").sum(),
    }).dropna()
    if hourly.empty:
        return pd.Series(0.0, index=df.index, dtype=float)

    hourly_atr = atr(hourly, n)
    atr_pct_hourly = hourly_atr / hourly["close"].replace(0, np.nan)
    atr_pct_hourly.name = "atr_pct_hourly"
    atr_proxy = df["ts"].dt.floor("h").map(atr_pct_hourly)
    return atr_proxy.fillna(0.0).astype(float)
# === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===


def slope(series: pd.Series, k: int = 3) -> pd.Series:
    return series - series.shift(k)


def session_key(ts: pd.Series) -> pd.Series:
    # Session daily reset (UTC day)
    return ts.dt.floor("D")


def vwap_session(df: pd.DataFrame) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    sess = session_key(df["ts"])
    pv = tp * df["volume"]
    return pv.groupby(sess).cumsum() / df["volume"].groupby(sess).cumsum()


# === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
TF_MAP = {"5m": "5min", "15m": "15min", "1h": "1h"}
# === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===


def resample_ohlcv(df5m: pd.DataFrame, tf: str) -> pd.DataFrame:
    rule = TF_MAP[tf]
    d = df5m.set_index("ts")
    out = pd.DataFrame({
        "open": d["open"].resample(rule).first(),
        "high": d["high"].resample(rule).max(),
        "low":  d["low"].resample(rule).min(),
        "close": d["close"].resample(rule).last(),
        "volume": d["volume"].resample(rule).sum(),
    }).dropna()
    return out.reset_index()


def pivots(df: pd.DataFrame, n: int) -> Tuple[pd.Series, pd.Series]:
    h = df["high"]
    l = df["low"]
    ph = pd.Series(False, index=df.index)
    pl = pd.Series(False, index=df.index)

    for i in range(n, len(df) - n):
        if h.iloc[i] > h.iloc[i-n:i].max() and h.iloc[i] > h.iloc[i+1:i+n+1].max():
            ph.iloc[i] = True
        if l.iloc[i] < l.iloc[i-n:i].min() and l.iloc[i] < l.iloc[i+1:i+n+1].min():
            pl.iloc[i] = True
    return ph, pl


def determine_context_bias_1h(df1h: pd.DataFrame, cfg: StrategyConfig) -> pd.Series:
    df = df1h.copy()
    df["ema"] = ema(df["close"], cfg.ema_len)
    df["atr"] = atr(df, cfg.atr_len)
    df["vwap"] = vwap_session(df)
    df["ema_slope"] = slope(df["ema"], k=3)

    dist = (df["close"] - df["ema"]).abs() / df["atr"].replace(0, np.nan)

    long_c1 = df["close"] > df["vwap"]
    long_c2 = df["ema_slope"] > 0
    long_c3 = dist >= cfg.ctx_min_dist_ema_atr_1h

    short_c1 = df["close"] < df["vwap"]
    short_c2 = df["ema_slope"] < 0
    short_c3 = dist >= cfg.ctx_min_dist_ema_atr_1h

    long_score = long_c1.astype(int) + long_c2.astype(int) + long_c3.astype(int)
    short_score = short_c1.astype(int) + short_c2.astype(int) + short_c3.astype(int)

    bias = pd.Series("NO_TRADE", index=df.index)
    bias[long_score >= 2] = "LONG"
    bias[short_score >= 2] = "SHORT"
    return bias


def structure_ok_15m(df15m: pd.DataFrame, cfg: StrategyConfig) -> pd.Series:
    df = df15m.copy().reset_index(drop=True)
    ph, pl = pivots(df, cfg.pivot_n)
    out = pd.Series("NONE", index=df.index)

    ph_idx: List[int] = []
    pl_idx: List[int] = []

    for i in range(len(df)):
        if ph.iloc[i]:
            ph_idx.append(i)
        if pl.iloc[i]:
            pl_idx.append(i)

        if len(ph_idx) >= 2 and len(pl_idx) >= 2:
            h_prev, h_last = df.loc[ph_idx[-2], "high"], df.loc[ph_idx[-1], "high"]
            l_prev, l_last = df.loc[pl_idx[-2], "low"], df.loc[pl_idx[-1], "low"]

            if h_last > h_prev and l_last > l_prev:
                out.iloc[i] = "LONG_OK"
            elif h_last < h_prev and l_last < l_prev:
                out.iloc[i] = "SHORT_OK"
    return out


def count_vwap_crosses_60m(df5m: pd.DataFrame, vwap5m: pd.Series) -> pd.Series:
    diff = df5m["close"] - vwap5m
    sign = np.sign(diff)
    cross = (sign != sign.shift(1)) & (sign != 0) & (sign.shift(1) != 0)
    # 12 bars 5m = 60m
    return cross.rolling(12).sum().fillna(0)


def generate_signals(df5m: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    """
    Output (por barra 5m):
      signal: "", "LONG", "SHORT"
      entry_long/short, stop_long/short, tp1_*, tp2_*
      bias_1h, struct_15m, ok_chop, ok_vol
    """
    df = df5m.copy().reset_index(drop=True)
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    else:
        # ensure UTC awareness if possible
        if df["ts"].dt.tz is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")

    # 5m indicators
    df["ema5"] = ema(df["close"], cfg.ema_len)
    df["atr5"] = atr(df, cfg.atr_len)
    df["vwap5"] = vwap_session(df)
    df["atr_pct"] = df["atr5"] / df["close"]
    df["vwap_crosses_60m"] = count_vwap_crosses_60m(df, df["vwap5"])

    # higher TF
    df15 = resample_ohlcv(df, "15m")
    df1h = resample_ohlcv(df, "1h")

    bias_1h = determine_context_bias_1h(df1h, cfg).set_axis(df1h["ts"]).reindex(df["ts"], method="ffill")
    struct_15 = structure_ok_15m(df15, cfg).set_axis(df15["ts"]).reindex(df["ts"], method="ffill")

    df["bias_1h"] = bias_1h.values
    df["struct_15m"] = struct_15.values

    # touch value zone
    touch_vwap = (df["close"] - df["vwap5"]).abs() <= cfg.touch_atr_mult * df["atr5"]
    touch_ema = (df["close"] - df["ema5"]).abs() <= cfg.touch_atr_mult * df["atr5"]
    df["touch_value"] = touch_vwap | touch_ema

    # triggers (confirmation)
    df["prev_high"] = df["high"].shift(1)
    df["prev_low"] = df["low"].shift(1)
    df["long_trigger"] = df["touch_value"] & (df["close"] > df["prev_high"])
    df["short_trigger"] = df["touch_value"] & (df["close"] < df["prev_low"])

    # filters
    df["ok_vol"] = df["atr_pct"] >= cfg.min_atr_pct_5m
    df["ok_chop"] = df["vwap_crosses_60m"] <= cfg.max_vwap_crosses_60m
    # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    atr_pct_daily = compute_daily_atr_pct(df, cfg.daily_atr_len)
    df["vol_ok"] = compute_monthly_vol_ok(
        df,
        atr_pct_daily,
        q=cfg.monthly_atr_pct_quantile,
        floor=cfg.monthly_atr_pct_floor,
    ).values
    # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===

    # entries
    df["entry_long"] = np.where(df["long_trigger"], df["high"] + 1e-8, np.nan)
    df["entry_short"] = np.where(df["short_trigger"], df["low"] - 1e-8, np.nan)

    # swing proxy last 6 bars
    lookback_swing = 6
    df["swing_low_5"] = df["low"].rolling(lookback_swing).min()
    df["swing_high_5"] = df["high"].rolling(lookback_swing).max()

    min_stop_dist = cfg.min_stop_atr * df["atr5"]

    raw_stop_long = df["swing_low_5"] - cfg.stop_atr_pad * df["atr5"]
    df["stop_long"] = np.where(
        df["long_trigger"],
        np.minimum(raw_stop_long, (df["entry_long"] - min_stop_dist)),
        np.nan
    )

    raw_stop_short = df["swing_high_5"] + cfg.stop_atr_pad * df["atr5"]
    df["stop_short"] = np.where(
        df["short_trigger"],
        np.maximum(raw_stop_short, (df["entry_short"] + min_stop_dist)),
        np.nan
    )

    # Rs + TPs
    df["R_long"] = df["entry_long"] - df["stop_long"]
    df["R_short"] = df["stop_short"] - df["entry_short"]

    df["tp1_long"] = df["entry_long"] + cfg.tp1_R * df["R_long"]
    df["tp2_long"] = df["entry_long"] + cfg.tp2_R * df["R_long"]
    df["tp1_short"] = df["entry_short"] - cfg.tp1_R * df["R_short"]
    df["tp2_short"] = df["entry_short"] - cfg.tp2_R * df["R_short"]

    # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    # final gating (base logic unchanged)
    long_ok = (
        (df["bias_1h"] == "LONG") &
        (df["struct_15m"] == "LONG_OK") &
        df["long_trigger"] &
        df["ok_vol"] & df["ok_chop"]
    )
    short_ok = (
        (df["bias_1h"] == "SHORT") &
        (df["struct_15m"] == "SHORT_OK") &
        df["short_trigger"] &
        df["ok_vol"] & df["ok_chop"]
    )

    df["signal_base"] = ""
    df.loc[long_ok, "signal_base"] = "LONG"
    df.loc[short_ok, "signal_base"] = "SHORT"

    df["signal"] = df["signal_base"]
    if cfg.enable_monthly_vol_filter:
        filtered = ~df["vol_ok"]
        df.loc[filtered, "signal"] = ""
        df.loc[filtered, [
            "entry_long",
            "entry_short",
            "stop_long",
            "stop_short",
            "tp1_long",
            "tp2_long",
            "tp1_short",
            "tp2_short",
        ]] = np.nan
    # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===

    return df


def get_last_signal_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    """Return a small snapshot of the latest signal row for observability-only logging."""
    if df is None or df.empty:
        return {}
    row = df.iloc[-1]

    def _pick(name: str) -> Any:
        if name not in df.columns:
            return None
        value = row.get(name)
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, np.generic):
            return value.item()
        if pd.isna(value):
            return None
        return value

    snapshot: Dict[str, Any] = {
        "close_ts": _pick("close_ts"),
        "timestamp": _pick("timestamp") or _pick("ts"),
        "close": _pick("close"),
        "signal": _pick("signal") or "",
    }
    for col in ("bias_1h", "struct_15m", "ok_vol", "ok_chop", "vol_ok", "atr_pct"):
        val = _pick(col)
        if val is not None:
            snapshot[col] = val
    return snapshot


# =============================================================================
# 2) ORDER ROUTER (CCXT BINANCE FUTURES) + SIZING
# =============================================================================

@dataclass
class RouterConfig:
    # Binance futures ccxt symbol
    symbol: str = "SOL/USDT:USDT"
    market_type: str = "future"
    leverage: int = 3
    margin_mode: Literal["isolated", "cross"] = "isolated"

    # Risk sizing
    account_equity_usdt: float = 1000.0
    risk_pct_per_trade: float = 0.0025
    # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
    max_notional_usdt: float = 0.0
    # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===

    # market constraints (overwritten by sync)
    price_tick: float = 0.001
    qty_step: float = 0.01
    min_qty: float = 0.01

    # bracket behavior
    tp1_reduce_frac: float = 0.5
    move_sl_to_be_on_tp1: bool = True
    be_pad_bps: float = 1.0

    # timeouts / control
    entry_timeout_sec: int = 15 * 60
    allow_one_plan_only: bool = True

    # reliability
    max_retries: int = 5
    retry_backoff_sec: float = 0.7

    # Safety
    dry_run: bool = True

    # persistence/logs
    state_path: str = "./state/order_manager_state.json"
    log_path: str = "./logs/bot.jsonl"


@dataclass
class BracketPlan:
    symbol: str
    side: Side
    entry_type: OrderType
    entry_price: Optional[float]
    stop_price: float
    tp1_price: float
    tp2_price: float
    qty: Optional[float] = None
    client_tag: str = ""
    meta: Optional[Dict[str, Any]] = None


class ExchangeClient:
    def set_leverage_and_margin(self, symbol: str, leverage: int, margin_mode: str) -> None:
        raise NotImplementedError

    def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_market_info(self, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError

    def create_order(self, symbol: str, type_: str, side: str, amount: float,
                     price: Optional[float], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def fetch_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError


class CCXTBinanceFuturesClient(ExchangeClient):
    def __init__(self, api_key: str, api_secret: str, market_type: str = "future"):
        import ccxt
        self.ex = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": market_type},
        })

    def set_leverage_and_margin(self, symbol: str, leverage: int, margin_mode: str) -> None:
        try:
            self.ex.set_margin_mode(margin_mode, symbol)
        except Exception:
            pass
        try:
            self.ex.set_leverage(leverage, symbol)
        except Exception:
            pass

    def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        return self.ex.fetch_ticker(symbol)

    def fetch_market_info(self, symbol: str) -> Dict[str, Any]:
        markets = self.ex.load_markets()
        if symbol not in markets:
            raise KeyError(f"Symbol not found in markets: {symbol}")
        return markets[symbol]

    def create_order(self, symbol: str, type_: str, side: str, amount: float,
                     price: Optional[float], params: Dict[str, Any]) -> Dict[str, Any]:
        return self.ex.create_order(symbol, type_, side, amount, price, params)

    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return self.ex.cancel_order(order_id, symbol)

    def fetch_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        return self.ex.fetch_open_orders(symbol)

    def fetch_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return self.ex.fetch_order(order_id, symbol)


@dataclass
class OrderRef:
    order_id: str
    client_order_id: str
    kind: str
    payload: Dict[str, Any]


class BinanceFuturesOrderRouter:
    def __init__(self, cfg: RouterConfig, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                 client: Optional[ExchangeClient] = None):
        self.cfg = cfg
        if client is not None:
            self.client = client
        else:
            if cfg.dry_run:
                self.client = None
            else:
                if not api_key or not api_secret:
                    raise ValueError("LIVE mode requires api_key/api_secret")
                self.client = CCXTBinanceFuturesClient(api_key, api_secret, cfg.market_type)
        self.symbol_info: Optional[Dict[str, Any]] = None

    def sync_symbol_info(self, symbol: Optional[str] = None) -> None:
        symbol = symbol or self.cfg.symbol
        if self.cfg.dry_run:
            self.symbol_info = {"symbol": symbol, "dry_run": True}
            return
        m = self.client.fetch_market_info(symbol)
        self.symbol_info = m
        price_prec = m.get("precision", {}).get("price", None)
        amt_prec = m.get("precision", {}).get("amount", None)
        if price_prec is not None:
            self.cfg.price_tick = 10 ** (-price_prec)
        if amt_prec is not None:
            self.cfg.qty_step = 10 ** (-amt_prec)
        lim = m.get("limits", {}).get("amount", {})
        if lim and lim.get("min") is not None:
            self.cfg.min_qty = float(lim["min"])

    def set_leverage_and_margin(self, symbol: str) -> None:
        if self.cfg.dry_run:
            return
        self.client.set_leverage_and_margin(symbol, self.cfg.leverage, self.cfg.margin_mode)

    def _get_last_price(self, symbol: str) -> float:
        t = self.client.fetch_ticker(symbol)
        px = t.get("last") or t.get("close")
        if px is None:
            raise RuntimeError("ticker missing last/close")
        return float(px)

    def compute_qty_by_risk(self, entry: float, stop: float) -> float:
        risk_usdt = self.cfg.account_equity_usdt * self.cfg.risk_pct_per_trade
        stop_dist = abs(entry - stop)
        if stop_dist <= 0:
            raise ValueError("stop_dist <= 0")

        qty = risk_usdt / stop_dist
        # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
        notional = qty * entry
        if self.cfg.max_notional_usdt > 0 and notional > self.cfg.max_notional_usdt:
            qty = self.cfg.max_notional_usdt / entry
        notional = qty * entry
        # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===

        qty = max(qty, self.cfg.min_qty)
        qty = _round_to_step(qty, self.cfg.qty_step)
        if qty < self.cfg.min_qty:
            raise ValueError("qty < min_qty after rounding")
        return qty

    def _safe_create(self, kind: str, symbol: str, type_: str, side: str, amount: float,
                     price: Optional[float], params: Dict[str, Any], client_id: str) -> OrderRef:
        if self.cfg.dry_run:
            ref = OrderRef(order_id=f"DRY_{uuid.uuid4().hex[:8]}", client_order_id=client_id, kind=kind, payload={
                "dry_run": True, "symbol": symbol, "type": type_, "side": side, "amount": amount, "price": price, "params": params
            })
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "DRY_CREATE", "kind": kind, "ref": asdict(ref)})
            return ref

        last_err = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                _log_line(self.cfg.log_path, {
                    "ts_ms": _now_ms(), "event": "CREATE_ORDER", "kind": kind,
                    "symbol": symbol, "type": type_, "side": side, "amount": amount, "price": price,
                    "params": params, "attempt": attempt
                })
                resp = self.client.create_order(symbol, type_, side, amount, price, params)
                ref = OrderRef(order_id=str(resp.get("id", "")), client_order_id=client_id, kind=kind, payload=resp)
                _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "CREATE_OK", "kind": kind, "id": ref.order_id, "client_id": client_id})
                return ref
            except Exception as e:
                last_err = repr(e)
                _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "CREATE_ERR", "kind": kind, "client_id": client_id, "err": last_err, "attempt": attempt})
                time.sleep(self.cfg.retry_backoff_sec * attempt)

        raise RuntimeError(f"Failed create {kind}. last_err={last_err}")

    def _safe_cancel(self, symbol: str, order_id: str) -> None:
        if self.cfg.dry_run:
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "DRY_CANCEL", "symbol": symbol, "order_id": order_id})
            return
        try:
            self.client.cancel_order(order_id, symbol)
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "CANCEL_OK", "symbol": symbol, "order_id": order_id})
        except Exception as e:
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "CANCEL_ERR", "symbol": symbol, "order_id": order_id, "err": repr(e)})

    # primitives

    def place_entry(self, plan: BracketPlan, qty: float, tag: str) -> OrderRef:
        symbol = plan.symbol
        side = "buy" if plan.side == "LONG" else "sell"
        entry_cid = _mk_client_id("E", tag)

        if plan.entry_type == "MARKET":
            type_ = "market"
            price = None
            params = {"newClientOrderId": entry_cid}
        elif plan.entry_type == "LIMIT":
            if plan.entry_price is None:
                raise ValueError("LIMIT entry requires entry_price")
            type_ = "limit"
            price = float(_round_price(plan.entry_price, self.cfg.price_tick))
            params = {"newClientOrderId": entry_cid, "timeInForce": "GTC"}
        elif plan.entry_type == "STOP_MARKET":
            if plan.entry_price is None:
                raise ValueError("STOP_MARKET entry requires entry_price")
            type_ = "stop_market"
            price = None
            params = {"newClientOrderId": entry_cid, "stopPrice": float(_round_price(plan.entry_price, self.cfg.price_tick))}
        else:
            raise ValueError(f"Unsupported entry_type: {plan.entry_type}")

        return self._safe_create("ENTRY", symbol, type_, side, qty, price, params, entry_cid)

    def place_stop_loss(self, plan: BracketPlan, qty: float, tag: str, stop_price: float) -> OrderRef:
        symbol = plan.symbol
        side = "sell" if plan.side == "LONG" else "buy"
        sl_cid = _mk_client_id("SL", tag)
        type_ = "stop_market"
        params = {
            "newClientOrderId": sl_cid,
            "stopPrice": float(_round_price(stop_price, self.cfg.price_tick)),
            "reduceOnly": True,
            "closePosition": False,
        }
        return self._safe_create("STOP_LOSS", symbol, type_, side, qty, None, params, sl_cid)

    def place_take_profit_limit(self, plan: BracketPlan, qty: float, tag: str, tp_price: float, which: str) -> OrderRef:
        symbol = plan.symbol
        side = "sell" if plan.side == "LONG" else "buy"
        cid = _mk_client_id(which, tag)
        type_ = "limit"
        params = {"newClientOrderId": cid, "timeInForce": "GTC", "reduceOnly": True}
        price = float(_round_price(tp_price, self.cfg.price_tick))
        return self._safe_create(which, symbol, type_, side, qty, price, params, cid)

    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    def place_reduce_only_market(self, symbol: str, side: str, qty: float, tag: str) -> OrderRef:
        cid = _mk_client_id("KILL", tag)
        params = {"newClientOrderId": cid, "reduceOnly": True}
        return self._safe_create("KILL_CLOSE", symbol, "market", side, qty, None, params, cid)
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===


# =============================================================================
# 3) ORDER MANAGER (STATE MACHINE) - entrada -> fill -> SL/TP -> TP1->BE -> DONE
# =============================================================================

@dataclass
class OMState:
    status: MgrStatus = "IDLE"
    active_tag: str = ""
    plan: Optional[Dict[str, Any]] = None

    entry_order_id: str = ""
    entry_client_id: str = ""
    entry_placed_ms: int = 0
    entry_fill_price: Optional[float] = None
    qty: Optional[float] = None

    sl_order_id: str = ""
    tp1_order_id: str = ""
    tp2_order_id: str = ""
    sl_price: Optional[float] = None
    tp1_price: Optional[float] = None
    tp2_price: Optional[float] = None

    tp1_done: bool = False
    done_reason: str = ""
    last_error: str = ""


class OrderManager:
    def __init__(self, cfg: RouterConfig, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                 client: Optional[ExchangeClient] = None):
        self.cfg = cfg
        self.router = BinanceFuturesOrderRouter(cfg, api_key=api_key, api_secret=api_secret, client=client)
        self.state = OMState()

    # persistence

    def save_state(self) -> None:
        _ensure_dir(self.cfg.state_path)
        with open(self.cfg.state_path, "w", encoding="utf-8") as f:
            json.dump(asdict(self.state), f, ensure_ascii=False, indent=2)

    def load_state(self) -> None:
        if not os.path.exists(self.cfg.state_path):
            return
        with open(self.cfg.state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.state = OMState(**data)
        _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "STATE_LOADED", "state": data})

    # exchange sync

    def sync_symbol_info(self) -> None:
        self.router.sync_symbol_info(self.cfg.symbol)

    # helpers

    def is_idle(self) -> bool:
        return self.state.status in ("IDLE", "DONE", "CANCELLED")

    def _plan(self) -> BracketPlan:
        if not self.state.plan:
            raise RuntimeError("Missing plan in state")
        return BracketPlan(**self.state.plan)

    # public

    def submit_plan(self, plan: BracketPlan) -> None:
        if self.cfg.allow_one_plan_only and not self.is_idle():
            raise RuntimeError(f"OrderManager busy: status={self.state.status}")
        if plan.symbol != self.cfg.symbol:
            raise ValueError(f"plan.symbol {plan.symbol} != cfg.symbol {self.cfg.symbol}")

        tag = plan.client_tag or uuid.uuid4().hex[:12]
        self.state = OMState(status="IDLE", active_tag=tag, plan=asdict(plan))

        _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "PLAN_SUBMITTED", "tag": tag, "plan": asdict(plan)})
        self.save_state()
        self._place_entry()

    def step(self) -> None:
        try:
            if self.state.status == "IDLE":
                return
            if self.state.status == "ENTRY_PLACED":
                self._check_entry_fill_or_timeout()
                return
            if self.state.status in ("IN_POSITION", "EXITING"):
                self._check_exit_progress()
                return
        except Exception as e:
            self.state.status = "ERROR"
            self.state.last_error = repr(e)
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "OM_ERROR", "err": self.state.last_error, "state": asdict(self.state)})
            self.save_state()

    def cancel_active(self, reason: str = "manual_cancel") -> None:
        if self.is_idle():
            return
        plan = self._plan()
        for oid in [self.state.entry_order_id, self.state.sl_order_id, self.state.tp1_order_id, self.state.tp2_order_id]:
            if oid:
                self.router._safe_cancel(plan.symbol, oid)
        self.state.status = "CANCELLED"
        self.state.done_reason = reason
        _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "CANCELLED", "reason": reason})
        self.save_state()

    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    def emergency_close_position_market(self, reason: str = "daily_dd_kill") -> None:
        if self.is_idle():
            return
        plan = self._plan()
        qty = float(self.state.qty or 0.0)
        if qty <= 0:
            return

        for oid in [self.state.entry_order_id, self.state.sl_order_id, self.state.tp1_order_id, self.state.tp2_order_id]:
            if oid:
                self.router._safe_cancel(plan.symbol, oid)

        if self.cfg.dry_run:
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "DRY_KILL_CLOSE", "reason": reason, "qty": qty})
        else:
            side = "sell" if plan.side == "LONG" else "buy"
            ref = self.router.place_reduce_only_market(plan.symbol, side, qty, self.state.active_tag or "kill")
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "KILL_CLOSE", "reason": reason, "order": asdict(ref)})

        self.state.status = "DONE"
        self.state.done_reason = reason
        self.save_state()
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===

    # internal transitions

    def _place_entry(self) -> None:
        plan = self._plan()
        tag = self.state.active_tag

        entry_price = _as_float(plan.entry_price)
        stop_price = _round_price(float(plan.stop_price), self.cfg.price_tick)
        tp1_price = _round_price(float(plan.tp1_price), self.cfg.price_tick)
        tp2_price = _round_price(float(plan.tp2_price), self.cfg.price_tick)

        # sizing
        if plan.qty is not None:
            qty = _round_to_step(float(plan.qty), self.cfg.qty_step)
            qty = max(qty, self.cfg.min_qty)
        else:
            if plan.entry_type == "MARKET":
                if self.cfg.dry_run:
                    raise RuntimeError("dry_run MARKET entry requires plan.qty or plan.entry_price reference.")
                ref_px = self.router._get_last_price(plan.symbol)
                qty = self.router.compute_qty_by_risk(ref_px, stop_price)
            else:
                if entry_price is None:
                    raise ValueError("Non-market entry requires entry_price")
                qty = self.router.compute_qty_by_risk(_round_price(float(entry_price), self.cfg.price_tick), stop_price)

        self.router.set_leverage_and_margin(plan.symbol)
        entry_ref = self.router.place_entry(plan, qty, tag)

        self.state.status = "ENTRY_PLACED"
        self.state.entry_order_id = entry_ref.order_id
        self.state.entry_client_id = entry_ref.client_order_id
        self.state.entry_placed_ms = _now_ms()
        self.state.qty = qty

        self.state.sl_price = stop_price
        self.state.tp1_price = tp1_price
        self.state.tp2_price = tp2_price

        _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "ENTRY_PLACED", "tag": tag, "entry": asdict(entry_ref), "qty": qty,
                                      "sl": stop_price, "tp1": tp1_price, "tp2": tp2_price})
        self.save_state()

        if plan.entry_type == "MARKET":
            self._check_entry_fill_or_timeout()

    def _check_entry_fill_or_timeout(self) -> None:
        plan = self._plan()
        tag = self.state.active_tag

        elapsed = (_now_ms() - self.state.entry_placed_ms) / 1000.0
        if elapsed >= self.cfg.entry_timeout_sec:
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "ENTRY_TIMEOUT", "tag": tag, "elapsed_sec": elapsed})
            if self.state.entry_order_id:
                self.router._safe_cancel(plan.symbol, self.state.entry_order_id)
            self.state.status = "CANCELLED"
            self.state.done_reason = "entry_timeout"
            self.save_state()
            return

        if self.cfg.dry_run:
            fill_price = plan.entry_price if plan.entry_price is not None else plan.stop_price
            self.state.entry_fill_price = float(fill_price)
            self.state.status = "IN_POSITION"
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "ENTRY_FILLED_DRY", "tag": tag, "fill_price": self.state.entry_fill_price})
            self.save_state()
            self._place_exits_after_entry_fill()
            return

        # live polling
        try:
            od = self.router.client.fetch_order(self.state.entry_order_id, plan.symbol)
            status = (od.get("status") or "").lower()
            filled = float(od.get("filled") or 0.0)
            avg = _as_float(od.get("average")) or _as_float(od.get("avgPrice")) or _as_float(od.get("price"))

            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "ENTRY_CHECK", "tag": tag, "status": status, "filled": filled, "avg": avg})

            if status in ("closed",) or (filled > 0 and status in ("open", "closed")):
                if avg is None:
                    avg = self.router._get_last_price(plan.symbol)
                self.state.entry_fill_price = float(avg)
                self.state.status = "IN_POSITION"
                _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "ENTRY_FILLED", "tag": tag, "fill_price": self.state.entry_fill_price})
                self.save_state()
                self._place_exits_after_entry_fill()
        except Exception as e:
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "ENTRY_CHECK_ERR", "tag": tag, "err": repr(e)})

    def _place_exits_after_entry_fill(self) -> None:
        plan = self._plan()
        tag = self.state.active_tag
        qty = float(self.state.qty or 0.0)
        if qty <= 0:
            raise RuntimeError("Invalid qty")

        sl_price = float(self.state.sl_price)
        tp1_price = float(self.state.tp1_price)
        tp2_price = float(self.state.tp2_price)

        sl_ref = self.router.place_stop_loss(plan, qty, tag, sl_price)

        tp1_qty = qty * self.cfg.tp1_reduce_frac
        tp2_qty = qty - tp1_qty

        tp1_ref = None
        if tp1_qty >= self.cfg.min_qty:
            tp1_ref = self.router.place_take_profit_limit(plan, _round_to_step(tp1_qty, self.cfg.qty_step), tag, tp1_price, "TP1")

        tp2_ref = self.router.place_take_profit_limit(plan, _round_to_step(tp2_qty, self.cfg.qty_step), tag, tp2_price, "TP2")

        self.state.sl_order_id = sl_ref.order_id
        self.state.tp1_order_id = tp1_ref.order_id if tp1_ref else ""
        self.state.tp2_order_id = tp2_ref.order_id
        self.state.status = "EXITING"

        _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "EXITS_PLACED", "tag": tag,
                                      "sl": asdict(sl_ref),
                                      "tp1": asdict(tp1_ref) if tp1_ref else None,
                                      "tp2": asdict(tp2_ref)})
        self.save_state()

    def _check_exit_progress(self) -> None:
        plan = self._plan()
        tag = self.state.active_tag

        if self.cfg.dry_run:
            # En dry_run dejamos marcado DONE para no “colgar” el ejemplo.
            self.state.status = "DONE"
            self.state.done_reason = "dry_run_done"
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "DONE_DRY", "tag": tag})
            self.save_state()
            return

        def fetch_status(oid: str) -> Optional[str]:
            if not oid:
                return None
            od = self.router.client.fetch_order(oid, plan.symbol)
            return (od.get("status") or "").lower()

        try:
            sl_status = fetch_status(self.state.sl_order_id)
            tp1_status = fetch_status(self.state.tp1_order_id) if self.state.tp1_order_id else None
            tp2_status = fetch_status(self.state.tp2_order_id)

            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "EXIT_CHECK", "tag": tag,
                                          "sl": sl_status, "tp1": tp1_status, "tp2": tp2_status,
                                          "tp1_done": self.state.tp1_done})

            if (not self.state.tp1_done) and tp1_status == "closed":
                self.state.tp1_done = True
                _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "TP1_FILLED", "tag": tag})
                self.save_state()
                if self.cfg.move_sl_to_be_on_tp1:
                    self._move_sl_to_be()

            if tp2_status == "closed":
                self._finalize_done("tp2_filled")
                return
            if sl_status == "closed":
                self._finalize_done("sl_filled")
                return

        except Exception as e:
            _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "EXIT_CHECK_ERR", "tag": tag, "err": repr(e)})

    def _move_sl_to_be(self) -> None:
        plan = self._plan()
        tag = self.state.active_tag
        entry = float(self.state.entry_fill_price or 0.0)
        if entry <= 0:
            return

        pad = entry * (self.cfg.be_pad_bps / 10000.0)
        new_sl = entry - pad if plan.side == "LONG" else entry + pad
        new_sl = _round_price(new_sl, self.cfg.price_tick)

        if self.state.sl_order_id:
            self.router._safe_cancel(plan.symbol, self.state.sl_order_id)

        qty_rem = float(self.state.qty or 0.0)
        if qty_rem <= 0:
            return

        sl_ref = self.router.place_stop_loss(plan, qty_rem, tag, new_sl)
        self.state.sl_order_id = sl_ref.order_id
        self.state.sl_price = new_sl

        _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "SL_MOVED_TO_BE", "tag": tag, "new_sl": new_sl, "sl": asdict(sl_ref)})
        self.save_state()

    def _finalize_done(self, reason: str) -> None:
        plan = self._plan()
        tag = self.state.active_tag

        # cancel leftover orders (best effort)
        for oid in [self.state.sl_order_id, self.state.tp1_order_id, self.state.tp2_order_id]:
            if oid:
                self.router._safe_cancel(plan.symbol, oid)

        self.state.status = "DONE"
        self.state.done_reason = reason
        _log_line(self.cfg.log_path, {"ts_ms": _now_ms(), "event": "DONE", "tag": tag, "reason": reason})
        self.save_state()


# =============================================================================
# 4) ADAPTER: SEÑAL -> BRACKETPLAN (100% plug and play)
# =============================================================================

def signal_row_to_plan(row: pd.Series, router_cfg: RouterConfig, *, entry_type: OrderType = "STOP_MARKET") -> Optional[BracketPlan]:
    """
    Convierte la última fila (cerrada) del dataframe de señales en un BracketPlan listo para el OrderManager.
    Retorna None si row["signal"] == "".
    """
    sig = str(row.get("signal", "")).upper().strip()
    if sig not in ("LONG", "SHORT"):
        return None

    # Extract levels depending on side
    if sig == "LONG":
        entry = float(row["entry_long"])
        stop = float(row["stop_long"])
        tp1 = float(row["tp1_long"])
        tp2 = float(row["tp2_long"])
    else:
        entry = float(row["entry_short"])
        stop = float(row["stop_short"])
        tp1 = float(row["tp1_short"])
        tp2 = float(row["tp2_short"])

    # Tag deterministic-ish (timestamp + side)
    ts = row["ts"]
    if isinstance(ts, pd.Timestamp):
        ts_str = ts.strftime("%Y%m%d_%H%M")
    else:
        ts_str = str(ts)

    tag = f"sol_vwap_{sig.lower()}_{ts_str}"

    meta = {
        "bias_1h": row.get("bias_1h", None),
        "struct_15m": row.get("struct_15m", None),
        "vwap_crosses_60m": float(row.get("vwap_crosses_60m", np.nan)) if "vwap_crosses_60m" in row else None,
        "atr_pct": float(row.get("atr_pct", np.nan)) if "atr_pct" in row else None,
    }

    # IMPORTANT: For STOP_MARKET entry, we set entry_price = trigger price.
    return BracketPlan(
        symbol=router_cfg.symbol,
        side=sig,  # type: ignore
        entry_type=entry_type,
        entry_price=entry,
        stop_price=stop,
        tp1_price=tp1,
        tp2_price=tp2,
        qty=None,  # size by risk
        client_tag=tag,
        meta=meta,
    )


# =============================================================================
# 5) RUNNER (CSV BACKTEST MODE + LIVE SKELETON)
# =============================================================================

def run_csv_signals(csv_path: str, strat_cfg: StrategyConfig) -> pd.DataFrame:
    """
    Carga CSV 5m (ts,open,high,low,close,volume) y devuelve df con señales.
    """
    df = load_ohlcv_csv(csv_path, strict=True)
    out = generate_signals(df, strat_cfg)
    return out


def run_csv_plug_and_play(csv_path: str) -> None:
    """
    End-to-end en DRY_RUN:
    - genera señales
    - toma última señal disponible y la envía al OrderManager
    """
    strat_cfg = StrategyConfig(symbol="SOLUSDT")
    router_cfg = RouterConfig(
        symbol="SOL/USDT:USDT",
        dry_run=True,
        account_equity_usdt=1000.0,
        risk_pct_per_trade=0.0025,
        leverage=3,
        margin_mode="isolated",
        log_path="./logs/bot.jsonl",
        state_path="./state/order_manager_state.json",
    )

    sigdf = run_csv_signals(csv_path, strat_cfg)
    print(sigdf[["ts", "signal", "bias_1h", "struct_15m"]].tail(20))

    om = OrderManager(router_cfg)
    om.sync_symbol_info()
    om.load_state()

    # Find last non-empty signal (closed bars)
    last_sig = sigdf[sigdf["signal"] != ""].tail(1)
    if last_sig.empty:
        print("No signals in CSV.")
        return

    row = last_sig.iloc[0]
    plan = signal_row_to_plan(row, router_cfg, entry_type="STOP_MARKET")
    if plan is None:
        print("Plan is None.")
        return

    if om.is_idle():
        om.submit_plan(plan)
    else:
        print("OrderManager busy:", om.state.status)

    # Step a few times (dry mode will complete)
    for _ in range(3):
        om.step()
        time.sleep(0.2)

    print("STATE:", json.dumps(asdict(om.state), indent=2))


# === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
def live_loop_skeleton(
    poll_sec: float = 2.0,
    enable_daily_dd_kill: bool = True,
    daily_dd_limit_pct: float = 0.02,
    daily_dd_mode: str = "soft",
    daily_dd_timezone: str = "UTC",
    params: Optional[Dict[str, Any]] = None,
) -> None:
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    """
    Skeleton live:
    - vos tenés que alimentar df5m con velas recientes (últimas N)
    - cada vez que hay una nueva vela cerrada 5m, generás señales y si hay señal: submit_plan
    - aparte, siempre llamás om.step() para manejar fills/SL/TP

    Este skeleton NO descarga OHLCV. Está preparado para que lo conectes a tu feed.
    """
    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    strat_cfg = StrategyConfig(
        symbol="SOLUSDT",
        enable_daily_dd_kill=enable_daily_dd_kill,
        daily_dd_limit_pct=daily_dd_limit_pct,
        daily_dd_mode=daily_dd_mode,
        daily_dd_timezone=daily_dd_timezone,
    )
    apply_overrides(strat_cfg, params or {})
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===

    router_cfg = RouterConfig(
        symbol="SOL/USDT:USDT",
        dry_run=False,
        account_equity_usdt=float(os.getenv("EQUITY_USDT", "1000")),
        risk_pct_per_trade=float(os.getenv("RISK_PCT", "0.0025")),
        leverage=int(os.getenv("LEVERAGE", "3")),
        margin_mode=os.getenv("MARGIN_MODE", "isolated"),  # "isolated" or "cross"
        log_path="./logs/bot.jsonl",
        state_path="./state/order_manager_state.json",
    )

    api_key = os.getenv("BINANCE_KEY")
    api_secret = os.getenv("BINANCE_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("Missing BINANCE_KEY / BINANCE_SECRET env vars.")

    om = OrderManager(router_cfg, api_key=api_key, api_secret=api_secret)
    om.sync_symbol_info()
    om.load_state()

    # TODO: Replace this with your real 5m candle buffer.
    df5m = pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    last_closed_ts: Optional[pd.Timestamp] = None
    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    dd_state = DailyDDState()
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===

    while True:
        # 1) ALWAYS manage orders
        om.step()

        # 2) UPDATE df5m from your data source (websocket/rest/db)
        #    It must contain the most recent CLOSED 5m bar(s).
        #
        # Example expectation:
        #   new_df5m = fetch_recent_5m_bars(...)
        #   df5m = new_df5m
        #
        # For now, this skeleton does nothing if df5m is empty.
        if len(df5m) >= 300:
            sigdf = generate_signals(df5m, strat_cfg)

            # Use last CLOSED bar (last row is assumed closed in your feed)
            row = sigdf.iloc[-1]
            ts = row["ts"]

            # Submit only once per bar close
            if last_closed_ts is None or ts > last_closed_ts:
                last_closed_ts = ts

                # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
                if strat_cfg.enable_daily_dd_kill:
                    day_key = _daily_key(ts, strat_cfg.daily_dd_timezone)
                    if dd_state.day_key != day_key:
                        dd_state.day_key = day_key
                        dd_state.equity_day_start = float(router_cfg.account_equity_usdt)
                        dd_state.kill_active = False
                    if dd_state.equity_day_start:
                        dd_pct = (float(router_cfg.account_equity_usdt) / dd_state.equity_day_start) - 1.0
                    else:
                        dd_pct = 0.0
                    if dd_pct <= -float(strat_cfg.daily_dd_limit_pct):
                        dd_state.kill_active = True
                        if strat_cfg.daily_dd_mode == "hard" and om.state.status in ("IN_POSITION", "EXITING"):
                            om.emergency_close_position_market(reason="daily_dd_kill")

                if dd_state.kill_active:
                    time.sleep(poll_sec)
                    continue
                # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===

                if om.is_idle():
                    plan = signal_row_to_plan(row, router_cfg, entry_type="STOP_MARKET")
                    if plan is not None:
                        om.submit_plan(plan)

        time.sleep(poll_sec)


# =============================================================================
# BACKTEST (SOLUSDT)
# =============================================================================

BTSide = Literal["LONG", "SHORT"]

@dataclass
class BTConfig:
    fee_taker: float = 0.0004         # 0.04% taker as baseline (ajustá)
    fee_maker: float = 0.0002         # 0.02% maker default
    slippage_bps_entry: float = 1.0   # 1 bp por fill (entry)
    slippage_bps_tp: float = 0.0      # TP slippage (0 si limit maker)
    slippage_bps_stop: float = 2.0    # 2 bp por fill (STOP)
    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    slip_model: str = "fixed"  # fixed | dynamic
    slip_atr_ref_pct: float = 0.01
    slip_notional_ref: float = 10000.0
    slip_k_size: float = 0.5
    slip_k_atr_entry: float = 0.75
    slip_k_atr_stop: float = 1.5
    slip_k_atr_tp: float = 0.25
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    tp_is_maker: bool = True
    initial_equity_usdt: float = 1000.0
    risk_pct_per_trade: float = 0.0025  # 0.25%
    # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
    # si querés cap por notional como en router:
    max_notional_usdt: float = 0.0
    # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===

    # comportamiento:
    entry_timeout_bars: int = 3       # si no activa entrada en N barras, cancel
    conservative_intrabar: bool = True
    # Si en una misma barra toca stop y tp, qué asumimos?
    # Conservative: para LONG si low<=stop y high>=tp => asumimos STOP primero.
    # Para SHORT si high>=stop y low<=tp => STOP primero.
    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    # Funding costs (perpetual futures)
    enable_funding_cost: bool = True
    funding_rate_8h: float = 0.0001
    funding_interval_hours: int = 8

    # TP bid/ask model
    enable_tp_bidask_model: bool = True
    tp_bidask_half_spread_bps: float = 1.0
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===


# === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
@dataclass
class DailyDDState:
    day_key: Optional[str] = None
    equity_day_start: float = 0.0
    kill_active: bool = False


def _daily_key(ts: pd.Timestamp, tz_name: str) -> str:
    # === CODEX_PATCH_BEGIN: TZ_ENSURE_UTC ===
    ts = ensure_utc(ts if isinstance(ts, pd.Timestamp) else pd.to_datetime(ts, utc=True))
    # === CODEX_PATCH_END: TZ_ENSURE_UTC ===
    if tz_name and tz_name.upper() != "UTC":
        ts = ts.tz_convert(tz_name)
    return ts.strftime("%Y-%m-%d")
# === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===


@dataclass
class TradeRec:
    side: BTSide
    signal_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    exit_ts: pd.Timestamp
    entry: float
    exit: float
    stop: float
    tp1: float
    tp2: float
    qty: float
    pnl_gross_usdt: float
    pnl_net_usdt: float
    fees_usdt: float
    tp1_pnl_gross_usdt: float
    tp1_fee_usdt: float
    pnl_R: float
    exit_reason: str
    took_tp1: bool
    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    funding_cost_usdt: float
    holding_hours: float
    tp_fill_price: Optional[float]
    tp_bidask_half_spread_bps: Optional[float]
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===


TRADE_COLUMNS: List[str] = [
    "side",
    "signal_ts",
    "entry_ts",
    "exit_ts",
    "entry",
    "exit",
    "stop",
    "tp1",
    "tp2",
    "qty",
    "pnl_gross_usdt",
    "pnl_net_usdt",
    "fees_usdt",
    "tp1_pnl_gross_usdt",
    "tp1_fee_usdt",
    "pnl_R",
    "exit_reason",
    "took_tp1",
    "funding_cost_usdt",
    "holding_hours",
    "tp_fill_price",
    "tp_bidask_half_spread_bps",
]

EQUITY_COLUMNS: List[str] = ["ts", "equity_usdt", "note"]


def trade_rec_to_dict(trade: TradeRec) -> Dict[str, Any]:
    data = asdict(trade)
    return {col: data.get(col) for col in TRADE_COLUMNS}


def _handle_issue(msg: str, strict: bool) -> None:
    if strict:
        raise ValueError(msg)
    print(f"WARNING: {msg}")


def load_ohlcv_csv(csv_path: str, strict: bool) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = {"ts", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        msg = f"CSV missing columns: {sorted(missing)}"
        if strict:
            raise ValueError(msg)
        print(f"WARNING: {msg}")
        return pd.DataFrame(columns=list(required))

    df = df[list(required)].copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    if df["ts"].isna().any():
        bad = int(df["ts"].isna().sum())
        _handle_issue(f"Found {bad} rows with invalid ts; dropping.", strict)
        df = df.dropna(subset=["ts"])

    df = df.sort_values("ts")
    before = len(df)
    df = df.drop_duplicates(subset=["ts"], keep="last")
    dropped = before - len(df)
    if dropped:
        _handle_issue(f"Dropped {dropped} duplicate ts rows.", strict)

    bad_rows = df[["open", "high", "low", "close", "volume"]].isna().any(axis=1)
    if bad_rows.any():
        dropped = int(bad_rows.sum())
        _handle_issue(f"Dropped {dropped} rows with NaN OHLCV.", strict)
        df = df.loc[~bad_rows]

    if df.empty:
        _handle_issue("CSV has no usable rows after cleaning.", strict)

    return df.reset_index(drop=True)


def _apply_slip(px: float, side: BTSide, is_entry: bool, slippage_bps: float) -> float:
    slip = px * (slippage_bps / 10000.0)
    if is_entry:
        return px + slip if side == "LONG" else px - slip
    return px - slip if side == "LONG" else px + slip


def _fee_cost(notional: float, fee_rate: float) -> float:
    return notional * fee_rate


# === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
def slippage_bps(
    kind: Literal["entry", "stop", "tp_half_spread"],
    notional: float,
    atr_pct: float,
    *,
    model: str,
    base_entry: float,
    base_stop: float,
    base_tp_half_spread: float,
    atr_ref_pct: float,
    notional_ref: float,
    k_size: float,
    k_atr_entry: float,
    k_atr_stop: float,
    k_atr_tp: float,
) -> float:
    if model == "fixed":
        if kind == "entry":
            return float(base_entry)
        if kind == "stop":
            return float(base_stop)
        return float(base_tp_half_spread)

    atr_ratio = (float(atr_pct) / float(atr_ref_pct)) if atr_ref_pct > 0 else 0.0
    size_term = 0.0
    if notional_ref > 0 and notional > 0:
        size_term = float(k_size) * math.sqrt(float(notional) / float(notional_ref))

    if kind == "entry":
        slip = float(base_entry) + float(k_atr_entry) * atr_ratio + size_term
    elif kind == "stop":
        slip = float(base_stop) + float(k_atr_stop) * atr_ratio + size_term
    else:
        slip = float(base_tp_half_spread) + float(k_atr_tp) * atr_ratio

    return max(0.0, slip)
# === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===


# === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
def _tp_fill_price(
    tp_price: float,
    side: BTSide,
    bt: BTConfig,
    tp_half_spread_bps: Optional[float] = None,
) -> float:
    if bt.enable_tp_bidask_model:
        half_spread = (
            float(tp_half_spread_bps)
            if tp_half_spread_bps is not None
            else float(bt.tp_bidask_half_spread_bps)
        )
        adj = tp_price * (half_spread / 10000.0)
        return tp_price - adj if side == "LONG" else tp_price + adj
    tp_slip_bps = 0.0 if bt.tp_is_maker else bt.slippage_bps_tp
    return _apply_slip(tp_price, side, False, tp_slip_bps)
# === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===


# === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
# === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
def backtest_from_signals(
    sigdf: pd.DataFrame,
    bt: BTConfig,
    strat_cfg: StrategyConfig,
    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    atr_pct_proxy: Optional[pd.Series] = None,
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    return_state: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, float, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
# === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    """
    Backtest event-driven basado en señales 5m de generate_signals().

    Reglas:
    - Entrada: stop-entry (entry_long/entry_short) se activa si la barra siguiente cruza el nivel.
    - SL/TP: se evalúan intrabar. Si en una misma barra se cruzan stop y tp => prioridad conservadora (stop primero).
    - TP1: reduce 50% y mueve stop a BE (muy simple) y deja correr a TP2.
    - Fees: taker en entry stop/stop loss; maker en TP1/TP2 (limit).
    - Risk sizing: qty = (equity*risk_pct) / stop_dist (cap notional opcional)
    """
    df = sigdf.copy().reset_index(drop=True)
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True)

    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    if atr_pct_proxy is not None and len(atr_pct_proxy) == len(df):
        df["atr_pct_proxy"] = pd.Series(atr_pct_proxy).reset_index(drop=True).astype(float)
    elif atr_pct_proxy is not None:
        df["atr_pct_proxy"] = pd.Series(atr_pct_proxy).reindex(df.index).astype(float)
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===

    equity = bt.initial_equity_usdt
    equity_curve = []
    trades: List[TradeRec] = []

    # estado
    pending = None  # dict con plan pendiente
    pending_age = 0
    pos = None      # dict con posición abierta (incluye qty_remaining)
    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    total_funding_cost_usdt = 0.0
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    dd_state = DailyDDState()
    daily_kills_count = 0
    max_daily_dd_pct_observed = 0.0
    days_killed: set[str] = set()
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    max_stops_day_key: Optional[str] = None
    stops_today = 0
    max_stops_kill_active = False
    max_stops_kills_count = 0
    days_stopped_by_max_stops: set[str] = set()
    # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    slip_entry_sum = 0.0
    slip_entry_count = 0
    slip_entry_max = 0.0
    slip_stop_sum = 0.0
    slip_stop_count = 0
    slip_stop_max = 0.0
    tp_half_spread_sum = 0.0
    tp_half_spread_count = 0
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===

    def size_qty(entry: float, stop: float) -> float:
        nonlocal equity
        R_usdt = equity * bt.risk_pct_per_trade
        dist = abs(entry - stop)
        if dist <= 0:
            return 0.0
        qty = R_usdt / dist
        # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
        notional = qty * entry
        if bt.max_notional_usdt > 0 and notional > bt.max_notional_usdt:
            qty = bt.max_notional_usdt / entry
        notional = qty * entry
        # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===
        return qty

    def record_equity(ts: pd.Timestamp, note: str = ""):
        equity_curve.append({"ts": ts, "equity_usdt": equity, "note": note})

    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    def _atr_pct_for_row(row: pd.Series) -> float:
        atr_val = row.get("atr_pct_proxy")
        if atr_val is None or not np.isfinite(float(atr_val)):
            atr_val = row.get("atr_pct")
        try:
            atr_val_f = float(atr_val)
        except Exception:
            atr_val_f = 0.0
        return atr_val_f if np.isfinite(atr_val_f) else 0.0

    def _slip_bps(kind: Literal["entry", "stop", "tp_half_spread"], notional: float, atr_pct: float) -> float:
        return slippage_bps(
            kind,
            notional,
            atr_pct,
            model=bt.slip_model,
            base_entry=bt.slippage_bps_entry,
            base_stop=bt.slippage_bps_stop,
            base_tp_half_spread=bt.tp_bidask_half_spread_bps,
            atr_ref_pct=bt.slip_atr_ref_pct,
            notional_ref=bt.slip_notional_ref,
            k_size=bt.slip_k_size,
            k_atr_entry=bt.slip_k_atr_entry,
            k_atr_stop=bt.slip_k_atr_stop,
            k_atr_tp=bt.slip_k_atr_tp,
        )

    def _tp_half_spread_for_pos(pos_dict: Dict[str, Any]) -> Optional[float]:
        if not bt.enable_tp_bidask_model:
            return None
        val = pos_dict.get("tp2_half_spread_bps") or pos_dict.get("tp1_half_spread_bps")
        if val is None:
            return float(bt.tp_bidask_half_spread_bps)
        return float(val)
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===

    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    def _apply_funding_if_due(pos_dict: Dict[str, Any], ts_now: pd.Timestamp) -> None:
        nonlocal equity, total_funding_cost_usdt
        if not bt.enable_funding_cost:
            return
        if bt.funding_interval_hours <= 0:
            return
        entry_ts = pos_dict.get("entry_ts")
        if entry_ts is None:
            return
        holding_hours = (ts_now - entry_ts).total_seconds() / 3600.0
        if holding_hours < 0:
            return
        intervals_elapsed = int(holding_hours // bt.funding_interval_hours)
        applied = int(pos_dict.get("funding_intervals_applied", 0))
        if intervals_elapsed <= applied:
            return
        qty_rem = float(pos_dict.get("qty_remaining", 0.0))
        entry_fill = float(pos_dict.get("entry_fill", 0.0))
        notional = abs(qty_rem * entry_fill)
        for _ in range(intervals_elapsed - applied):
            funding_cost = notional * bt.funding_rate_8h
            equity -= funding_cost
            total_funding_cost_usdt += funding_cost
            pos_dict["funding_cost_usdt"] = float(pos_dict.get("funding_cost_usdt", 0.0)) + funding_cost
            pos_dict["funding_intervals_applied"] = int(pos_dict.get("funding_intervals_applied", 0)) + 1
            record_equity(ts_now, "FUNDING")

    def _apply_min_funding_on_exit(pos_dict: Dict[str, Any], ts_now: pd.Timestamp) -> None:
        nonlocal equity, total_funding_cost_usdt
        if not bt.enable_funding_cost:
            return
        if bt.funding_interval_hours <= 0:
            return
        if int(pos_dict.get("funding_intervals_applied", 0)) > 0:
            return
        qty_rem = float(pos_dict.get("qty_remaining", 0.0))
        entry_fill = float(pos_dict.get("entry_fill", 0.0))
        notional = abs(qty_rem * entry_fill)
        funding_cost = notional * bt.funding_rate_8h
        equity -= funding_cost
        total_funding_cost_usdt += funding_cost
        pos_dict["funding_cost_usdt"] = float(pos_dict.get("funding_cost_usdt", 0.0)) + funding_cost
        pos_dict["funding_intervals_applied"] = int(pos_dict.get("funding_intervals_applied", 0)) + 1
        record_equity(ts_now, "FUNDING")
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===

    # helper to build pending from a signal row
    def build_pending(row: pd.Series) -> Optional[Dict[str, Any]]:
        sig = str(row.get("signal", "")).upper().strip()
        if sig not in ("LONG", "SHORT"):
            return None
        if sig == "LONG":
            entry = float(row["entry_long"])
            stop = float(row["stop_long"])
            tp1 = float(row["tp1_long"])
            tp2 = float(row["tp2_long"])
        else:
            entry = float(row["entry_short"])
            stop = float(row["stop_short"])
            tp1 = float(row["tp1_short"])
            tp2 = float(row["tp2_short"])

        # sanity
        if any(np.isnan([entry, stop, tp1, tp2])):
            return None

        qty = size_qty(entry, stop)
        if qty <= 0:
            return None

        return {
            "side": sig,
            "signal_ts": row["ts"],
            "entry": entry,
            "stop": stop,
            "tp1": tp1,
            "tp2": tp2,
            "qty": qty,
            "took_tp1": False,
            "qty_remaining": qty,
            "entry_ts": None,
            "entry_fill": None,
            "stop_active": stop,
            "realized_pnl": 0.0,
            "fees_usdt": 0.0,
            "tp1_fee_usdt": 0.0,
            "tp1_pnl_gross_usdt": 0.0,
            # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
            "funding_cost_usdt": 0.0,
            "funding_intervals_applied": 0,
            "tp1_fill_price": None,
            "tp2_fill_price": None,
            # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
        }

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]  # señal se decide al cierre de prev
        row = df.iloc[i]       # ejecución en la barra siguiente
        ts = row["ts"]
        # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
        if pos is not None:
            _apply_funding_if_due(pos, ts)
        # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===

        # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
        if strat_cfg.enable_daily_dd_kill:
            day_key = _daily_key(ts, strat_cfg.daily_dd_timezone)
            if dd_state.day_key != day_key:
                dd_state.day_key = day_key
                dd_state.equity_day_start = float(equity)
                dd_state.kill_active = False
            if dd_state.equity_day_start:
                dd_day_pct = (float(equity) / dd_state.equity_day_start) - 1.0
            else:
                dd_day_pct = 0.0
            max_daily_dd_pct_observed = min(max_daily_dd_pct_observed, dd_day_pct)

            if dd_day_pct <= -float(strat_cfg.daily_dd_limit_pct):
                if not dd_state.kill_active:
                    daily_kills_count += 1
                    days_killed.add(day_key)
                dd_state.kill_active = True

                if strat_cfg.daily_dd_mode == "hard" and pos is not None:
                    side: BTSide = pos["side"]
                    entry_fill = float(pos["entry_fill"])
                    qty_total = float(pos["qty"])
                    qty_rem = float(pos["qty_remaining"])
                    realized_pnl = float(pos.get("realized_pnl", 0.0))
                    fees_usdt = float(pos.get("fees_usdt", 0.0))
                    tp1_fee_usdt = float(pos.get("tp1_fee_usdt", 0.0))
                    tp1_pnl_gross_usdt = float(pos.get("tp1_pnl_gross_usdt", 0.0))
                    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    _apply_min_funding_on_exit(pos, ts)
                    funding_cost_usdt = float(pos.get("funding_cost_usdt", 0.0))
                    holding_hours = (ts - pos["entry_ts"]).total_seconds() / 3600.0
                    tp_fill_price = pos.get("tp2_fill_price") or pos.get("tp1_fill_price")
                    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    tp_bidask_half_spread_bps = _tp_half_spread_for_pos(pos)
                    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===

                    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    atr_pct = _atr_pct_for_row(row)
                    notional = abs(qty_rem * float(row["close"]))
                    slip_bps_stop = _slip_bps("stop", notional, atr_pct)
                    exit_px = _apply_slip(float(row["close"]), side, False, slip_bps_stop)
                    slip_stop_sum += slip_bps_stop
                    slip_stop_count += 1
                    slip_stop_max = max(slip_stop_max, slip_bps_stop)
                    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    exit_fee = _fee_cost(qty_rem * exit_px, bt.fee_taker)
                    equity -= exit_fee

                    if side == "LONG":
                        pnl = (exit_px - entry_fill) * qty_rem
                        R = (entry_fill - pos["stop"])
                    else:
                        pnl = (entry_fill - exit_px) * qty_rem
                        R = (pos["stop"] - entry_fill)
                    total_pnl = realized_pnl + pnl
                    pnl_R = total_pnl / (R * qty_total) if (R * qty_total) > 0 else 0.0
                    total_fees = fees_usdt + tp1_fee_usdt + exit_fee
                    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    pnl_net = total_pnl - total_fees - funding_cost_usdt
                    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    equity += pnl
                    trades.append(TradeRec(
                        side=side,
                        signal_ts=pos["signal_ts"],
                        entry_ts=pos["entry_ts"],
                        exit_ts=ts,
                        entry=entry_fill,
                        exit=exit_px,
                        stop=pos["stop"],
                        tp1=pos["tp1"],
                        tp2=pos["tp2"],
                        qty=qty_total,
                        pnl_gross_usdt=total_pnl,
                        pnl_net_usdt=pnl_net,
                        fees_usdt=total_fees,
                        tp1_pnl_gross_usdt=tp1_pnl_gross_usdt,
                        tp1_fee_usdt=tp1_fee_usdt,
                        pnl_R=pnl_R,
                        exit_reason="DAILY_DD_KILL",
                        took_tp1=bool(pos["took_tp1"]),
                        # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                        funding_cost_usdt=funding_cost_usdt,
                        holding_hours=holding_hours,
                        tp_fill_price=tp_fill_price,
                        tp_bidask_half_spread_bps=tp_bidask_half_spread_bps,
                        # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    ))
                    record_equity(ts, "DAILY_DD_KILL")
                    pos = None
                    pending = None
                    continue

            if dd_state.kill_active:
                pending = None
        # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
        if strat_cfg.enable_max_stops_kill:
            day_key = _daily_key(ts, strat_cfg.max_stops_timezone)
            if max_stops_day_key != day_key:
                max_stops_day_key = day_key
                stops_today = 0
                max_stops_kill_active = False
            if max_stops_kill_active and strat_cfg.max_stops_mode == "hard" and pos is not None:
                side: BTSide = pos["side"]
                entry_fill = float(pos["entry_fill"])
                qty_total = float(pos["qty"])
                qty_rem = float(pos["qty_remaining"])
                realized_pnl = float(pos.get("realized_pnl", 0.0))
                fees_usdt = float(pos.get("fees_usdt", 0.0))
                tp1_fee_usdt = float(pos.get("tp1_fee_usdt", 0.0))
                tp1_pnl_gross_usdt = float(pos.get("tp1_pnl_gross_usdt", 0.0))
                # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                _apply_min_funding_on_exit(pos, ts)
                funding_cost_usdt = float(pos.get("funding_cost_usdt", 0.0))
                holding_hours = (ts - pos["entry_ts"]).total_seconds() / 3600.0
                tp_fill_price = pos.get("tp2_fill_price") or pos.get("tp1_fill_price")
                # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                tp_bidask_half_spread_bps = _tp_half_spread_for_pos(pos)
                # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===

                # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                atr_pct = _atr_pct_for_row(row)
                notional = abs(qty_rem * float(row["close"]))
                slip_bps_stop = _slip_bps("stop", notional, atr_pct)
                exit_px = _apply_slip(float(row["close"]), side, False, slip_bps_stop)
                slip_stop_sum += slip_bps_stop
                slip_stop_count += 1
                slip_stop_max = max(slip_stop_max, slip_bps_stop)
                # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                exit_fee = _fee_cost(qty_rem * exit_px, bt.fee_taker)
                equity -= exit_fee

                if side == "LONG":
                    pnl = (exit_px - entry_fill) * qty_rem
                    R = (entry_fill - pos["stop"])
                else:
                    pnl = (entry_fill - exit_px) * qty_rem
                    R = (pos["stop"] - entry_fill)
                total_pnl = realized_pnl + pnl
                pnl_R = total_pnl / (R * qty_total) if (R * qty_total) > 0 else 0.0
                total_fees = fees_usdt + tp1_fee_usdt + exit_fee
                # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                pnl_net = total_pnl - total_fees - funding_cost_usdt
                # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                equity += pnl
                trades.append(TradeRec(
                    side=side,
                    signal_ts=pos["signal_ts"],
                    entry_ts=pos["entry_ts"],
                    exit_ts=ts,
                    entry=entry_fill,
                    exit=exit_px,
                    stop=pos["stop"],
                    tp1=pos["tp1"],
                    tp2=pos["tp2"],
                    qty=qty_total,
                    pnl_gross_usdt=total_pnl,
                    pnl_net_usdt=pnl_net,
                    fees_usdt=total_fees,
                    tp1_pnl_gross_usdt=tp1_pnl_gross_usdt,
                    tp1_fee_usdt=tp1_fee_usdt,
                    pnl_R=pnl_R,
                    exit_reason="MAX_STOPS_KILL",
                    took_tp1=bool(pos["took_tp1"]),
                    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    funding_cost_usdt=funding_cost_usdt,
                    holding_hours=holding_hours,
                    tp_fill_price=tp_fill_price,
                    tp_bidask_half_spread_bps=tp_bidask_half_spread_bps,
                    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                ))
                record_equity(ts, "MAX_STOPS_KILL")
                pos = None
                pending = None
                continue

            if max_stops_kill_active:
                pending = None
        # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===

        # 1) si no hay posición ni pendiente, tomamos nueva señal del cierre anterior
        # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
        allow_new_entries = (not strat_cfg.enable_daily_dd_kill or not dd_state.kill_active)
        if strat_cfg.enable_max_stops_kill:
            allow_new_entries = allow_new_entries and not max_stops_kill_active
        if allow_new_entries and pos is None and pending is None:
        # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
            p = build_pending(prev)
            if p is not None:
                pending = p
                pending_age = 0

        # 2) manejar pendiente (activar entrada o cancelar por timeout)
        if pending is not None and pos is None:
            pending_age += 1
            if pending_age > bt.entry_timeout_bars:
                pending = None
                pending_age = 0
            else:
                # activación de stop-entry
                side = pending["side"]
                entry_lvl = pending["entry"]

                if side == "LONG":
                    triggered = row["high"] >= entry_lvl
                else:
                    triggered = row["low"] <= entry_lvl

                if triggered:
                    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    atr_pct = _atr_pct_for_row(row)
                    notional = abs(pending["qty"] * entry_lvl)
                    slip_bps_entry = _slip_bps("entry", notional, atr_pct)
                    fill = _apply_slip(entry_lvl, side, True, slip_bps_entry)
                    slip_entry_sum += slip_bps_entry
                    slip_entry_count += 1
                    slip_entry_max = max(slip_entry_max, slip_bps_entry)
                    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    qty = pending["qty"]
                    # fee entry
                    entry_fee = _fee_cost(qty * fill, bt.fee_taker)
                    equity -= entry_fee
                    pending["fees_usdt"] = entry_fee

                    pending["entry_ts"] = ts
                    pending["entry_fill"] = fill
                    pending["qty_remaining"] = qty
                    pending["stop_active"] = pending["stop"]  # inicial
                    pos = pending
                    pending = None
                    pending_age = 0

        # 3) manejar posición (SL/TP intrabar)
        if pos is not None:
            side: BTSide = pos["side"]
            entry_fill = float(pos["entry_fill"])
            stop_lvl = float(pos["stop_active"])
            tp1 = float(pos["tp1"])
            tp2 = float(pos["tp2"])
            qty_total = float(pos["qty"])
            qty_rem = float(pos["qty_remaining"])
            took_tp1 = bool(pos["took_tp1"])
            realized_pnl = float(pos.get("realized_pnl", 0.0))
            fees_usdt = float(pos.get("fees_usdt", 0.0))
            tp1_fee_usdt = float(pos.get("tp1_fee_usdt", 0.0))
            tp1_pnl_gross_usdt = float(pos.get("tp1_pnl_gross_usdt", 0.0))

            # definir chequeos por lado
            if side == "LONG":
                hit_stop = row["low"] <= stop_lvl
                hit_tp1 = (not took_tp1) and (row["high"] >= tp1)
                hit_tp2 = row["high"] >= tp2
                # Si en misma barra toca stop y tp -> conservador: stop primero
                if bt.conservative_intrabar and hit_stop and (hit_tp1 or hit_tp2):
                    hit_tp1 = False
                    hit_tp2 = False
            else:
                hit_stop = row["high"] >= stop_lvl
                hit_tp1 = (not took_tp1) and (row["low"] <= tp1)
                hit_tp2 = row["low"] <= tp2
                if bt.conservative_intrabar and hit_stop and (hit_tp1 or hit_tp2):
                    hit_tp1 = False
                    hit_tp2 = False

            # 3a) STOP
            if hit_stop:
                # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                _apply_min_funding_on_exit(pos, ts)
                funding_cost_usdt = float(pos.get("funding_cost_usdt", 0.0))
                holding_hours = (ts - pos["entry_ts"]).total_seconds() / 3600.0
                tp_fill_price = pos.get("tp2_fill_price") or pos.get("tp1_fill_price")
                # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                tp_bidask_half_spread_bps = _tp_half_spread_for_pos(pos)
                # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                atr_pct = _atr_pct_for_row(row)
                notional = abs(qty_rem * stop_lvl)
                slip_bps_stop = _slip_bps("stop", notional, atr_pct)
                exit_px = _apply_slip(stop_lvl, side, False, slip_bps_stop)
                slip_stop_sum += slip_bps_stop
                slip_stop_count += 1
                slip_stop_max = max(slip_stop_max, slip_bps_stop)
                # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                # fee exit
                exit_fee = _fee_cost(qty_rem * exit_px, bt.fee_taker)
                equity -= exit_fee

                # pnl
                if side == "LONG":
                    pnl = (exit_px - entry_fill) * qty_rem
                    R = (entry_fill - pos["stop"])  # stop original para R
                    total_pnl = realized_pnl + pnl
                    pnl_R = total_pnl / (R * qty_total) if (R * qty_total) > 0 else 0.0
                else:
                    pnl = (entry_fill - exit_px) * qty_rem
                    R = (pos["stop"] - entry_fill)
                    total_pnl = realized_pnl + pnl
                    pnl_R = total_pnl / (R * qty_total) if (R * qty_total) > 0 else 0.0

                total_pnl = realized_pnl + pnl
                total_fees = fees_usdt + tp1_fee_usdt + exit_fee
                # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                pnl_net = total_pnl - total_fees - funding_cost_usdt
                # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                equity += pnl
                trades.append(TradeRec(
                    side=side,
                    signal_ts=pos["signal_ts"],
                    entry_ts=pos["entry_ts"],
                    exit_ts=ts,
                    entry=entry_fill,
                    exit=exit_px,
                    stop=pos["stop"],
                    tp1=tp1,
                    tp2=tp2,
                    qty=qty_total,
                    pnl_gross_usdt=total_pnl,
                    pnl_net_usdt=pnl_net,
                    fees_usdt=total_fees,
                    tp1_pnl_gross_usdt=tp1_pnl_gross_usdt,
                    tp1_fee_usdt=tp1_fee_usdt,
                    pnl_R=pnl_R,
                    exit_reason="STOP",
                    took_tp1=took_tp1,
                    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    funding_cost_usdt=funding_cost_usdt,
                    holding_hours=holding_hours,
                    tp_fill_price=tp_fill_price,
                    tp_bidask_half_spread_bps=tp_bidask_half_spread_bps,
                    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                ))
                # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
                if strat_cfg.enable_max_stops_kill:
                    stops_today += 1
                    if stops_today >= int(strat_cfg.max_stops_per_day):
                        if not max_stops_kill_active:
                            max_stops_kills_count += 1
                            days_stopped_by_max_stops.add(_daily_key(ts, strat_cfg.max_stops_timezone))
                        max_stops_kill_active = True
                        if strat_cfg.max_stops_mode == "hard":
                            pending = None
                # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
                record_equity(ts, "STOP")
                pos = None
                continue

            # 3b) TP1 parcial (50%) y mover stop a BE (simple)
            if hit_tp1:
                # cerrar 50% del qty_total
                close_qty = qty_total * 0.5
                close_qty = min(close_qty, qty_rem)
                # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                atr_pct = _atr_pct_for_row(row)
                notional = abs(close_qty * tp1)
                tp_half_spread_bps = (
                    _slip_bps("tp_half_spread", notional, atr_pct) if bt.enable_tp_bidask_model else None
                )
                exit_px = _tp_fill_price(tp1, side, bt, tp_half_spread_bps=tp_half_spread_bps)
                pos["tp1_fill_price"] = exit_px
                pos["tp1_half_spread_bps"] = tp_half_spread_bps
                if tp_half_spread_bps is not None:
                    tp_half_spread_sum += float(tp_half_spread_bps)
                    tp_half_spread_count += 1
                # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                tp1_fee = _fee_cost(close_qty * exit_px, bt.fee_maker)
                equity -= tp1_fee

                if side == "LONG":
                    pnl = (exit_px - entry_fill) * close_qty
                else:
                    pnl = (entry_fill - exit_px) * close_qty
                equity += pnl
                pos["realized_pnl"] = realized_pnl + pnl
                pos["tp1_fee_usdt"] = tp1_fee_usdt + tp1_fee
                pos["tp1_pnl_gross_usdt"] = tp1_pnl_gross_usdt + pnl

                qty_rem -= close_qty
                pos["qty_remaining"] = qty_rem
                pos["took_tp1"] = True

                # mover stop a BE (1bp pad implícito por slip/fees ya están modelados)
                if side == "LONG":
                    pos["stop_active"] = entry_fill  # BE
                else:
                    pos["stop_active"] = entry_fill  # BE

                record_equity(ts, "TP1")

            # 3c) TP2 final (si lo toca)
            # Nota: si se ejecutó TP1, aún puede ejecutar TP2 en la misma barra (se permite).
            if pos is not None:
                # recompute after tp1
                qty_rem = float(pos["qty_remaining"])
                took_tp1 = bool(pos["took_tp1"])
                stop_lvl = float(pos["stop_active"])
                realized_pnl = float(pos.get("realized_pnl", 0.0))
                tp1_fee_usdt = float(pos.get("tp1_fee_usdt", 0.0))
                tp1_pnl_gross_usdt = float(pos.get("tp1_pnl_gross_usdt", 0.0))

                if side == "LONG":
                    hit_tp2 = row["high"] >= tp2
                else:
                    hit_tp2 = row["low"] <= tp2

                if hit_tp2 and qty_rem > 0:
                    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    atr_pct = _atr_pct_for_row(row)
                    notional = abs(qty_rem * tp2)
                    tp_half_spread_bps = (
                        _slip_bps("tp_half_spread", notional, atr_pct) if bt.enable_tp_bidask_model else None
                    )
                    exit_px = _tp_fill_price(tp2, side, bt, tp_half_spread_bps=tp_half_spread_bps)
                    pos["tp2_fill_price"] = exit_px
                    pos["tp2_half_spread_bps"] = tp_half_spread_bps
                    if tp_half_spread_bps is not None:
                        tp_half_spread_sum += float(tp_half_spread_bps)
                        tp_half_spread_count += 1
                    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    exit_fee = _fee_cost(qty_rem * exit_px, bt.fee_maker)
                    equity -= exit_fee

                    if side == "LONG":
                        pnl = (exit_px - entry_fill) * qty_rem
                        R = (entry_fill - pos["stop"])
                        total_pnl = realized_pnl + pnl
                        pnl_R = total_pnl / (R * qty_total) if (R * qty_total) > 0 else 0.0
                    else:
                        pnl = (entry_fill - exit_px) * qty_rem
                        R = (pos["stop"] - entry_fill)
                        total_pnl = realized_pnl + pnl
                        pnl_R = total_pnl / (R * qty_total) if (R * qty_total) > 0 else 0.0

                    total_pnl = realized_pnl + pnl
                    total_fees = fees_usdt + tp1_fee_usdt + exit_fee
                    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    _apply_min_funding_on_exit(pos, ts)
                    funding_cost_usdt = float(pos.get("funding_cost_usdt", 0.0))
                    holding_hours = (ts - pos["entry_ts"]).total_seconds() / 3600.0
                    tp_fill_price = pos.get("tp2_fill_price") or pos.get("tp1_fill_price")
                    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    tp_bidask_half_spread_bps = _tp_half_spread_for_pos(pos)
                    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
                    pnl_net = total_pnl - total_fees - funding_cost_usdt
                    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    equity += pnl
                    trades.append(TradeRec(
                        side=side,
                        signal_ts=pos["signal_ts"],
                        entry_ts=pos["entry_ts"],
                        exit_ts=ts,
                        entry=entry_fill,
                        exit=exit_px,
                        stop=pos["stop"],
                        tp1=tp1,
                        tp2=tp2,
                        qty=qty_total,
                        pnl_gross_usdt=total_pnl,
                        pnl_net_usdt=pnl_net,
                        fees_usdt=total_fees,
                        tp1_pnl_gross_usdt=tp1_pnl_gross_usdt,
                        tp1_fee_usdt=tp1_fee_usdt,
                        pnl_R=pnl_R,
                        exit_reason="TP2",
                        took_tp1=took_tp1,
                        # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                        funding_cost_usdt=funding_cost_usdt,
                        holding_hours=holding_hours,
                        tp_fill_price=tp_fill_price,
                        tp_bidask_half_spread_bps=tp_bidask_half_spread_bps,
                        # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
                    ))
                    record_equity(ts, "TP2")
                    pos = None
                    continue

        # record curve each bar (optional)
        if i % 50 == 0:
            record_equity(ts, "")

    trades_df = pd.DataFrame([trade_rec_to_dict(t) for t in trades], columns=TRADE_COLUMNS)
    eq_df = pd.DataFrame(equity_curve, columns=EQUITY_COLUMNS)
    if not eq_df.empty:
        eq_df = eq_df.sort_values("ts").reset_index(drop=True)
    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    daily_dd_stats = {
        "daily_kills_count": int(daily_kills_count),
        "max_daily_dd_pct_observed": float(max_daily_dd_pct_observed),
        "days_killed": sorted(days_killed),
    }
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    max_stops_stats = {
        "max_stops_kills_count": int(max_stops_kills_count),
        "days_stopped_by_max_stops": sorted(days_stopped_by_max_stops),
    }
    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    slip_stats = {
        "avg_slip_bps_entry": float(slip_entry_sum / slip_entry_count) if slip_entry_count else 0.0,
        "avg_slip_bps_stop": float(slip_stop_sum / slip_stop_count) if slip_stop_count else 0.0,
        "avg_tp_half_spread_bps": (
            float(tp_half_spread_sum / tp_half_spread_count) if tp_half_spread_count else 0.0
        ),
        "max_slip_bps_entry": float(slip_entry_max),
        "max_slip_bps_stop": float(slip_stop_max),
    }
    if return_state:
        open_position = 1 if pos is not None else 0
        open_position_info = None
        if pos is not None:
            entry_fill = pos.get("entry_fill")
            entry_val = entry_fill if entry_fill is not None else pos.get("entry")
            open_position_info = {
                "side": pos.get("side"),
                "signal_ts": pos.get("signal_ts"),
                "entry_ts": pos.get("entry_ts"),
                "entry": entry_val,
                "stop": pos.get("stop"),
                "tp1": pos.get("tp1"),
                "tp2": pos.get("tp2"),
            }
        state = {"open_position": open_position, "open_position_info": open_position_info}
        return trades_df, eq_df, float(equity), daily_dd_stats, max_stops_stats, slip_stats, state
    return trades_df, eq_df, float(equity), daily_dd_stats, max_stops_stats, slip_stats
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===


def compute_metrics(
    trades_df: pd.DataFrame,
    eq_df: pd.DataFrame,
    initial_equity: float,
    final_equity: float,
    start_ts: Optional[pd.Timestamp],
    end_ts: Optional[pd.Timestamp],
) -> Dict[str, Any]:
    def build_breakdown(period: str) -> Dict[str, Dict[str, Any]]:
        if trades_df is None:
            trades = pd.DataFrame()
        else:
            trades = trades_df.copy()
        if eq_df is None:
            eq = pd.DataFrame()
        else:
            eq = eq_df.copy()

        trade_periods: List[str] = []
        eq_periods: List[str] = []
        if not trades.empty:
            trades["period"] = pd.to_datetime(trades["exit_ts"]).dt.to_period(period).astype(str)
            trade_periods = trades["period"].unique().tolist()
        if not eq.empty and "ts" in eq.columns:
            eq["period"] = pd.to_datetime(eq["ts"]).dt.to_period(period).astype(str)
            eq_periods = eq["period"].unique().tolist()

        periods = sorted(set(trade_periods) | set(eq_periods))
        breakdown: Dict[str, Dict[str, Any]] = {}

        for key in periods:
            t = trades[trades["period"] == key] if not trades.empty else trades
            e = eq[eq["period"] == key] if not eq.empty else eq
            trades_count = int(len(t))

            if trades_count > 0:
                winrate = float((t["pnl_net_usdt"] > 0).mean())
                gross_profit = float(t.loc[t["pnl_net_usdt"] > 0, "pnl_net_usdt"].sum())
                gross_loss = float(-t.loc[t["pnl_net_usdt"] < 0, "pnl_net_usdt"].sum())
                profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else np.inf
                sum_pnl_net_usdt = float(t["pnl_net_usdt"].sum())
            else:
                winrate = 0.0
                profit_factor = None
                sum_pnl_net_usdt = 0.0

            return_pct = None
            max_dd_pct = None
            if not e.empty and "equity_usdt" in e.columns:
                eq_vals = e["equity_usdt"].astype(float).values
                if len(eq_vals) >= 1:
                    start_eq = eq_vals[0]
                    end_eq = eq_vals[-1]
                    if start_eq:
                        return_pct = float(((end_eq / start_eq) - 1.0) * 100.0)
                    peak = np.maximum.accumulate(eq_vals)
                    dd = (eq_vals - peak) / peak
                    max_dd_pct = float(abs(dd.min()) * 100.0) if len(dd) else None

            breakdown[key] = {
                "trades": trades_count,
                "winrate": winrate,
                "profit_factor": float(profit_factor) if profit_factor is not None else None,
                "sum_pnl_net_usdt": sum_pnl_net_usdt,
                "max_drawdown_pct": max_dd_pct,
                "return_pct": return_pct,
            }

        return breakdown

    breakdown_by_year = build_breakdown("Y")
    breakdown_by_month = build_breakdown("M")
    if trades_df is None or trades_df.empty:
        final_equity = float(final_equity)
        ret = (final_equity / initial_equity) - 1.0 if initial_equity else 0.0
        return {
            "trades": 0,
            "winrate": 0.0,
            "profit_factor": None,
            "avg_R": 0.0,
            "sum_R": 0.0,
            "max_drawdown_pct": None,
            "final_equity_usdt": final_equity,
            "return_pct": float(ret * 100.0),
            "trades_per_month_avg": 0.0,
            "dataset_start": start_ts.isoformat() if start_ts is not None else None,
            "dataset_end": end_ts.isoformat() if end_ts is not None else None,
            "by_year": breakdown_by_year,
            "by_month": breakdown_by_month,
        }

    wins = trades_df["pnl_net_usdt"] > 0
    winrate = float(wins.mean())

    gross_profit = float(trades_df.loc[trades_df["pnl_net_usdt"] > 0, "pnl_net_usdt"].sum())
    gross_loss = float(-trades_df.loc[trades_df["pnl_net_usdt"] < 0, "pnl_net_usdt"].sum())
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else np.inf

    avg_R = float(trades_df["pnl_R"].mean())
    sum_R = float(trades_df["pnl_R"].sum())
    avg_usdt = float(trades_df["pnl_net_usdt"].mean())
    sum_usdt = float(trades_df["pnl_net_usdt"].sum())

    # Max DD from equity curve
    max_dd = None
    if eq_df is not None and not eq_df.empty and "equity_usdt" in eq_df.columns:
        e = eq_df["equity_usdt"].astype(float).values
        peak = np.maximum.accumulate(e)
        dd = (e - peak) / peak
        max_dd = float(dd.min())

    # Trades per month (rough)
    trades_month = pd.to_datetime(trades_df["entry_ts"]).dt.to_period("M").astype(str)
    tpm = (
        float(trades_df.assign(month=trades_month).groupby("month").size().mean())
        if trades_month.nunique() > 0
        else float(len(trades_df))
    )

    final_equity = float(final_equity)
    ret = (final_equity / initial_equity) - 1.0

    return {
        "trades": int(len(trades_df)),
        "winrate": winrate,
        "profit_factor": float(profit_factor),
        "avg_R": avg_R,
        "sum_R": sum_R,
        "avg_pnl_usdt": avg_usdt,
        "sum_pnl_usdt": sum_usdt,
        "final_equity_usdt": float(final_equity),
        "return_pct": float(ret * 100.0),
        "max_drawdown_pct": float(abs(max_dd) * 100.0) if max_dd is not None else None,
        "trades_per_month_avg": tpm,
        "dataset_start": start_ts.isoformat() if start_ts is not None else None,
        "dataset_end": end_ts.isoformat() if end_ts is not None else None,
        "by_year": breakdown_by_year,
        "by_month": breakdown_by_month,
    }


def run_backtest(
    csv_path: str,
    fee_taker: float,
    fee_maker: float,
    slip_bps: float,
    slip_bps_entry: float,
    slip_bps_tp: float,
    slip_bps_stop: float,
    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    slip_model: str,
    slip_atr_ref_pct: float,
    slip_notional_ref: float,
    slip_k_size: float,
    slip_k_atr_entry: float,
    slip_k_atr_stop: float,
    slip_k_atr_tp: float,
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    equity0: float,
    risk_pct: float,
    # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
    max_notional_usdt: float,
    # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===
    outdir: str,
    strict: bool,
    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    enable_funding_cost: bool,
    funding_rate_8h: float,
    enable_tp_bidask_model: bool,
    tp_bidask_half_spread_bps: float,
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    vol_filter_monthly: bool,
    monthly_atr_q: float,
    daily_atr_len: int,
    monthly_atr_floor: float,
    # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    daily_dd_kill: bool,
    daily_dd_limit: float,
    daily_dd_mode: str,
    daily_dd_timezone: str,
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    max_stops_kill: bool,
    max_stops_per_day: int,
    max_stops_mode: str,
    max_stops_timezone: str,
    params: Optional[Dict[str, Any]] = None,
    # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
) -> None:
    # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    strat_cfg = StrategyConfig(
        symbol="SOLUSDT",
        # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
        enable_funding_cost=enable_funding_cost,
        funding_rate_8h=funding_rate_8h,
        enable_tp_bidask_model=enable_tp_bidask_model,
        tp_bidask_half_spread_bps=tp_bidask_half_spread_bps,
        # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
        enable_monthly_vol_filter=vol_filter_monthly,
        daily_atr_len=daily_atr_len,
        monthly_atr_pct_quantile=monthly_atr_q,
        monthly_atr_pct_floor=monthly_atr_floor,
        # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
        enable_daily_dd_kill=daily_dd_kill,
        daily_dd_limit_pct=daily_dd_limit,
        daily_dd_mode=daily_dd_mode,
        daily_dd_timezone=daily_dd_timezone,
        # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
        enable_max_stops_kill=max_stops_kill,
        max_stops_per_day=max_stops_per_day,
        max_stops_mode=max_stops_mode,
        max_stops_timezone=max_stops_timezone,
        # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    )
    apply_overrides(strat_cfg, params or {})
    # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    df = load_ohlcv_csv(csv_path, strict)
    if df.empty:
        _handle_issue("No data available for backtest.", strict)
        df = df if not df.empty else pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    atr_pct_proxy = compute_atr_pct_proxy(df, n=strat_cfg.daily_atr_len) if not df.empty else pd.Series()
    if not df.empty and len(atr_pct_proxy) == len(df):
        df = df.copy()
        df["atr_pct_proxy"] = atr_pct_proxy.values
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    sigdf = generate_signals(df, strat_cfg) if not df.empty else df
    start_ts = df["ts"].min() if not df.empty else None
    end_ts = df["ts"].max() if not df.empty else None

    tp_is_maker = True
    slip_bps_tp_effective = 0.0 if tp_is_maker else slip_bps_tp
    bt = BTConfig(
        fee_taker=fee_taker,
        fee_maker=fee_maker,
        slippage_bps_entry=slip_bps_entry,
        slippage_bps_tp=slip_bps_tp_effective,
        slippage_bps_stop=slip_bps_stop,
        # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
        slip_model=slip_model,
        slip_atr_ref_pct=slip_atr_ref_pct,
        slip_notional_ref=slip_notional_ref,
        slip_k_size=slip_k_size,
        slip_k_atr_entry=slip_k_atr_entry,
        slip_k_atr_stop=slip_k_atr_stop,
        slip_k_atr_tp=slip_k_atr_tp,
        # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
        tp_is_maker=tp_is_maker,
        initial_equity_usdt=equity0,
        risk_pct_per_trade=risk_pct,
        # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
        max_notional_usdt=max_notional_usdt,
        # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===
        entry_timeout_bars=strat_cfg.entry_timeout_bars,
        conservative_intrabar=True,
        # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
        enable_funding_cost=enable_funding_cost,
        funding_rate_8h=funding_rate_8h,
        funding_interval_hours=strat_cfg.funding_interval_hours,
        enable_tp_bidask_model=enable_tp_bidask_model,
        tp_bidask_half_spread_bps=tp_bidask_half_spread_bps,
        # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    )
    apply_overrides(bt, params or {})

    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    trades_df, eq_df, final_equity, daily_dd_stats, max_stops_stats, slip_stats = (
        backtest_from_signals(sigdf, bt, strat_cfg, atr_pct_proxy=atr_pct_proxy)
        if not df.empty
        else (
            pd.DataFrame(),
            pd.DataFrame(),
            equity0,
            {"daily_kills_count": 0, "max_daily_dd_pct_observed": 0.0, "days_killed": []},
            {"max_stops_kills_count": 0, "days_stopped_by_max_stops": []},
            {
                "avg_slip_bps_entry": 0.0,
                "avg_slip_bps_stop": 0.0,
                "avg_tp_half_spread_bps": 0.0,
                "max_slip_bps_entry": 0.0,
                "max_slip_bps_stop": 0.0,
            },
        )
    )
    # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_STOPS_TOGGLE_FIX (2026-02-02) ===
    if not max_stops_kill:
        max_stops_stats = {"max_stops_kills_count": 0, "days_stopped_by_max_stops": []}
    # === CODEX_PATCH_END: MAX_STOPS_TOGGLE_FIX (2026-02-02) ===
    metrics = compute_metrics(trades_df, eq_df, equity0, final_equity, start_ts, end_ts)
    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    if trades_df is not None and not trades_df.empty and "funding_cost_usdt" in trades_df.columns:
        total_funding_cost_usdt = float(trades_df["funding_cost_usdt"].sum())
        avg_funding_cost_per_trade = float(trades_df["funding_cost_usdt"].mean())
    else:
        total_funding_cost_usdt = 0.0
        avg_funding_cost_per_trade = 0.0
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    if "vol_ok" in sigdf.columns and len(sigdf) > 0:
        bars_filtered = int((~sigdf["vol_ok"]).sum())
        bars_filtered_pct = float(bars_filtered / len(sigdf))
    else:
        bars_filtered = 0
        bars_filtered_pct = 0.0

    if {"signal_base", "signal"}.issubset(sigdf.columns):
        base_signals = int((sigdf["signal_base"] != "").sum())
        trades_filtered = int(((sigdf["signal_base"] != "") & (sigdf["signal"] == "")).sum())
        trades_filtered_pct = float(trades_filtered / base_signals) if base_signals > 0 else 0.0
    else:
        base_signals = 0
        trades_filtered = 0
        trades_filtered_pct = 0.0
    # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
    if trades_df is not None and not trades_df.empty:
        notionals = (trades_df["qty"].astype(float) * trades_df["entry"].astype(float)).abs()
        max_position_notional_observed = float(notionals.max())
        avg_position_notional = float(notionals.mean())
    else:
        max_position_notional_observed = 0.0
        avg_position_notional = 0.0
    # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===
    metrics.update({
        "fee_taker": float(fee_taker),
        "fee_maker": float(fee_maker),
        "slip_bps": float(slip_bps),
        "slip_bps_entry": float(slip_bps_entry),
        "slip_bps_tp": float(slip_bps_tp_effective),
        "slip_bps_stop": float(slip_bps_stop),
        # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
        "slip_model": str(slip_model),
        "slip_atr_ref_pct": float(slip_atr_ref_pct),
        "slip_notional_ref": float(slip_notional_ref),
        "slip_k_size": float(slip_k_size),
        "slip_k_atr_entry": float(slip_k_atr_entry),
        "slip_k_atr_stop": float(slip_k_atr_stop),
        "slip_k_atr_tp": float(slip_k_atr_tp),
        "avg_slip_bps_entry": float(slip_stats.get("avg_slip_bps_entry", 0.0)),
        "avg_slip_bps_stop": float(slip_stats.get("avg_slip_bps_stop", 0.0)),
        "avg_tp_half_spread_bps": float(slip_stats.get("avg_tp_half_spread_bps", 0.0)),
        "max_slip_bps_entry": float(slip_stats.get("max_slip_bps_entry", 0.0)),
        "max_slip_bps_stop": float(slip_stats.get("max_slip_bps_stop", 0.0)),
        # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
        "risk": float(risk_pct),
        "equity0": float(equity0),
        "outdir": outdir,
        # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
        "max_notional_usdt": float(max_notional_usdt),
        "max_position_notional_observed": float(max_position_notional_observed),
        "avg_position_notional": float(avg_position_notional),
        # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
        "enable_funding_cost": bool(enable_funding_cost),
        "funding_rate_8h": float(funding_rate_8h),
        "total_funding_cost_usdt": float(total_funding_cost_usdt),
        "avg_funding_cost_per_trade": float(avg_funding_cost_per_trade),
        "enable_tp_bidask_model": bool(enable_tp_bidask_model),
        "tp_bidask_half_spread_bps": float(tp_bidask_half_spread_bps),
        # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
        "daily_dd_kill": bool(daily_dd_kill),
        "daily_dd_limit_pct": float(daily_dd_limit),
        "daily_dd_mode": str(daily_dd_mode),
        "daily_dd_timezone": str(daily_dd_timezone),
        "daily_kills_count": int(daily_dd_stats.get("daily_kills_count", 0)),
        "max_daily_dd_pct_observed": float(daily_dd_stats.get("max_daily_dd_pct_observed", 0.0)),
        "days_killed": daily_dd_stats.get("days_killed", []),
        # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
        "max_stops_kill_enabled": bool(max_stops_kill),
        "max_stops_per_day": int(max_stops_per_day),
        "max_stops_mode": str(max_stops_mode),
        "max_stops_kills_count": int(max_stops_stats.get("max_stops_kills_count", 0)),
        "days_stopped_by_max_stops": max_stops_stats.get("days_stopped_by_max_stops", []),
        # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
        # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
        "vol_filter_monthly": bool(vol_filter_monthly),
        "monthly_atr_q": float(monthly_atr_q),
        "daily_atr_len": int(daily_atr_len),
        "monthly_atr_floor": float(monthly_atr_floor),
        "bars_filtered": bars_filtered,
        "bars_filtered_pct": bars_filtered_pct,
        "trades_filtered": trades_filtered,
        "trades_filtered_pct": trades_filtered_pct,
        "base_signals": base_signals,
        # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    })

    print("\n=== METRICS ===")
    for k, v in metrics.items():
        print(f"{k}: {v}")

    print("\n=== LAST TRADES ===")
    if trades_df.empty:
        print("No trades.")
    else:
        cols = ["side", "signal_ts", "entry_ts", "exit_ts", "entry", "exit", "pnl_net_usdt", "pnl_R", "exit_reason", "took_tp1"]
        print(trades_df[cols].tail(15).to_string(index=False))

    # Save artifacts
    outdir = outdir or "./results"
    os.makedirs(outdir, exist_ok=True)

    trades_path = os.path.join(outdir, "trades.csv")
    eq_path = os.path.join(outdir, "equity_curve.csv")
    metrics_path = os.path.join(outdir, "metrics.json")

    trades_df.to_csv(trades_path, index=False)
    eq_df.to_csv(eq_path, index=False)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    print(f"\nSaved: {trades_path}, {eq_path}, {metrics_path}")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    # 1) Backtest:
    #    python solusdt_vwap_bot.py --bt --csv ./SOLUSDT_5m.csv
    #
    # 2) Prueba rápida con CSV (dry-run):
    #    python solusdt_vwap_bot.py --csv ./SOLUSDT_5m.csv
    #
    # 3) Live skeleton:
    #    python solusdt_vwap_bot.py --live

    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default="")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--bt", action="store_true")
    ap.add_argument("--fee", type=float, default=0.0004)
    ap.add_argument("--fee_taker", type=float, default=None)
    ap.add_argument("--fee_maker", type=float, default=None)
    ap.add_argument("--slip_bps", type=float, default=1.0)
    ap.add_argument("--slip_bps_entry", type=float, default=None)
    ap.add_argument("--slip_bps_tp", type=float, default=None)
    ap.add_argument("--slip_bps_stop", type=float, default=None)
    # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    ap.add_argument("--slip_model", type=str, default="fixed", choices=["fixed", "dynamic"])
    ap.add_argument("--slip_atr_ref_pct", type=float, default=0.01)
    ap.add_argument("--slip_notional_ref", type=float, default=10000.0)
    ap.add_argument("--slip_k_size", type=float, default=0.5)
    ap.add_argument("--slip_k_atr_entry", type=float, default=0.75)
    ap.add_argument("--slip_k_atr_stop", type=float, default=1.5)
    ap.add_argument("--slip_k_atr_tp", type=float, default=0.25)
    # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
    ap.add_argument("--equity", type=float, default=1000.0)
    ap.add_argument("--risk", type=float, default=0.0025)
    # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
    ap.add_argument("--max_notional_usdt", type=float, default=0.0)
    # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===
    ap.add_argument("--outdir", type=str, default="./results")
    ap.add_argument("--strict", action="store_true")
    # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    ap.add_argument("--enable_funding_cost", type=int, default=1)
    ap.add_argument("--funding_rate_8h", type=float, default=0.0001)
    ap.add_argument("--enable_tp_bidask", type=int, default=1)
    ap.add_argument("--tp_bidask_half_spread_bps", type=float, default=1.0)
    # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    ap.add_argument("--daily_dd_kill", type=int, default=1)
    ap.add_argument("--daily_dd_limit", type=float, default=0.02)
    ap.add_argument("--daily_dd_mode", type=str, default="soft", choices=["soft", "hard"])
    ap.add_argument("--daily_dd_timezone", type=str, default="UTC")
    # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    ap.add_argument("--vol_filter_monthly", type=int, default=1)
    ap.add_argument("--monthly_atr_q", type=float, default=0.25)
    ap.add_argument("--daily_atr_len", type=int, default=14)
    ap.add_argument("--monthly_atr_floor", type=float, default=0.0)
    # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
    # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    ap.add_argument("--max_stops_kill", type=int, default=1)
    ap.add_argument("--max_stops_per_day", type=int, default=3)
    ap.add_argument("--max_stops_mode", type=str, default="soft", choices=["soft", "hard"])
    ap.add_argument("--max_stops_timezone", type=str, default="UTC")
    # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
    ap.add_argument("--preset", type=str, default="", choices=["", "max_throughput", "strict"])
    ap.add_argument("--cooldown_sec", type=float, default=None)
    ap.add_argument("--max_trades_day", type=int, default=None)
    ap.add_argument("--rr_min", type=float, default=None)
    ap.add_argument("--use_time_filter", type=int, default=None)
    ap.add_argument("--hour_start", type=int, default=None)
    ap.add_argument("--hour_end", type=int, default=None)
    ap.add_argument("--delta_threshold", type=float, default=None)
    ap.add_argument("--delta_rolling_sec", type=int, default=None)
    ap.add_argument("--vwap_range_filter", type=int, default=None)
    args = ap.parse_args()

    params = resolve_effective_params(args)

    slip_bps_entry = args.slip_bps if args.slip_bps_entry is None else args.slip_bps_entry
    slip_bps_tp = slip_bps_entry if args.slip_bps_tp is None else args.slip_bps_tp
    slip_bps_stop = (args.slip_bps * 2.0) if args.slip_bps_stop is None else args.slip_bps_stop
    fee_taker = args.fee if args.fee_taker is None else args.fee_taker
    fee_maker = args.fee if args.fee_maker is None else args.fee_maker

    if args.live:
        # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
        live_loop_skeleton(
            poll_sec=2.0,
            enable_daily_dd_kill=bool(args.daily_dd_kill),
            daily_dd_limit_pct=args.daily_dd_limit,
            daily_dd_mode=args.daily_dd_mode,
            daily_dd_timezone=args.daily_dd_timezone,
            params=params,
        )
        # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
    elif args.bt:
        if not args.csv:
            raise SystemExit("Provide --csv for backtest")
        run_backtest(
            args.csv,
            fee_taker,
            fee_maker,
            args.slip_bps,
            slip_bps_entry,
            slip_bps_tp,
            slip_bps_stop,
            # === CODEX_PATCH_BEGIN: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
            args.slip_model,
            args.slip_atr_ref_pct,
            args.slip_notional_ref,
            args.slip_k_size,
            args.slip_k_atr_entry,
            args.slip_k_atr_stop,
            args.slip_k_atr_tp,
            # === CODEX_PATCH_END: DYNAMIC_SLIPPAGE_MODEL (2026-02-02) ===
            args.equity,
            args.risk,
            # === CODEX_PATCH_BEGIN: MAX_NOTIONAL_CAP (2026-02-02) ===
            args.max_notional_usdt,
            # === CODEX_PATCH_END: MAX_NOTIONAL_CAP (2026-02-02) ===
            args.outdir,
            args.strict,
            # === CODEX_PATCH_BEGIN: FUNDING_AND_TP_BIDASK (2026-02-02) ===
            bool(args.enable_funding_cost),
            args.funding_rate_8h,
            bool(args.enable_tp_bidask),
            args.tp_bidask_half_spread_bps,
            # === CODEX_PATCH_END: FUNDING_AND_TP_BIDASK (2026-02-02) ===
            # === CODEX_PATCH_BEGIN: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
            bool(args.vol_filter_monthly),
            args.monthly_atr_q,
            args.daily_atr_len,
            args.monthly_atr_floor,
            # === CODEX_PATCH_END: VOL_FILTER_MONTHLY_ATR (2026-02-02) ===
            # === CODEX_PATCH_BEGIN: DAILY_DD_KILL_SWITCH (2026-02-02) ===
            bool(args.daily_dd_kill),
            args.daily_dd_limit,
            args.daily_dd_mode,
            args.daily_dd_timezone,
            # === CODEX_PATCH_END: DAILY_DD_KILL_SWITCH (2026-02-02) ===
            # === CODEX_PATCH_BEGIN: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
            bool(args.max_stops_kill),
            args.max_stops_per_day,
            args.max_stops_mode,
            args.max_stops_timezone,
            params,
            # === CODEX_PATCH_END: MAX_STOPS_PER_DAY_KILL (2026-02-02) ===
        )
    else:
        if not args.csv:
            raise SystemExit("Provide --csv path (ts,open,high,low,close,volume) or use --live")
        run_csv_plug_and_play(args.csv)

# -------------------------------------------------------------------
# Shadow entrypoint: scripts.run_shadow espera un symbol "Strategy"
# con método: on_candle(candle: dict, state: dict) -> dict|None
# -------------------------------------------------------------------


# Entrypoint para shadow runner: adapta generate_signals(batch) a ejecución streaming por vela.
class Strategy:
    def __init__(
        self,
        warmup_bars: int = 300,
        max_bars: int = 3000,
        cooldown_bars: int = 3,
        sl_atr_mult: float = 1.5,
        tp_r_mult: float = 1.0,
        allow_shorts: bool = True,
        **overrides: Any,
    ) -> None:
        self.warmup_bars = max(1, int(warmup_bars))
        self.cooldown_bars = max(0, int(cooldown_bars))
        self.sl_atr_mult = float(sl_atr_mult)
        self.tp_r_mult = float(tp_r_mult)
        self.allow_shorts = bool(allow_shorts)
        self._buffer = deque(maxlen=max(1, int(max_bars)))
        self._buf = self._buffer
        self._bar_index = 0

        self.cfg = StrategyConfig()
        if overrides:
            try:
                apply_overrides(self.cfg, overrides)
            except Exception:
                pass

        self._active_signal: str = ""
        self._last_entry_bar: Optional[int] = None
        self._err_count: int = 0
        self._last_err: str = ""
        self._last_err_log_ts_ms: int = 0


        # Meta for orchestrators / unified logging (PaperBroker ignores extra keys)
        self.strategy_name = os.getenv("VPC_BASE_NAME", "VPC-Base")
        try:
            self.meta_confidence = float(os.getenv("VPC_BASE_CONFIDENCE", "0.55"))
        except Exception:
            self.meta_confidence = 0.55
        raw_symbol = os.getenv("SYMBOL", self.cfg.symbol)
        tf_env = os.getenv("TF", "5m")
        mexc_symbol = self._mexc_contract_symbol(raw_symbol)
        interval = self._mexc_interval_from_tf(tf_env)
        prefill_enabled = os.getenv("SHADOW_PREFILL_REST", "1") == "1"

        if prefill_enabled:
            self._prefill_from_mexc_rest()

        prefill_rows = len(self._buf)
        prefill_ok = prefill_rows >= 1
        warmup_ready = prefill_rows >= self.warmup_bars

        last_ts = ""
        try:
            if self._buf:
                ts_val = self._buf[-1].get("ts")
                if ts_val is not None:
                    last_ts = pd.Timestamp(ts_val).isoformat()
        except Exception:
            last_ts = ""

        self._prefill_ok = prefill_ok
        self._prefill_rows = prefill_rows
        self._warmup_ready = warmup_ready

        ts_ms_fn = globals().get("_now_ms")
        try:
            ts_ms = int(ts_ms_fn()) if callable(ts_ms_fn) else int(time.time() * 1000)
        except Exception:
            ts_ms = int(time.time() * 1000)

        status = {
            "ts_ms": ts_ms,
            "prefill_enabled": prefill_enabled,
            "prefill_ok": prefill_ok,
            "prefill_rows": prefill_rows,
            "warmup_bars": self.warmup_bars,
            "warmup_ready": warmup_ready,
            "symbol_env": str(raw_symbol),
            "tf_env": str(tf_env),
            "mexc_symbol": str(mexc_symbol),
            "interval": str(interval),
            "last_ts": last_ts,
        }
        self._write_status_file(status)

        # === CODEX_PATCH_BEGIN: SHADOW_PREFILL_LOG_DEFAULT ===
        if os.getenv("SHADOW_PREFILL_LOG", "1") == "1":
            try:
                print(
                    "[INFO] STRATEGY_PREFILL "
                    f"prefill_ok={1 if prefill_ok else 0} "
                    f"prefill_rows={prefill_rows} "
                    f"warmup_bars={self.warmup_bars} "
                    f"warmup_ready={1 if warmup_ready else 0} "
                    f"symbol={raw_symbol} "
                    f"tf={tf_env} "
                    f"interval={interval} "
                    f"last_ts={last_ts}"
                )
            except Exception:
                pass
        # === CODEX_PATCH_END: SHADOW_PREFILL_LOG_DEFAULT ===

    @staticmethod
    def _mexc_contract_symbol(sym: str) -> str:
        symbol = str(sym or "").strip().upper()
        if not symbol:
            return ""
        if "_" in symbol:
            return symbol
        if symbol.endswith("USDT") and len(symbol) > 4:
            return f"{symbol[:-4]}_USDT"
        return symbol

    @staticmethod
    def _mexc_interval_from_tf(tf: str) -> str:
        mapping = {
            "1m": "Min1",
            "5m": "Min5",
            "15m": "Min15",
            "30m": "Min30",
            "60m": "Min60",
            "4h": "Hour4",
            "8h": "Hour8",
            "1d": "Day1",
            "1w": "Week1",
            "1M": "Month1",
        }
        key = str(tf or "").strip()
        return mapping.get(key, mapping.get(key.lower(), "Min5"))

    def _write_status_file(self, status: Dict[str, Any]) -> None:
        path = os.getenv("STRATEGY_STATUS_FILE", "").strip()
        if not path:
            return
        try:
            try:
                _ensure_dir(path)
            except Exception:
                d = os.path.dirname(path)
                if d:
                    os.makedirs(d, exist_ok=True)

            target_dir = os.path.dirname(path) or "."
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=target_dir, delete=False
            ) as tmp:
                json.dump(status, tmp, ensure_ascii=False)
                tmp.flush()
                os.fsync(tmp.fileno())
                tmp_path = tmp.name

            os.replace(tmp_path, path)
        except Exception:
            return

    def _prefill_from_mexc_rest(self) -> None:
        try:
            raw_symbol = os.getenv("SYMBOL", self.cfg.symbol)
            symbol = self._mexc_contract_symbol(raw_symbol)
            if not symbol:
                return
            interval = self._mexc_interval_from_tf(os.getenv("TF", "5m"))
            base = os.getenv("MEXC_REST_BASE", "https://api.mexc.com").rstrip("/")
            timeout_sec = float(os.getenv("MEXC_REST_TIMEOUT_SEC", "5.0"))
            url = f"{base}/api/v1/contract/kline/{symbol}?interval={interval}"

            with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
                payload = json.loads(resp.read().decode("utf-8"))

            if not isinstance(payload, dict) or payload.get("success") is not True:
                return
            data = payload.get("data")
            if not isinstance(data, dict):
                return

            times = data.get("time")
            opens = data.get("open")
            highs = data.get("high")
            lows = data.get("low")
            closes = data.get("close")
            vols = data.get("vol")

            if not isinstance(times, list):
                return

            size = min(
                len(times),
                len(opens) if isinstance(opens, list) else 0,
                len(highs) if isinstance(highs, list) else 0,
                len(lows) if isinstance(lows, list) else 0,
                len(closes) if isinstance(closes, list) else 0,
            )
            if size <= 0:
                return

            rows: List[Dict[str, Any]] = []
            has_vol = isinstance(vols, list)
            for i in range(size):
                rows.append(
                    {
                        "ts": pd.to_datetime(times[i], unit="s", utc=True),
                        "open": float(opens[i]),
                        "high": float(highs[i]),
                        "low": float(lows[i]),
                        "close": float(closes[i]),
                        "volume": float(vols[i]) if has_vol and i < len(vols) else 0.0,
                    }
                )

            keep = min(len(rows), self._buf.maxlen or len(rows))
            for row in rows[-keep:]:
                self._buf.append(row)
            self._bar_index = len(self._buf)
        except Exception:
            return

    @staticmethod
    def _parse_ts(candle: Dict[str, Any]) -> pd.Timestamp:
        raw_ts: Any = None
        for key in ("ts", "timestamp", "t", "open_time", "time"):
            if key in candle and candle[key] is not None:
                raw_ts = candle[key]
                break

        if raw_ts is None:
            # === CODEX_PATCH_BEGIN: TZ_ENSURE_UTC ===
            return ensure_utc(pd.Timestamp.utcnow())
            # === CODEX_PATCH_END: TZ_ENSURE_UTC ===

        try:
            if isinstance(raw_ts, str):
                raw_ts = float(raw_ts)
            if isinstance(raw_ts, (int, float)):
                unit = "ms" if abs(float(raw_ts)) >= 1e12 else "s"
                ts = pd.to_datetime(raw_ts, unit=unit, utc=True, errors="coerce")
            else:
                ts = pd.to_datetime(raw_ts, utc=True, errors="coerce")
            if pd.isna(ts):
                return ensure_utc(pd.Timestamp.utcnow())
            return ensure_utc(ts)
        except Exception:
            return ensure_utc(pd.Timestamp.utcnow())

    @staticmethod
    def _safe_float(value: Any, default: float) -> float:
        try:
            val = float(value)
            if not math.isfinite(val):
                return default
            return val
        except Exception:
            return default

    def _stop_distance(self, last_row: pd.Series, close_px: float) -> float:
        atr_value = float("nan")
        for col in ("atr5", "atr", "atr_5m"):
            if col in last_row.index:
                atr_value = self._safe_float(last_row.get(col), float("nan"))
                if math.isfinite(atr_value):
                    break
        if not math.isfinite(atr_value):
            return close_px * 0.01
        return atr_value * self.sl_atr_mult

    def on_candle(self, candle: Dict[str, float], state: Dict[str, float]) -> Optional[Dict[str, float]]:
        del state

        # === CODEX_PATCH_BEGIN: STRATEGY_CRASH_GUARD ===
        try:
            close = self._safe_float(candle.get("close"), 0.0)
            row = {
                "ts": self._parse_ts(candle),
                "open": self._safe_float(candle.get("open", close), close),
                "high": self._safe_float(candle.get("high", close), close),
                "low": self._safe_float(candle.get("low", close), close),
                "close": close,
                "volume": self._safe_float(candle.get("volume", 0.0), 0.0),
            }

            if self._buf and row["ts"] == self._buf[-1].get("ts"):
                self._buf[-1] = row
            else:
                self._buf.append(row)
                self._bar_index += 1

            if len(self._buf) < self.warmup_bars:
                return None

            df5m = pd.DataFrame(self._buf)
            df5m["ts"] = pd.to_datetime(df5m["ts"], utc=True, errors="coerce")

            sigdf = generate_signals(df5m, self.cfg)
            if sigdf is None or sigdf.empty:
                return None

            last = sigdf.iloc[-1]
            sig = str(last.get("signal", "")).upper().strip()
            if sig == "":
                return None
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            now_ms = _now_ms()
            self._err_count += 1
            self._last_err = repr(e)[:300]
            if now_ms - int(self._last_err_log_ts_ms) >= 60_000:
                print(f'[WARN] STRATEGY_ERROR count={self._err_count} err="{self._last_err}"')
                self._last_err_log_ts_ms = now_ms
            return None
        # === CODEX_PATCH_END: STRATEGY_CRASH_GUARD ===

        current_bar = max(0, self._bar_index - 1)
        can_enter = self._last_entry_bar is None or (current_bar - self._last_entry_bar) >= self.cooldown_bars

        if sig in {"LONG", "SHORT"} and self._active_signal and sig != self._active_signal:
            self._active_signal = ""
            return {"action": "EXIT", "strategy": self.strategy_name, "confidence": self.meta_confidence, "reason": "flip_signal"}

        close_px = self._safe_float(last.get("close", close), close)
        stop_distance = self._stop_distance(last, close_px)

        if sig == "LONG" and can_enter and self._active_signal != "LONG":
            self._active_signal = "LONG"
            self._last_entry_bar = current_bar
            return {
                "action": "ENTER_LONG",
                "stop": close_px - stop_distance,
                "tp": close_px + (self.tp_r_mult * stop_distance),
                "strategy": self.strategy_name,
                "confidence": self.meta_confidence,
                "reason": "signal=LONG",
            }

        if sig == "SHORT" and self.allow_shorts and can_enter and self._active_signal != "SHORT":
            self._active_signal = "SHORT"
            self._last_entry_bar = current_bar
            return {
                "action": "ENTER_SHORT",
                "stop": close_px + stop_distance,
                "tp": close_px - (self.tp_r_mult * stop_distance),
                "strategy": self.strategy_name,
                "confidence": self.meta_confidence,
                "reason": "signal=SHORT",
            }

        return None


# Smoke test manual (no ejecutable):
# python -c "from scripts.solusdt_vwap_bot import Strategy; s=Strategy(warmup_bars=3); print('ok');"
