from __future__ import annotations

"""VPC-Retest (Break + Retest Entry)

Pure strategy module with:
  - generate_signal(candles_5m, ctx_15m, ctx_1h, state, cfg) -> Signal | None
  - Optional Strategy wrapper class (compatible with scripts/run_shadow.py loader)
      exports class Strategy with on_candle(candle, state) -> dict | None

Design goals:
  - Simple, stable, debug-friendly.
  - No execution / sizing / telegram side effects.
  - Uses only closed candles for higher TF contexts.

Expected candle schema (dict or object with attrs):
  open, high, low, close, volume
  close_ts or timestamp (ms or sec) recommended for session VWAP.

Expected ctx inputs:
  - ctx_1h: either precomputed dict/object {close,vwap_session,ema,atr,ema_slope}
           OR list of 1h candles (fallback computes)
  - ctx_15m: either precomputed dict/object {long_ok, short_ok}
            OR list of 15m candles (fallback computes pivots)
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple, Union, Literal
from collections import deque
import json
import os

from scripts.strategies.signal_adapters import normalize_strategy_output
from scripts.strategies.signal_models import Signal

Side = Literal["LONG", "SHORT"]


@dataclass
class VPC_RetestConfig:
    strategy_name: str = "VPC-Retest"

    # Warmup / indicators (5m)
    atr_len_5m: int = 14
    ema_len_5m: int = 20
    extra_warmup_bars: int = 50

    # Value touch (5m)
    touch_atr_mult: float = 0.35
    touch_lookback_bars: int = 6

    # Break level (5m range)
    break_lookback_bars: int = 20

    # Break validation
    break_buf_atr: float = 0.10
    break_min_tr_atr: float = 0.60
    break_min_body_tr: float = 0.45

    # Retest validation
    retest_tol_atr: float = 0.20
    retest_reclaim_atr: float = 0.05
    retest_min_wick_body: float = 1.2
    retest_max_wait_bars: int = 10

    # Filters
    min_atr_pct_5m: float = 0.0012
    max_vwap_cross_1h: int = 4  # last 12 bars of 5m

    # 1H bias
    slope_len_1h: int = 3
    ctx_min_dist_ema_atr_1h: float = 1.0  # |close-ema| / ATR_1h

    # 15m structure fallback
    pivot_len_15m: int = 2  # fractal size for confirmed pivots

    # Stops / Targets
    sl_swing_lookback_bars: int = 12
    sl_pad_atr: float = 0.15
    sl_min_atr: float = 0.60
    tp1_R: float = 1.0
    tp2_R: float = 2.0

    # Anti-spam
    cooldown_bars: int = 6

    # Optional entry buffer (stop-entry)
    entry_buf_atr: float = 0.0  # 0.05..0.15 if desired

    # VWAP session handling
    vwap_fallback_window_bars: int = 288  # 24h of 5m bars

    # Wrapper history sizes
    max_candles_5m: int = 2000
    max_candles_15m: int = 2000
    max_candles_1h: int = 2000


# -----------------------------
# Candle access helpers
# -----------------------------

def _get(x: Any, k: str, default: Any = None) -> Any:
    if x is None:
        return default
    if isinstance(x, dict):
        return x.get(k, default)
    return getattr(x, k, default)


def _f(x: Any, k: str, default: float = 0.0) -> float:
    v = _get(x, k, None)
    if v is None:
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _ts_ms(x: Any) -> Optional[int]:
    """Best-effort timestamp in milliseconds from common fields."""
    for key in ("close_ts", "timestamp", "ts", "time", "open_ts"):
        v = _get(x, key, None)
        if v is None:
            continue
        try:
            t = int(v)
        except Exception:
            continue
        # If looks like seconds, convert to ms
        if t < 10_000_000_000:
            t *= 1000
        return t
    return None


def _utc_day_key_from_ms(ts_ms: int) -> Tuple[int, int, int]:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return (dt.year, dt.month, dt.day)


# -----------------------------
# Indicators (minimal, stable)
# -----------------------------

def ema_last(values: Sequence[float], length: int) -> Optional[float]:
    if length <= 0 or len(values) < length:
        return None
    alpha = 2.0 / (length + 1.0)
    e = values[0]
    for v in values[1:]:
        e = alpha * v + (1.0 - alpha) * e
    return e


def atr_wilder_last(candles: Sequence[Any], length: int) -> Optional[float]:
    if length <= 0 or len(candles) < length + 1:
        return None
    trs: List[float] = []
    prev_close = _f(candles[0], "close", 0.0)
    for c in candles[1:]:
        h = _f(c, "high", 0.0)
        l = _f(c, "low", 0.0)
        cl = _f(c, "close", 0.0)
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = cl
    if len(trs) < length:
        return None
    a = sum(trs[:length]) / float(length)
    for tr in trs[length:]:
        a = (a * (length - 1) + tr) / float(length)
    return a


def vwap_session_series(candles: Sequence[Any], fallback_window: int) -> List[float]:
    """Session VWAP (UTC day reset) if timestamps exist; otherwise rolling VWAP."""
    if not candles:
        return []
    has_ts = _ts_ms(candles[0]) is not None

    out: List[float] = []
    if has_ts:
        cum_pv = 0.0
        cum_v = 0.0
        cur_key: Optional[Tuple[int, int, int]] = None
        for c in candles:
            t_ms = _ts_ms(c)
            if t_ms is None:
                t_ms = _ts_ms(candles[0]) or 0
            key = _utc_day_key_from_ms(t_ms)
            if cur_key is None or key != cur_key:
                cur_key = key
                cum_pv, cum_v = 0.0, 0.0
            h = _f(c, "high", 0.0)
            l = _f(c, "low", 0.0)
            cl = _f(c, "close", 0.0)
            vol = _f(c, "volume", 1.0)
            if vol <= 0:
                vol = 1.0
            tp = (h + l + cl) / 3.0
            cum_pv += tp * vol
            cum_v += vol
            out.append(cum_pv / cum_v if cum_v > 0 else cl)
        return out

    # fallback: rolling
    for i in range(len(candles)):
        lo = max(0, i - fallback_window + 1)
        cum_pv = 0.0
        cum_v = 0.0
        for c in candles[lo : i + 1]:
            h = _f(c, "high", 0.0)
            l = _f(c, "low", 0.0)
            cl = _f(c, "close", 0.0)
            vol = _f(c, "volume", 1.0)
            if vol <= 0:
                vol = 1.0
            tp = (h + l + cl) / 3.0
            cum_pv += tp * vol
            cum_v += vol
        cl_i = _f(candles[i], "close", 0.0)
        out.append(cum_pv / cum_v if cum_v > 0 else cl_i)
    return out


def count_vwap_crosses(closes: Sequence[float], vwaps: Sequence[float], lookback_bars: int) -> int:
    n = min(len(closes), len(vwaps))
    if n < 3:
        return 0
    lb = min(lookback_bars, n - 1)
    start = n - lb

    def sgn(x: float) -> int:
        if x > 0:
            return 1
        if x < 0:
            return -1
        return 0

    crosses = 0
    prev = sgn(closes[start - 1] - vwaps[start - 1])
    for i in range(start, n):
        cur = sgn(closes[i] - vwaps[i])
        if cur != 0 and prev != 0 and cur != prev:
            crosses += 1
        if cur != 0:
            prev = cur
    return crosses


# -----------------------------
# ctx helpers (1H bias, 15m structure)
# -----------------------------

@dataclass(frozen=True)
class BiasResult:
    side: Literal["LONG", "SHORT", "NO_TRADE"]
    score: int
    long_score: int
    short_score: int


def compute_bias_1h(ctx_1h: Any, cfg: VPC_RetestConfig) -> Optional[BiasResult]:
    # A) precomputed dict/object
    close = _get(ctx_1h, "close", None)
    vwap = _get(ctx_1h, "vwap_session", None)
    ema = _get(ctx_1h, "ema", None)
    atr = _get(ctx_1h, "atr", None)
    ema_slope = _get(ctx_1h, "ema_slope", None)
    if close is not None and vwap is not None and ema is not None and atr is not None:
        close_f = float(close)
        vwap_f = float(vwap)
        ema_f = float(ema)
        atr_f = float(atr)
        if atr_f <= 0:
            return None
        ema_slope_f = float(ema_slope) if ema_slope is not None else (close_f - ema_f)
        dist_atr = abs(close_f - ema_f) / atr_f

        long_score = 0
        short_score = 0
        if close_f > vwap_f:
            long_score += 1
        if close_f < vwap_f:
            short_score += 1
        if ema_slope_f > 0:
            long_score += 1
        if ema_slope_f < 0:
            short_score += 1
        if dist_atr >= cfg.ctx_min_dist_ema_atr_1h:
            if close_f > ema_f:
                long_score += 1
            elif close_f < ema_f:
                short_score += 1

        side: Literal["LONG", "SHORT", "NO_TRADE"] = "NO_TRADE"
        score = max(long_score, short_score)
        if score >= 2:
            side = "LONG" if long_score >= short_score else "SHORT"
        return BiasResult(side=side, score=score, long_score=long_score, short_score=short_score)

    # B) list of 1h candles
    if isinstance(ctx_1h, (list, tuple)) and len(ctx_1h) >= 60:
        candles_1h = ctx_1h
        closes = [_f(c, "close", 0.0) for c in candles_1h]
        ema_1h = ema_last(closes, length=max(5, min(55, cfg.ema_len_5m)))
        atr_1h = atr_wilder_last(candles_1h, length=max(5, min(55, cfg.atr_len_5m)))
        vwap_1h = vwap_session_series(candles_1h, fallback_window=min(len(candles_1h), 200))
        if ema_1h is None or atr_1h is None or not vwap_1h:
            return None

        close_f = closes[-1]
        ema_f = float(ema_1h)
        atr_f = float(atr_1h)
        if atr_f <= 0:
            return None
        vwap_f = float(vwap_1h[-1])

        k = max(1, int(cfg.slope_len_1h))
        ema_prev = ema_last(closes[:-k], length=max(5, min(55, cfg.ema_len_5m))) if len(closes) > k + 10 else None
        ema_slope_f = (ema_f - float(ema_prev)) if ema_prev is not None else (close_f - ema_f)

        dist_atr = abs(close_f - ema_f) / atr_f

        long_score = 0
        short_score = 0
        if close_f > vwap_f:
            long_score += 1
        if close_f < vwap_f:
            short_score += 1
        if ema_slope_f > 0:
            long_score += 1
        if ema_slope_f < 0:
            short_score += 1
        if dist_atr >= cfg.ctx_min_dist_ema_atr_1h:
            if close_f > ema_f:
                long_score += 1
            elif close_f < ema_f:
                short_score += 1

        side: Literal["LONG", "SHORT", "NO_TRADE"] = "NO_TRADE"
        score = max(long_score, short_score)
        if score >= 2:
            side = "LONG" if long_score >= short_score else "SHORT"
        return BiasResult(side=side, score=score, long_score=long_score, short_score=short_score)

    return None


@dataclass(frozen=True)
class Structure15m:
    long_ok: bool
    short_ok: bool


def _pivots_fractal(candles: Sequence[Any], left_right: int) -> Tuple[List[int], List[int]]:
    n = len(candles)
    lr = max(1, int(left_right))
    ph: List[int] = []
    pl: List[int] = []
    highs = [_f(c, "high", 0.0) for c in candles]
    lows = [_f(c, "low", 0.0) for c in candles]
    for i in range(lr, n - lr):
        h = highs[i]
        l = lows[i]
        if all(h > highs[j] for j in range(i - lr, i + lr + 1) if j != i):
            ph.append(i)
        if all(l < lows[j] for j in range(i - lr, i + lr + 1) if j != i):
            pl.append(i)
    return ph, pl


def compute_structure_15m(ctx_15m: Any, cfg: VPC_RetestConfig) -> Optional[Structure15m]:
    lo = _get(ctx_15m, "long_ok", None)
    so = _get(ctx_15m, "short_ok", None)
    if lo is not None and so is not None:
        return Structure15m(long_ok=bool(lo), short_ok=bool(so))

    if isinstance(ctx_15m, (list, tuple)) and len(ctx_15m) >= 50:
        candles = ctx_15m
        ph, pl = _pivots_fractal(candles, left_right=cfg.pivot_len_15m)
        if len(ph) < 2 or len(pl) < 2:
            return None

        h1_i, h2_i = ph[-2], ph[-1]
        l1_i, l2_i = pl[-2], pl[-1]
        h1 = _f(candles[h1_i], "high", 0.0)
        h2 = _f(candles[h2_i], "high", 0.0)
        l1 = _f(candles[l1_i], "low", 0.0)
        l2 = _f(candles[l2_i], "low", 0.0)

        long_ok = (h2 > h1) and (l2 > l1)
        short_ok = (h2 < h1) and (l2 < l1)
        return Structure15m(long_ok=long_ok, short_ok=short_ok)

    return None


# -----------------------------
# Break / Retest logic
# -----------------------------

def _true_range(last: Any, prev_close: float) -> float:
    h = _f(last, "high", 0.0)
    l = _f(last, "low", 0.0)
    return max(h - l, abs(h - prev_close), abs(l - prev_close))


def is_valid_break_long(last: Any, prev: Any, L_break: float, atr: float, cfg: VPC_RetestConfig) -> bool:
    close = _f(last, "close", 0.0)
    open_ = _f(last, "open", close)
    prev_close = _f(prev, "close", close)
    tr = _true_range(last, prev_close)
    body = abs(close - open_)
    if atr <= 0 or tr <= 0:
        return False
    if close <= (L_break + cfg.break_buf_atr * atr):
        return False
    if tr < cfg.break_min_tr_atr * atr:
        return False
    if (body / tr) < cfg.break_min_body_tr:
        return False
    return True


def is_valid_break_short(last: Any, prev: Any, L_break: float, atr: float, cfg: VPC_RetestConfig) -> bool:
    close = _f(last, "close", 0.0)
    open_ = _f(last, "open", close)
    prev_close = _f(prev, "close", close)
    tr = _true_range(last, prev_close)
    body = abs(close - open_)
    if atr <= 0 or tr <= 0:
        return False
    if close >= (L_break - cfg.break_buf_atr * atr):
        return False
    if tr < cfg.break_min_tr_atr * atr:
        return False
    if (body / tr) < cfg.break_min_body_tr:
        return False
    return True


def is_valid_retest_long(last: Any, L_break: float, atr: float, cfg: VPC_RetestConfig) -> bool:
    low = _f(last, "low", 0.0)
    close = _f(last, "close", 0.0)
    open_ = _f(last, "open", close)

    if low > (L_break + cfg.retest_tol_atr * atr):
        return False

    body = abs(close - open_)
    wick_down = max(0.0, min(open_, close) - low)

    reclaim = close >= (L_break + cfg.retest_reclaim_atr * atr)
    reject = (body > 0 and (wick_down / body) >= cfg.retest_min_wick_body and close >= open_)
    return bool(reclaim or reject)


def is_valid_retest_short(last: Any, L_break: float, atr: float, cfg: VPC_RetestConfig) -> bool:
    high = _f(last, "high", 0.0)
    close = _f(last, "close", 0.0)
    open_ = _f(last, "open", close)

    if high < (L_break - cfg.retest_tol_atr * atr):
        return False

    body = abs(close - open_)
    wick_up = max(0.0, high - max(open_, close))

    reclaim = close <= (L_break - cfg.retest_reclaim_atr * atr)
    reject = (body > 0 and (wick_up / body) >= cfg.retest_min_wick_body and close <= open_)
    return bool(reclaim or reject)


def _clamp01(x: float) -> float:
    if x < 0:
        return 0.0
    if x > 1:
        return 1.0
    return x


def compute_confidence(
    bias: BiasResult,
    structure: Structure15m,
    crosses_1h: int,
    atr_pct: float,
    cfg: VPC_RetestConfig,
) -> float:
    c = 0.50
    c += 0.08 * max(0, bias.score - 1)  # 2/3 -> +0.08, 3/3 -> +0.16
    c += 0.10  # structure already gated ok
    c += 0.08 * (1.0 - min(1.0, crosses_1h / max(1.0, float(cfg.max_vwap_cross_1h))))
    if cfg.min_atr_pct_5m > 0:
        c += 0.06 * min(1.0, max(0.0, (atr_pct / cfg.min_atr_pct_5m) - 1.0))
    return _clamp01(c)


# -----------------------------
# Pure function: generate_signal
# -----------------------------

def generate_signal(
    candles_5m: Sequence[Any],
    ctx_15m: Any,
    ctx_1h: Any,
    state: Dict[str, Any],
    cfg_in: Union[VPC_RetestConfig, Dict[str, Any]],
) -> Optional[Signal]:
    cfg = cfg_in if isinstance(cfg_in, VPC_RetestConfig) else VPC_RetestConfig(**cfg_in)

    n = len(candles_5m)
    min_need = max(cfg.atr_len_5m, cfg.ema_len_5m, cfg.break_lookback_bars) + cfg.extra_warmup_bars
    if n < min_need or n < 3:
        return None

    bar_i = n - 1
    last = candles_5m[-1]
    prev = candles_5m[-2]

    close = _f(last, "close", 0.0)
    if close <= 0:
        return None

    # cooldown
    last_sig = int(state.get("last_signal_bar", -10**9))
    if (bar_i - last_sig) < cfg.cooldown_bars:
        return None

    # indicators (5m)
    atr = atr_wilder_last(candles_5m[-(cfg.atr_len_5m + 50):], cfg.atr_len_5m)
    if atr is None or atr <= 0:
        return None
    atr_pct = atr / close
    if atr_pct < cfg.min_atr_pct_5m:
        return None

    closes_5m = [_f(c, "close", 0.0) for c in candles_5m]
    ema = ema_last(closes_5m[-(cfg.ema_len_5m + 200):], cfg.ema_len_5m)
    if ema is None:
        return None
    ema = float(ema)

    vwaps = vwap_session_series(candles_5m, fallback_window=cfg.vwap_fallback_window_bars)
    if not vwaps:
        return None
    vwap = float(vwaps[-1])

    crosses = count_vwap_crosses(closes_5m, vwaps, lookback_bars=12)
    if crosses > cfg.max_vwap_cross_1h:
        state.pop("vpc_retest", None)
        return None

    # 1H bias (score >= 2)
    bias = compute_bias_1h(ctx_1h, cfg)
    if bias is None or bias.side == "NO_TRADE":
        state.pop("vpc_retest", None)
        return None

    # 15m structure gate
    structure = compute_structure_15m(ctx_15m, cfg)
    if structure is None:
        return None
    if bias.side == "LONG" and not structure.long_ok:
        state.pop("vpc_retest", None)
        return None
    if bias.side == "SHORT" and not structure.short_ok:
        state.pop("vpc_retest", None)
        return None

    # touch_value
    dist_to_value = min(abs(close - vwap), abs(close - ema))
    touched = dist_to_value <= (cfg.touch_atr_mult * atr)
    if touched:
        state["vpc_retest_last_touch_bar"] = bar_i
    last_touch = state.get("vpc_retest_last_touch_bar", None)
    touch_recent = (last_touch is not None) and ((bar_i - int(last_touch)) <= cfg.touch_lookback_bars)

    # state machine
    setup = state.get("vpc_retest", None)
    if not isinstance(setup, dict):
        setup = {"status": "IDLE", "side": None}
        state["vpc_retest"] = setup

    # invalidate if bias flips
    if setup.get("status") != "IDLE" and setup.get("side") and setup.get("side") != bias.side:
        state["vpc_retest"] = {"status": "IDLE", "side": None}
        return None

    # IDLE -> ARMED
    if setup["status"] == "IDLE":
        if not touch_recent:
            return None
        setup.update({"status": "ARMED", "side": bias.side, "armed_bar": bar_i})

    # ARMED: break detection
    if setup["status"] == "ARMED":
        lb = cfg.break_lookback_bars
        if n < lb + 2:
            return None
        window = candles_5m[-(lb + 1):-1]  # exclude last
        range_high = max(_f(c, "high", 0.0) for c in window)
        range_low = min(_f(c, "low", 0.0) for c in window)

        if setup["side"] == "LONG":
            L_break = float(range_high)
            broke = is_valid_break_long(last, prev, L_break, atr, cfg)
        else:
            L_break = float(range_low)
            broke = is_valid_break_short(last, prev, L_break, atr, cfg)

        if not broke:
            # avoid staying armed forever
            if (bar_i - int(setup.get("armed_bar", bar_i))) > cfg.retest_max_wait_bars:
                state["vpc_retest"] = {"status": "IDLE", "side": None}
            return None

        setup.update({"status": "WAIT_RETEST", "L_break": L_break, "break_bar": bar_i})
        return None

    # WAIT_RETEST
    if setup["status"] == "WAIT_RETEST":
        break_bar = int(setup.get("break_bar", bar_i))
        if (bar_i - break_bar) > cfg.retest_max_wait_bars:
            state["vpc_retest"] = {"status": "IDLE", "side": None}
            return None

        L_break = float(setup.get("L_break", 0.0))
        if L_break <= 0:
            state["vpc_retest"] = {"status": "IDLE", "side": None}
            return None

        if setup["side"] == "LONG":
            ok_retest = is_valid_retest_long(last, L_break, atr, cfg)
        else:
            ok_retest = is_valid_retest_short(last, L_break, atr, cfg)

        if not ok_retest:
            return None

        side: Side = setup["side"]

        # Entry / SL / TP
        if side == "LONG":
            entry = max(close, _f(last, "high", close) + cfg.entry_buf_atr * atr)
            swing_low = min(_f(c, "low", 0.0) for c in candles_5m[-cfg.sl_swing_lookback_bars:])
            sl_raw = swing_low - cfg.sl_pad_atr * atr
            sl_min = entry - cfg.sl_min_atr * atr
            sl = min(sl_raw, sl_min)
            R = entry - sl
            if R <= 0:
                state["vpc_retest"] = {"status": "IDLE", "side": None}
                return None
            tp1 = entry + cfg.tp1_R * R
            tp2 = entry + cfg.tp2_R * R
        else:
            entry = min(close, _f(last, "low", close) - cfg.entry_buf_atr * atr)
            swing_high = max(_f(c, "high", 0.0) for c in candles_5m[-cfg.sl_swing_lookback_bars:])
            sl_raw = swing_high + cfg.sl_pad_atr * atr
            sl_min = entry + cfg.sl_min_atr * atr
            sl = max(sl_raw, sl_min)
            R = sl - entry
            if R <= 0:
                state["vpc_retest"] = {"status": "IDLE", "side": None}
                return None
            tp1 = entry - cfg.tp1_R * R
            tp2 = entry - cfg.tp2_R * R

        confidence = compute_confidence(bias, structure, crosses, atr_pct, cfg)

        reason = (
            f"{cfg.strategy_name} {side}: bias={bias.score}/3 (L{bias.long_score}/S{bias.short_score}), "
            f"touch_recent=1, break_level={L_break:.6f}, retest_ok=1, "
            f"vwap_crosses_1h={crosses}, atr_pct_5m={atr_pct:.5f}"
        )

        state["last_signal_bar"] = bar_i
        state["vpc_retest"] = {"status": "IDLE", "side": None}

        return Signal(
            side=side,
            entry=float(entry),
            sl=float(sl),
            tp1=float(tp1),
            tp2=float(tp2),
            reason=reason,
            confidence=float(confidence),
            strategy_name=cfg.strategy_name,
        )

    # unknown status
    state["vpc_retest"] = {"status": "IDLE", "side": None}
    return None


# -----------------------------
# Optional wrapper: Strategy (compatible with run_shadow.py)
# -----------------------------

class _BucketAgg:
    def __init__(self, interval_ms: int) -> None:
        self.interval_ms = int(interval_ms)
        self.bucket_start: Optional[int] = None
        self.open_ts: Optional[int] = None
        self.close_ts: Optional[int] = None
        self.open: Optional[float] = None
        self.high: Optional[float] = None
        self.low: Optional[float] = None
        self.close: Optional[float] = None
        self.volume: float = 0.0

    def update_5m(self, c: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ts = _ts_ms(c)
        if ts is None:
            return None
        # Prefer open_ts for bucketing if present
        ot = _get(c, "open_ts", None)
        try:
            ot_ms = int(ot) if ot is not None else ts
            if ot_ms < 10_000_000_000:
                ot_ms *= 1000
        except Exception:
            ot_ms = ts
        b = (ot_ms // self.interval_ms) * self.interval_ms

        o = float(c.get("open"))
        h = float(c.get("high"))
        l = float(c.get("low"))
        cl = float(c.get("close"))
        v = float(c.get("volume") or 0.0)

        if self.bucket_start is None:
            self._start(b, c, o, h, l, cl, v)
            return None

        if b == self.bucket_start:
            # update current bucket
            self.high = max(self.high or h, h)
            self.low = min(self.low or l, l)
            self.close = cl
            self.close_ts = int(c.get("close_ts") or ts)
            self.volume += v
            return None

        # bucket changed -> emit previous closed candle
        out = self._close()
        self._start(b, c, o, h, l, cl, v)
        return out

    def _start(self, b: int, c: Dict[str, Any], o: float, h: float, l: float, cl: float, v: float) -> None:
        self.bucket_start = b
        self.open_ts = int(c.get("open_ts") or b)
        self.close_ts = int(c.get("close_ts") or (b + self.interval_ms))
        self.open = o
        self.high = h
        self.low = l
        self.close = cl
        self.volume = v

    def _close(self) -> Optional[Dict[str, Any]]:
        if self.bucket_start is None or self.open is None or self.high is None or self.low is None or self.close is None:
            return None
        return {
            "open_ts": self.open_ts,
            "close_ts": self.close_ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timestamp": self.close_ts,  # convenience for session VWAP
        }


class Strategy:
    """Drop-in for scripts/run_shadow.py which expects Strategy().on_candle(candle, state)->dict|None.

    This wrapper:
      - keeps closed 5m candles history
      - aggregates closed 15m & 1h candles from 5m
      - calls generate_signal(...)
      - maps Signal -> paper signal dict: ENTER_LONG/ENTER_SHORT with stop+tp (tp1 used)
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None) -> None:
        # Optional runtime overrides (no code changes needed):
        # - STRATEGY_CFG_JSON: generic json dict
        # - VPC_RETEST_CFG_JSON: strategy-specific json dict
        merged: Dict[str, Any] = {}
        try:
            raw = os.getenv("STRATEGY_CFG_JSON")
            if raw:
                merged.update(json.loads(raw))
        except Exception:
            pass
        try:
            raw = os.getenv("VPC_RETEST_CFG_JSON")
            if raw:
                merged.update(json.loads(raw))
        except Exception:
            pass
        if cfg:
            merged.update(cfg)
        self.cfg = VPC_RetestConfig(**merged)
        self.state: Dict[str, Any] = {}

        self.c5: Deque[Dict[str, Any]] = deque(maxlen=self.cfg.max_candles_5m)
        self.c15: Deque[Dict[str, Any]] = deque(maxlen=self.cfg.max_candles_15m)
        self.c1h: Deque[Dict[str, Any]] = deque(maxlen=self.cfg.max_candles_1h)

        self._agg15 = _BucketAgg(interval_ms=15 * 60_000)
        self._agg1h = _BucketAgg(interval_ms=60 * 60_000)

    def prefill(self, candles: Sequence[Dict[str, Any]]) -> None:
        """Warm up internal buffers using historical 5m candles.

        - Builds 15m / 1h aggregated candles.
        - Does NOT emit signals (warmup only).
        - Resets state machine after warmup to avoid stale retest setups.
        """
        try:
            items = list(candles or [])
        except Exception:
            items = []
        if not items:
            return
        # reset buffers + aggregators
        self.c5.clear(); self.c15.clear(); self.c1h.clear()
        self._agg15 = _BucketAgg(interval_ms=15 * 60_000)
        self._agg1h = _BucketAgg(interval_ms=60 * 60_000)
        for candle in items:
            c = dict(candle)
            if "timestamp" not in c:
                c["timestamp"] = int(c.get("close_ts") or c.get("open_ts") or 0)
            self.c5.append(c)
            c15 = self._agg15.update_5m(c)
            if c15:
                self.c15.append(c15)
            c1h = self._agg1h.update_5m(c)
            if c1h:
                self.c1h.append(c1h)
        # Clean state after warmup (avoid immediate entries from prefill history)
        self.state.clear()
        self.state["prefilled"] = True

    def on_candle(self, candle: Dict[str, Any], _state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Normalize: ensure timestamp exists for VWAP session
        c = dict(candle)
        if "timestamp" not in c:
            c["timestamp"] = int(c.get("close_ts") or c.get("open_ts") or 0)

        self.c5.append(c)

        # update higher TF aggregations (closed only)
        c15 = self._agg15.update_5m(c)
        if c15:
            self.c15.append(c15)
        c1h = self._agg1h.update_5m(c)
        if c1h:
            self.c1h.append(c1h)

        sig = generate_signal(
            candles_5m=list(self.c5),
            ctx_15m=list(self.c15),
            ctx_1h=list(self.c1h),
            state=self.state,
            cfg_in=self.cfg,
        )
        if sig is None:
            return None
        return normalize_strategy_output(sig)


__all__ = ["Signal", "VPC_RetestConfig", "generate_signal", "Strategy"]
