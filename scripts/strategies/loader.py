from __future__ import annotations

import importlib
import json
import os
from collections import deque
from dataclasses import is_dataclass
from typing import Any, Deque, Dict, Optional, Sequence

from scripts.strategies.signal_adapters import normalize_strategy_output


class _NormalizedStrategyWrapper:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def prefill(self, candles: Sequence[Dict[str, Any]]) -> None:
        if hasattr(self._inner, "prefill"):
            self._inner.prefill(candles)

    def on_candle(self, candle: Dict[str, Any], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        out = self._inner.on_candle(candle, state)
        return normalize_strategy_output(out)


class _BucketAgg:
    def __init__(self, interval_ms: int) -> None:
        self.interval_ms = int(interval_ms)
        self.bucket_start: Optional[int] = None
        self.current: Optional[Dict[str, Any]] = None

    def _normalize_ts(self, c: Dict[str, Any]) -> Optional[int]:
        for key in ("open_ts", "close_ts", "timestamp", "ts"):
            if c.get(key) is None:
                continue
            try:
                ts = int(c[key])
            except Exception:
                continue
            if ts < 10_000_000_000:
                ts *= 1000
            return ts
        return None

    def update_5m(self, candle: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ts = self._normalize_ts(candle)
        if ts is None:
            return None
        bucket = (ts // self.interval_ms) * self.interval_ms

        try:
            o = float(candle["open"])
            h = float(candle["high"])
            l = float(candle["low"])
            cl = float(candle["close"])
            v = float(candle.get("volume") or 0.0)
        except Exception:
            return None

        if self.bucket_start is None:
            self.bucket_start = bucket
            self.current = {
                "open_ts": int(candle.get("open_ts") or bucket),
                "close_ts": int(candle.get("close_ts") or (bucket + self.interval_ms)),
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": v,
                "timestamp": int(candle.get("close_ts") or (bucket + self.interval_ms)),
            }
            return None

        if bucket == self.bucket_start:
            assert self.current is not None
            self.current["high"] = max(float(self.current["high"]), h)
            self.current["low"] = min(float(self.current["low"]), l)
            self.current["close"] = cl
            self.current["close_ts"] = int(candle.get("close_ts") or ts)
            self.current["timestamp"] = int(candle.get("close_ts") or ts)
            self.current["volume"] = float(self.current["volume"]) + v
            return None

        out = dict(self.current or {}) if self.current else None
        self.bucket_start = bucket
        self.current = {
            "open_ts": int(candle.get("open_ts") or bucket),
            "close_ts": int(candle.get("close_ts") or (bucket + self.interval_ms)),
            "open": o,
            "high": h,
            "low": l,
            "close": cl,
            "volume": v,
            "timestamp": int(candle.get("close_ts") or (bucket + self.interval_ms)),
        }
        return out


class GeneratedSignalStrategy:
    def __init__(self, module: Any) -> None:
        self._module = module
        self._generate = module.generate_signal
        self._state: Dict[str, Any] = {}

        cfg = self._load_cfg(module)
        self._cfg = cfg

        self._c5: Deque[Dict[str, Any]] = deque(maxlen=int(getattr(cfg, "max_candles_5m", 2000) if cfg is not None else 2000))
        self._c15: Deque[Dict[str, Any]] = deque(maxlen=int(getattr(cfg, "max_candles_15m", 2000) if cfg is not None else 2000))
        self._c1h: Deque[Dict[str, Any]] = deque(maxlen=int(getattr(cfg, "max_candles_1h", 2000) if cfg is not None else 2000))
        self._agg15 = _BucketAgg(15 * 60_000)
        self._agg1h = _BucketAgg(60 * 60_000)

    def _load_cfg(self, module: Any) -> Any:
        merged: Dict[str, Any] = {}
        for key in ("STRATEGY_CFG_JSON", f"{module.__name__.split('.')[-1].upper()}_CFG_JSON"):
            raw = os.getenv(key, "").strip()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    merged.update(parsed)
            except Exception:
                continue

        config_cls = getattr(module, "Config", None)
        if config_cls is not None and is_dataclass(config_cls):
            try:
                return config_cls(**merged)
            except Exception:
                return config_cls()

        return merged

    def prefill(self, candles: Sequence[Dict[str, Any]]) -> None:
        self._c5.clear(); self._c15.clear(); self._c1h.clear()
        self._agg15 = _BucketAgg(15 * 60_000)
        self._agg1h = _BucketAgg(60 * 60_000)
        for candle in candles or []:
            self.on_candle(candle, {})
        self._state.clear()
        self._state["prefilled"] = True

    def on_candle(self, candle: Dict[str, Any], _state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        c = dict(candle)
        if "timestamp" not in c:
            c["timestamp"] = int(c.get("close_ts") or c.get("open_ts") or 0)

        self._c5.append(c)
        c15 = self._agg15.update_5m(c)
        if c15:
            self._c15.append(c15)
        c1h = self._agg1h.update_5m(c)
        if c1h:
            self._c1h.append(c1h)

        out = self._generate(list(self._c5), list(self._c15), list(self._c1h), self._state, self._cfg)
        return normalize_strategy_output(out)


def load_strategy(module_path: str) -> Any:
    module = importlib.import_module(module_path)

    if hasattr(module, "Strategy"):
        return _NormalizedStrategyWrapper(module.Strategy())

    if hasattr(module, "generate_signal"):
        return GeneratedSignalStrategy(module)

    raise RuntimeError(f"No supported strategy export in {module_path}")


__all__ = ["load_strategy", "GeneratedSignalStrategy"]
