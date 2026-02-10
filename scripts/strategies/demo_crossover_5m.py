from __future__ import annotations

from typing import Dict, Optional


class Strategy:
    def __init__(
        self,
        fast_period: int = 8,
        slow_period: int = 21,
        atr_period: int = 14,
        sl_atr_mult: float = 1.5,
        tp_r_mult: float = 1.0,
        cooldown_bars: int = 3,
        long_only: bool = True,
    ) -> None:
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.atr_period = atr_period
        self.sl_atr_mult = sl_atr_mult
        self.tp_r_mult = tp_r_mult
        self.cooldown_bars = cooldown_bars
        self.long_only = long_only
        self._bar_index = 0
        self._last_entry_index: Optional[int] = None
        self._ema_fast: Optional[float] = None
        self._ema_slow: Optional[float] = None
        self._prev_fast: Optional[float] = None
        self._prev_slow: Optional[float] = None
        self._prev_close: Optional[float] = None
        self._trs: list[float] = []

    def on_candle(self, candle: Dict[str, float], state: Dict[str, float]) -> Optional[Dict[str, float]]:
        close = float(candle.get("close"))
        high = float(candle.get("high"))
        low = float(candle.get("low"))
        self._bar_index += 1

        self._prev_fast = self._ema_fast
        self._prev_slow = self._ema_slow
        self._ema_fast = self._update_ema(self._ema_fast, close, self.fast_period)
        self._ema_slow = self._update_ema(self._ema_slow, close, self.slow_period)

        tr = self._true_range(high, low, self._prev_close)
        self._prev_close = close
        if tr is not None:
            self._trs.append(tr)
            if len(self._trs) > self.atr_period:
                self._trs.pop(0)
        atr = sum(self._trs) / len(self._trs) if self._trs else None

        if self._ema_fast is None or self._ema_slow is None:
            return None

        can_enter = (
            self._last_entry_index is None
            or (self._bar_index - self._last_entry_index) >= self.cooldown_bars
        )

        crossed_up = self._prev_fast is not None and self._prev_slow is not None and (
            self._prev_fast <= self._prev_slow and self._ema_fast > self._ema_slow
        )
        crossed_down = self._prev_fast is not None and self._prev_slow is not None and (
            self._prev_fast >= self._prev_slow and self._ema_fast < self._ema_slow
        )

        if crossed_up and can_enter:
            self._last_entry_index = self._bar_index
            stop_distance = (atr * self.sl_atr_mult) if atr else close * 0.01
            stop = close - stop_distance
            tp = close + self.tp_r_mult * stop_distance
            return {"action": "ENTER_LONG", "stop": stop, "tp": tp}

        if crossed_down:
            return {"action": "EXIT"}

        return None

    def _update_ema(self, prev: Optional[float], price: float, period: int) -> float:
        if prev is None:
            return price
        k = 2 / (period + 1)
        return prev + k * (price - prev)

    def _true_range(self, high: float, low: float, prev_close: Optional[float]) -> Optional[float]:
        if prev_close is None:
            return high - low
        return max(high - low, abs(high - prev_close), abs(low - prev_close))
