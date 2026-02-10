from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class TelegramNotifier:
    def __init__(self, enabled: Optional[bool] = None, timeout_sec: float = 6.0, **_: Any) -> None:
        env_enabled = os.getenv("TELEGRAM_ENABLED", "").strip() == "1"
        self.enabled = env_enabled if enabled is None else bool(enabled)
        self.timeout_sec = float(timeout_sec)
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    @staticmethod
    def _fmt(value: Optional[float], dec: int = 6) -> str:
        if value is None:
            return "N/A"
        try:
            return f"{float(value):.{dec}f}".rstrip("0").rstrip(".")
        except (TypeError, ValueError):
            return "N/A"

    @staticmethod
    def _fmt_ts(ts_ms: Optional[int]) -> str:
        if ts_ms is None:
            return "N/A"
        try:
            dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except (TypeError, ValueError):
            return "N/A"

    def send_message(self, text: str, event_type: Optional[str] = None) -> bool:
        if not self.enabled:
            print(f"[TG] DRY_RUN event={event_type or 'N/A'}\n{text}", flush=True)
            return True
        if not self.token or not self.chat_id:
            print("[TG] ERROR missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID", flush=True)
            return False

        payload = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}).encode("utf-8")
        req = urllib.request.Request(f"https://api.telegram.org/bot{self.token}/sendMessage", data=payload)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                parsed = json.loads(body) if body else {}
                ok = bool(parsed.get("ok", 200 <= getattr(resp, "status", 0) < 300))
                if ok:
                    print(f"[TG] SENT ok chat_id={self.chat_id} len={len(text)} event={event_type or 'N/A'}", flush=True)
                    return True
                print(f"[TG] ERROR telegram_response_not_ok event={event_type or 'N/A'}", flush=True)
                return False
        except Exception as exc:
            print(f"[TG] ERROR {exc}", flush=True)
            return False

    def build_vip_entry(self, event: Dict[str, Any]) -> str:
        symbol = event.get("symbol") or "N/A"
        side = str(event.get("side") or "N/A").upper()
        tf = event.get("tf")
        tf_line = f"\nTF: `{tf}`" if tf else ""
        return (
            f"🟣 VIP SIGNAL {symbol} {side}\n"
            "ENTRY\n"
            f"Entry: `{self._fmt(event.get('entry_price'))}`\n"
            f"Qty: `{self._fmt(event.get('qty'))}`\n"
            f"SL: `{self._fmt(event.get('stop_price'))}`\n"
            f"TP: `{self._fmt(event.get('take_profit'))}`\n"
            f"Time: `{self._fmt_ts(event.get('entry_ts'))}`"
            f"{tf_line}"
        )

    def build_vip_exit(self, event: Dict[str, Any]) -> str:
        symbol = event.get("symbol") or "N/A"
        side = str(event.get("side") or "N/A").upper()
        return (
            f"🟣 VIP SIGNAL {symbol} {side}\n"
            "EXIT\n"
            f"Exit: `{self._fmt(event.get('exit_price'))}`\n"
            f"PnL: `{self._fmt(event.get('pnl'))}` ({self._fmt(event.get('pnl_pct'), dec=4)}%)\n"
            f"Equity: `{self._fmt(event.get('equity_after'))}`\n"
            f"Reason: `{event.get('reason') or 'N/A'}`\n"
            f"Time: `{self._fmt_ts(event.get('exit_ts'))}`"
        )

    def notify_vip_event(self, event: Dict[str, Any]) -> bool:
        event_type = str(event.get("event_type") or "").upper()
        if event_type == "ENTRY":
            return self.send_message(self.build_vip_entry(event), event_type="ENTRY")
        if event_type == "EXIT":
            return self.send_message(self.build_vip_exit(event), event_type="EXIT")
        print(f"[TG] ERROR unsupported_event_type={event_type or 'N/A'}", flush=True)
        return False

    # Backward-compatible wrappers
    def send_vip_entry(self, **kwargs: Any) -> Dict[str, Any]:
        ok = self.notify_vip_event(
            {
                "event_type": "ENTRY",
                "symbol": kwargs.get("symbol"),
                "side": str(kwargs.get("side") or "").lower(),
                "qty": kwargs.get("qty"),
                "entry_price": kwargs.get("entry_price"),
                "entry_ts": kwargs.get("ts_ms"),
                "stop_price": kwargs.get("sl"),
                "take_profit": kwargs.get("tp"),
                "tf": kwargs.get("timeframe"),
                "reason": "signal_entry",
            }
        )
        return {"ok": ok, "sent": ok, "dry_run": not self.enabled}

    def send_vip_exit(self, **kwargs: Any) -> Dict[str, Any]:
        ok = self.notify_vip_event(
            {
                "event_type": "EXIT",
                "symbol": kwargs.get("symbol"),
                "side": str(kwargs.get("side") or "").lower(),
                "entry_price": kwargs.get("entry_price"),
                "exit_price": kwargs.get("exit_price"),
                "exit_ts": kwargs.get("ts_ms"),
                "pnl": kwargs.get("pnl_abs"),
                "pnl_pct": kwargs.get("pnl_pct"),
                "equity_after": kwargs.get("equity_after"),
                "reason": kwargs.get("reason"),
            }
        )
        return {"ok": ok, "sent": ok, "dry_run": not self.enabled}


if __name__ == "__main__":
    TelegramNotifier().send_message("Test trade notification")
