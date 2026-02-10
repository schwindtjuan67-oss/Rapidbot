from __future__ import annotations

import csv
import os
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from scripts.paper.paper_models import Order, Position, Trade

if TYPE_CHECKING:
    from scripts.telegram_notifier import TelegramNotifier


class PaperBroker:
    def __init__(
        self,
        starting_equity: float,
        log_dir: str,
        risk_pct_per_trade: float = 0.0025,
        max_position_size: Optional[float] = None,
        slippage_bps: float = 0.0,
        stop_first: bool = True,
        notifier: Optional["TelegramNotifier"] = None,
        symbol: Optional[str] = None,
        tf: Optional[str] = None,
        exchange: Optional[str] = None,
        event_sink: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.starting_equity = float(starting_equity)
        self.equity = float(starting_equity)
        self.risk_pct_per_trade = float(risk_pct_per_trade)
        self.max_position_size = max_position_size
        self.slippage_bps = float(slippage_bps)
        self.stop_first = bool(stop_first)
        self.position: Optional[Position] = None
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.order_id = 0
        self.trade_id = 0
        self.log_dir = log_dir
        self.notifier = notifier
        self.symbol = symbol
        self.tf = tf
        self.exchange = exchange
        self.event_sink = event_sink
        self.on_event = on_event
        self._notified_exit_events: set[tuple[int, str]] = set()
        self.orders_path = os.path.join(log_dir, "paper_orders.csv")
        self.trades_path = os.path.join(log_dir, "paper_trades.csv")
        self.equity_path = os.path.join(log_dir, "paper_equity.csv")
        os.makedirs(log_dir, exist_ok=True)
        self._ensure_headers()

    def _ensure_headers(self) -> None:
        self._ensure_header(
            self.orders_path,
            ["order_id", "ts", "action", "side", "qty", "price", "reason"],
        )
        self._ensure_header(
            self.trades_path,
            [
                "trade_id",
                "entry_ts",
                "exit_ts",
                "side",
                "qty",
                "entry",
                "exit",
                "pnl_gross",
                "pnl_net",
                "R",
            ],
        )
        self._ensure_header(
            self.equity_path,
            ["ts", "equity", "unrealized", "realized", "drawdown"],
        )

    def _ensure_header(self, path: str, header: list[str]) -> None:
        if os.path.exists(path):
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

    def _append_row(self, path: str, row: list[Any]) -> None:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)

    def _apply_slippage(self, price: float, side: str, is_entry: bool) -> float:
        if self.slippage_bps <= 0:
            return price
        bps = self.slippage_bps / 10_000.0
        if side.lower() == "long":
            return price * (1 + bps) if is_entry else price * (1 - bps)
        return price * (1 - bps) if is_entry else price * (1 + bps)

    def _log_order(self, order: Order) -> None:
        self._append_row(
            self.orders_path,
            [order.order_id, order.ts, order.action, order.side, order.qty, order.price, order.reason],
        )

    def _log_trade(self, trade: Trade) -> None:
        self._append_row(
            self.trades_path,
            [
                trade.trade_id,
                trade.entry_ts,
                trade.exit_ts,
                trade.side,
                trade.qty,
                trade.entry_price,
                trade.exit_price,
                trade.pnl_gross,
                trade.pnl_net,
                trade.r_multiple,
            ],
        )

    def _log_equity(self, ts: int) -> None:
        drawdown = min(0.0, self.equity - self.starting_equity)
        self._append_row(
            self.equity_path,
            [ts, self.equity, self.unrealized_pnl, self.realized_pnl, drawdown],
        )

    def on_signal(self, candle: Dict[str, Any], signal: Dict[str, Any]) -> None:
        action = signal.get("action")
        if action == "ENTER_LONG" and self.position is None:
            self._enter_long(candle, signal)
        elif action == "ENTER_SHORT" and self.position is None:
            self._enter_short(candle, signal)
        elif action == "EXIT" and self.position is not None:
            self._exit_position(candle, reason="signal_exit")

    def on_candle(self, candle: Dict[str, Any]) -> None:
        if self.position is None:
            self.unrealized_pnl = 0.0
            return
        high = float(candle.get("high"))
        low = float(candle.get("low"))
        stop = self.position.stop_price
        tp = self.position.take_profit
        exit_price = None
        reason = None
        is_long = self.position.side == "long"
        if stop is not None and tp is not None:
            stop_hit = (low <= stop) if is_long else (high >= stop)
            tp_hit = (high >= tp) if is_long else (low <= tp)
            if stop_hit and tp_hit:
                exit_price = stop if self.stop_first else tp
                reason = "stop_first" if self.stop_first else "tp_first"
            elif stop_hit:
                exit_price = stop
                reason = "stop"
            elif tp_hit:
                exit_price = tp
                reason = "tp"
        elif stop is not None and ((low <= stop) if is_long else (high >= stop)):
            exit_price = stop
            reason = "stop"
        elif tp is not None and ((high >= tp) if is_long else (low <= tp)):
            exit_price = tp
            reason = "tp"

        if exit_price is not None:
            self._exit_position(candle, reason=reason, exit_price=exit_price)
        else:
            self._mark_to_market(candle)

    def _enter_long(self, candle: Dict[str, Any], signal: Dict[str, Any]) -> None:
        entry_price = float(candle.get("close"))
        entry_price = self._apply_slippage(entry_price, "long", True)
        stop_price = signal.get("stop")
        take_profit = signal.get("tp")
        if stop_price is None:
            return
        stop_price = float(stop_price)
        stop_dist = max(0.0, entry_price - stop_price)
        if stop_dist <= 0:
            return
        risk_amount = self.equity * self.risk_pct_per_trade
        qty = risk_amount / stop_dist
        if self.max_position_size is not None:
            qty = min(qty, self.max_position_size)
        if qty <= 0:
            return
        self.order_id += 1
        order = Order(
            order_id=self.order_id,
            ts=int(candle.get("close_ts")),
            action="ENTRY",
            side="long",
            qty=qty,
            price=entry_price,
            reason="signal_entry",
        )
        self._log_order(order)
        self.position = Position(
            side="long",
            qty=qty,
            entry_price=entry_price,
            entry_ts=int(candle.get("close_ts")),
            stop_price=stop_price,
            take_profit=float(take_profit) if take_profit is not None else None,
        )
        print(
            f"[TRADE] OPEN ts={int(candle.get('close_ts'))} symbol={self.symbol or 'N/A'} side=LONG entry={entry_price:.6f} sl={stop_price:.6f} tp={self.position.take_profit if self.position.take_profit is not None else 'N/A'} qty={qty:.6f}",
            flush=True,
        )
        if self.event_sink is not None:
            self.event_sink(
                "OPEN",
                {
                    "ts": int(candle.get("close_ts")),
                    "symbol": self.symbol,
                    "side": "LONG",
                    "entry": entry_price,
                    "sl": stop_price,
                    "tp": self.position.take_profit,
                    "qty": qty,
                    "trade_id": self.trade_id + 1,
                },
            )
        if self.notifier is not None:
            try:
                resp = self.notifier.send_vip_entry(
                    exchange=self.exchange,
                    symbol=self.symbol,
                    timeframe=self.tf,
                    side="LONG",
                    entry_price=entry_price,
                    tp=self.position.take_profit,
                    sl=stop_price,
                    qty=qty,
                    trade_id=self.trade_id + 1,
                    ts_ms=int(candle.get("close_ts")),
                )
                if self.event_sink is not None:
                    self.event_sink(
                        "TG",
                        {"kind": "entry", "ok": int(bool(resp.get("ok"))), "dry_run": int(bool(resp.get("dry_run")))},
                    )
            except Exception as exc:
                print(f"[TG] entry notification failed: {exc}", flush=True)
        entry_event: Dict[str, Any] = {
            "event_type": "ENTRY",
            "symbol": self.symbol,
            "side": "long",
            "qty": qty,
            "entry_price": entry_price,
            "entry_ts": int(candle.get("close_ts")),
            "stop_price": stop_price,
            "take_profit": self.position.take_profit,
            "reason": "signal_entry",
        }
        if self.tf:
            entry_event["tf"] = self.tf
        self._safe_emit_event(entry_event)

    def _enter_short(self, candle: Dict[str, Any], signal: Dict[str, Any]) -> None:
        entry_price = float(candle.get("close"))
        entry_price = self._apply_slippage(entry_price, "short", True)
        stop_price = signal.get("stop")
        take_profit = signal.get("tp")
        if stop_price is None:
            return
        stop_price = float(stop_price)
        stop_dist = max(0.0, stop_price - entry_price)
        if stop_dist <= 0:
            return
        risk_amount = self.equity * self.risk_pct_per_trade
        qty = risk_amount / stop_dist
        if self.max_position_size is not None:
            qty = min(qty, self.max_position_size)
        if qty <= 0:
            return
        self.order_id += 1
        order = Order(
            order_id=self.order_id,
            ts=int(candle.get("close_ts")),
            action="ENTRY",
            side="short",
            qty=qty,
            price=entry_price,
            reason="signal_entry",
        )
        self._log_order(order)
        self.position = Position(
            side="short",
            qty=qty,
            entry_price=entry_price,
            entry_ts=int(candle.get("close_ts")),
            stop_price=stop_price,
            take_profit=float(take_profit) if take_profit is not None else None,
        )
        entry_event: Dict[str, Any] = {
            "event_type": "ENTRY",
            "symbol": self.symbol,
            "side": "short",
            "qty": qty,
            "entry_price": entry_price,
            "entry_ts": int(candle.get("close_ts")),
            "stop_price": stop_price,
            "take_profit": self.position.take_profit,
            "reason": "signal_entry",
        }
        if self.tf:
            entry_event["tf"] = self.tf
        self._safe_emit_event(entry_event)

    def _exit_position(
        self,
        candle: Dict[str, Any],
        reason: str,
        exit_price: Optional[float] = None,
    ) -> None:
        if self.position is None:
            return
        exit_val = exit_price if exit_price is not None else float(candle.get("close"))
        exit_val = self._apply_slippage(exit_val, self.position.side, False)
        direction = 1.0 if self.position.side == "long" else -1.0
        pnl_gross = (exit_val - self.position.entry_price) * self.position.qty * direction
        pnl_net = pnl_gross
        self.realized_pnl += pnl_net
        self.equity += pnl_net
        self.unrealized_pnl = 0.0
        self.trade_id += 1
        r_multiple = None
        if self.position.stop_price is not None:
            if self.position.side == "long":
                risk_per_unit = self.position.entry_price - self.position.stop_price
            else:
                risk_per_unit = self.position.stop_price - self.position.entry_price
            if risk_per_unit > 0:
                r_multiple = pnl_gross / (risk_per_unit * self.position.qty)
        trade = Trade(
            trade_id=self.trade_id,
            entry_ts=self.position.entry_ts,
            exit_ts=int(candle.get("close_ts")),
            side=self.position.side,
            qty=self.position.qty,
            entry_price=self.position.entry_price,
            exit_price=exit_val,
            pnl_gross=pnl_gross,
            pnl_net=pnl_net,
            r_multiple=r_multiple,
        )
        self._log_trade(trade)
        self.order_id += 1
        self._log_order(
            Order(
                order_id=self.order_id,
                ts=int(candle.get("close_ts")),
                action="EXIT",
                side=self.position.side,
                qty=self.position.qty,
                price=exit_val,
                reason=reason,
            )
        )
        r_mult_txt = f"{r_multiple:.4f}" if r_multiple is not None else "N/A"
        print(
            f"[TRADE] CLOSE ts={int(candle.get('close_ts'))} symbol={self.symbol or 'N/A'} side={self.position.side.upper()} exit={exit_val:.6f} pnl={pnl_net:.6f} r_mult={r_mult_txt}",
            flush=True,
        )
        if self.event_sink is not None:
            self.event_sink(
                "CLOSE",
                {
                    "ts": int(candle.get("close_ts")),
                    "symbol": self.symbol,
                    "side": self.position.side.upper(),
                    "exit": exit_val,
                    "entry": self.position.entry_price,
                    "pnl": pnl_net,
                    "r_mult": r_multiple,
                    "trade_id": self.trade_id,
                    "reason": reason,
                },
            )
        notify_reason = "tp" if "tp" in reason else "sl" if "stop" in reason or reason == "sl" else None
        if self.notifier is not None and notify_reason is not None:
            dedupe_key = (self.trade_id, notify_reason)
            if dedupe_key not in self._notified_exit_events:
                self._notified_exit_events.add(dedupe_key)
                try:
                    pnl_pct = 0.0
                    if self.position.entry_price != 0:
                        pnl_pct = ((exit_val - self.position.entry_price) / self.position.entry_price) * 100.0 * direction
                    resp = self.notifier.send_vip_exit(
                        symbol=self.symbol,
                        side=self.position.side.upper(),
                        entry_price=self.position.entry_price,
                        exit_price=exit_val,
                        pnl_abs=pnl_net,
                        pnl_pct=pnl_pct,
                        reason=notify_reason,
                        trade_id=self.trade_id,
                        ts_ms=int(candle.get("close_ts")),
                    )
                    if self.event_sink is not None:
                        self.event_sink(
                            "TG",
                            {"kind": "exit", "ok": int(bool(resp.get("ok"))), "dry_run": int(bool(resp.get("dry_run")))},
                        )
                except Exception as exc:
                    print(f"[TG] exit notification failed: {exc}", flush=True)
        exit_event: Dict[str, Any] = {
            "event_type": "EXIT",
            "symbol": self.symbol,
            "side": self.position.side,
            "qty": trade.qty,
            "entry_price": trade.entry_price,
            "entry_ts": trade.entry_ts,
            "stop_price": self.position.stop_price,
            "take_profit": self.position.take_profit,
            "exit_price": trade.exit_price,
            "exit_ts": trade.exit_ts,
            "pnl": trade.pnl_net,
            "pnl_pct": (((trade.exit_price - trade.entry_price) / trade.entry_price) * 100.0 * direction) if trade.entry_price else 0.0,
            "equity_after": self.equity,
            "reason": reason,
        }
        if self.tf:
            exit_event["tf"] = self.tf
        self._safe_emit_event(exit_event)
        self.position = None

    def _mark_to_market(self, candle: Dict[str, Any]) -> None:
        if self.position is None:
            self.unrealized_pnl = 0.0
            return
        close_price = float(candle.get("close"))
        direction = 1.0 if self.position.side == "long" else -1.0
        self.unrealized_pnl = (close_price - self.position.entry_price) * self.position.qty * direction

    def snapshot(self, candle: Dict[str, Any]) -> None:
        ts = int(candle.get("close_ts"))
        self._log_equity(ts)

    def _safe_emit_event(self, event: Dict[str, Any]) -> None:
        if self.on_event is None:
            return
        try:
            self.on_event(event)
        except Exception as exc:
            print(f"[EVENT] callback_error={exc}", flush=True)
