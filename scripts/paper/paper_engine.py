from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from scripts.paper.paper_broker import PaperBroker

if TYPE_CHECKING:
    from scripts.telegram_notifier import TelegramNotifier


@dataclass
class PaperConfig:
    starting_equity: float = 5000.0
    risk_pct_per_trade: float = 0.0025
    max_position_size: Optional[float] = None
    slippage_bps: float = 0.0
    stop_first: bool = True


class PaperEngine:
    def __init__(
        self,
        config: PaperConfig,
        log_dir: str,
        notifier: Optional["TelegramNotifier"] = None,
        symbol: Optional[str] = None,
        tf: Optional[str] = None,
        exchange: Optional[str] = None,
        event_sink: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.config = config
        self.broker = PaperBroker(
            starting_equity=config.starting_equity,
            log_dir=log_dir,
            risk_pct_per_trade=config.risk_pct_per_trade,
            max_position_size=config.max_position_size,
            slippage_bps=config.slippage_bps,
            stop_first=config.stop_first,
            notifier=notifier,
            symbol=symbol,
            tf=tf,
            exchange=exchange,
            event_sink=event_sink,
            on_event=on_event,
        )

    def on_candle(self, candle: Dict[str, Any], signal: Optional[Dict[str, Any]], reason: Optional[str] = None) -> None:
        if signal:
            self.broker.on_signal(candle, signal)
        self.broker.on_candle(candle)
        self.broker.snapshot(candle)
