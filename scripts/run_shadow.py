from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import random
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Optional

import websockets

from adapters.mexc_ws import (
    build_kline_subscribe_payload,
    is_kline_closed,
    parse_mexc_kline_message_with_reason,
    parse_mexc_trade_message,
    should_accept_closed_kline,
)
from scripts.mexc_rest import fetch_contract_klines
from scripts.paper.paper_engine import PaperConfig, PaperEngine
from scripts.shadow_trade_sink import ShadowTradeSink
from scripts.strategies.loader import load_strategy
from scripts.strategies.signal_adapters import normalize_strategy_output
from scripts.telegram_notifier import TelegramNotifier
from scripts.ws_utils import CandleAggregator, MessageClassifier, SampleRecorder


_LOG_FILE: Optional[Any] = None


@dataclass
class RunConfig:
    exchange: str
    ws_url: str
    symbol: str
    interval: str
    mode: str
    out_dir: str
    log_dir: str
    stop_file: str
    status_path: str
    health_path: str
    manifest_path: str
    pid_path: str
    idle_timeout_sec: int = 60
    recv_timeout_sec: int = 60
    data_stall_sec: int = 180
    data_stall_max_attempts: int = 2
    kline_fallback_after_sec: int = 90
    log_every_sec: int = 15
    ping_sec: int = 15
    ping_timeout_sec: int = 45
    ping_log_every: int = 4
    unknown_samples_path: str = "./logs/ws_unknown_samples.jsonl"
    parse_fail_samples_path: str = "./logs/ws_parse_fail_samples.jsonl"
    samples_limit: int = 50
    start_equity: float = 5000.0
    strategy_module: str = "scripts.strategies.demo_crossover_5m"
    fee_taker: float = 0.0002
    fee_maker: float = 0.0
    fee_units: str = "rate"
    risk_per_trade: float = 0.03
    prefill_bars: int = 0
    prefill_write_candles: int = 1
    prefill_seed_paper: int = 0



def _interval_ms(interval: str) -> int:
    if not interval:
        return 0
    interval = interval.strip().lower()
    if interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    if interval.endswith("h"):
        return int(interval[:-1]) * 60 * 60_000
    if interval.endswith("d"):
        return int(interval[:-1]) * 24 * 60 * 60_000
    return 0


def _log(msg: str) -> None:
    print(msg, flush=True)
    if _LOG_FILE is None:
        return
    try:
        _LOG_FILE.write(msg + "\n")
        _LOG_FILE.flush()
        os.fsync(_LOG_FILE.fileno())
    except Exception:
        return


def _init_logging(log_path: str) -> None:
    global _LOG_FILE
    if _LOG_FILE is not None:
        return
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    _LOG_FILE = open(log_path, "a", encoding="utf-8", newline="\n")


def _close_logging() -> None:
    global _LOG_FILE
    if _LOG_FILE is None:
        return
    _LOG_FILE.close()
    _LOG_FILE = None


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _stop_requested(stop_file: str) -> bool:
    return bool(stop_file) and os.path.exists(stop_file)


def _clear_stale_stopfile(stop_file: str) -> None:
    if not stop_file or not os.path.exists(stop_file):
        return
    try:
        os.remove(stop_file)
        _log(f"[WS] WARN removed stale stopfile: {stop_file}")
    except Exception:
        _log(f"[WS] WARN could not remove stale stopfile: {stop_file}")


def _write_pid(pid_path: str) -> None:
    os.makedirs(os.path.dirname(pid_path) or ".", exist_ok=True)
    with open(pid_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(str(os.getpid()))
        f.flush()
        os.fsync(f.fileno())


def _cleanup_pid(pid_path: str) -> None:
    if pid_path and os.path.exists(pid_path):
        os.remove(pid_path)


async def _subscribe(ws: websockets.WebSocketClientProtocol, symbol: str, interval: str) -> None:
    mexc_payload, fallback_payload = build_kline_subscribe_payload(symbol, interval)
    await ws.send(json.dumps(mexc_payload))
    await ws.send(json.dumps(fallback_payload))


async def run_shadow(cfg: RunConfig) -> None:
    _write_pid(cfg.pid_path)
    trade_sink = ShadowTradeSink(cfg.out_dir)
    classifier = MessageClassifier()
    unknown_samples = SampleRecorder(cfg.unknown_samples_path, cfg.samples_limit)
    parse_fail_samples = SampleRecorder(cfg.parse_fail_samples_path, cfg.samples_limit)
    aggregator = CandleAggregator(cfg.symbol, cfg.interval, _interval_ms(cfg.interval))
    paper_engine: Optional[PaperEngine] = None
    strategy = None
    if cfg.mode == "paper":
        os.environ["FEE_TAKER"] = str(getattr(cfg, "fee_taker", os.getenv("FEE_TAKER", "0.0002")))
        os.environ["FEE_MAKER"] = str(getattr(cfg, "fee_maker", os.getenv("FEE_MAKER", "0.0")))
        os.environ["FEE_UNITS"] = str(getattr(cfg, "fee_units", os.getenv("FEE_UNITS", "rate")))
        os.environ["RISK_PER_TRADE"] = str(getattr(cfg, "risk_per_trade", os.getenv("RISK_PER_TRADE", "0.03")))
        strategy = load_strategy(cfg.strategy_module)
        paper_config = PaperConfig(starting_equity=cfg.start_equity)
        telegram_enabled = os.getenv("TELEGRAM_ENABLED", "").strip() == "1"
        notifier = TelegramNotifier() if telegram_enabled else None
        if notifier is not None:
            try:
                notifier.send_message("bot inicializado", event_type="INIT")
            except Exception as exc:
                _log(f"[TG] init notification failed: {exc}")
        on_event = notifier.notify_vip_event if notifier is not None else None
        paper_engine = PaperEngine(
            paper_config,
            cfg.log_dir,
            notifier=None,
            symbol=cfg.symbol,
            tf=cfg.interval,
            exchange=cfg.exchange,
            on_event=on_event,
        )

    counters = {
        "rx_total": 0,
        "pong_total": 0,
        "ping_total": 0,
        "kline_total": 0,
        "kline_closed_total": 0,
        "parse_fail_total": 0,
        "unknown_total": 0,
        "trade_total": 0,
    }
    last_rx_ts: Optional[int] = None
    last_data_ts: Optional[int] = None
    last_kline_rx_ts: Optional[int] = None
    last_close_ts: Optional[int] = None
    last_pong_ts: Optional[int] = None
    last_ping_ts: Optional[int] = None
    data_stall_attempts = 0
    fallback_enabled = False

    # --- Startup Prefill (REST) ---
    if cfg.prefill_bars and int(cfg.prefill_bars) > 0 and str(cfg.exchange).lower() == "mexc":
        try:
            candles = fetch_contract_klines(cfg.symbol, cfg.interval, int(cfg.prefill_bars))
            if candles:
                _log(f"[PREFILL] fetched={len(candles)} tf={cfg.interval} symbol={cfg.symbol}")
                # Write candles for visibility/debug (optional)
                if int(cfg.prefill_write_candles) == 1:
                    for c in candles:
                        trade_sink.append_candle(c)
                # Let the strategy build its internal buffers / higher TF context
                if cfg.mode == "paper" and strategy is not None:
                    if hasattr(strategy, "prefill"):
                        try:
                            strategy.prefill(candles)
                        except Exception as exc:
                            _log(f"[PREFILL] strategy.prefill failed: {exc}")
                    else:
                        # Fallback: feed candles one by one (signals ignored)
                        for c in candles:
                            try:
                                strategy.on_candle(c, {})
                            except Exception:
                                break
                # Optionally seed paper engine (usually unnecessary)
                if cfg.mode == "paper" and paper_engine is not None and int(cfg.prefill_seed_paper) == 1:
                    for c in candles:
                        try:
                            paper_engine.on_candle(c, None)
                        except Exception:
                            pass
                last_close_ts = int(candles[-1].get("close_ts") or 0) or last_close_ts
                last_data_ts = last_close_ts
            else:
                _log("[PREFILL] WARN no candles returned")
        except Exception as exc:
            _log(f"[PREFILL] WARN failed: {exc}")


    async def _heartbeat_loop(connected_since: Optional[float]) -> None:
        while True:
            await asyncio.sleep(cfg.log_every_sec)
            up_sec = 0.0
            if connected_since is not None:
                up_sec = max(0.0, time.monotonic() - connected_since)
            _log(
                "[WS] HEALTH "
                f"up={up_sec:.1f}s rx={counters['rx_total']} "
                f"ping={counters['ping_total']} pong={counters['pong_total']} "
                f"kline={counters['kline_total']} closed={counters['kline_closed_total']} "
                f"parse_fail={counters['parse_fail_total']} unknown={counters['unknown_total']} "
                f"last_close_ts={last_close_ts} last_pong_ts={last_pong_ts}"
            )

    attempt = 0
    while True:
        if _stop_requested(cfg.stop_file):
            _log("[WS] STOP_SHADOW detected, exiting")
            break
        delay = min(60, 2 ** attempt)
        if attempt > 0:
            await asyncio.sleep(delay + random.uniform(0, delay * 0.5))
        ws = None
        heartbeat_task = None
        ping_task = None
        connected_since = None
        last_frame_monotonic = time.monotonic()
        try:
            _log("[WS] CONNECT")
            ws = await websockets.connect(cfg.ws_url, ping_interval=None, ping_timeout=None, max_queue=None)
            connected_since = time.monotonic()
            last_pong_ts = int(time.time() * 1000)
            await _subscribe(ws, cfg.symbol, cfg.interval)
            _log("[WS] SUB_SENT")
            heartbeat_task = asyncio.create_task(_heartbeat_loop(connected_since))

            async def _ping_loop() -> None:
                nonlocal last_ping_ts
                while True:
                    await ws.send(json.dumps({"method": "ping"}))
                    counters["ping_total"] += 1
                    last_ping_ts = int(time.time() * 1000)
                    if cfg.ping_log_every > 0 and counters["ping_total"] % cfg.ping_log_every == 0:
                        _log(f"[WS] PING sent n={counters['ping_total']}")
                    await asyncio.sleep(cfg.ping_sec)

            ping_task = asyncio.create_task(_ping_loop())

            while True:
                if _stop_requested(cfg.stop_file):
                    _log("[WS] STOP_SHADOW detected, closing")
                    return
                if ping_task and ping_task.done():
                    ping_exc = ping_task.exception()
                    if ping_exc:
                        raise RuntimeError(f"Ping loop failed: {ping_exc}") from ping_exc
                    raise RuntimeError("Ping loop stopped")
                if time.monotonic() - last_frame_monotonic > cfg.idle_timeout_sec:
                    _log("[WS] IDLE_TIMEOUT")
                    raise asyncio.TimeoutError("Idle timeout")
                if last_pong_ts is not None:
                    pong_age_ms = int(time.time() * 1000) - last_pong_ts
                    if pong_age_ms > cfg.ping_timeout_sec * 1000:
                        _log("[WS] PONG timeout")
                        raise asyncio.TimeoutError("Pong timeout")
                stall_ts = last_kline_rx_ts or last_data_ts
                if (
                    (stall_ts and (time.time() * 1000 - stall_ts) > cfg.data_stall_sec * 1000)
                    or (
                        stall_ts is None
                        and counters["rx_total"] > 0
                        and connected_since is not None
                        and (time.monotonic() - connected_since) > cfg.data_stall_sec
                    )
                ):
                    _write_json(
                        cfg.status_path,
                        {
                            "reason": "data_stall",
                            "last_data_ts": stall_ts,
                            "last_kline_rx_ts": last_kline_rx_ts,
                            "last_rx_ts": last_rx_ts,
                            "rx_total": counters["rx_total"],
                            "kline_closed_total": counters["kline_closed_total"],
                            "trade_total": counters["trade_total"],
                        },
                    )
                    data_stall_attempts += 1
                    _log("[WS] DATA_STALL parsing_broken=1")
                    if data_stall_attempts <= cfg.data_stall_max_attempts:
                        raise asyncio.TimeoutError("Data stall")
                    return

                raw_msg = await asyncio.wait_for(ws.recv(), timeout=cfg.recv_timeout_sec)
                last_frame_monotonic = time.monotonic()
                last_rx_ts = int(time.time() * 1000)
                counters["rx_total"] += 1
                try:
                    msg = json.loads(raw_msg)
                except Exception:
                    counters["unknown_total"] += 1
                    unknown_samples.record("unknown", "json_decode", {"raw": str(raw_msg)[:2048]})
                    continue

                category, reason = classifier.classify(msg)
                if category == "pong":
                    counters["pong_total"] += 1
                    last_pong_ts = int(time.time() * 1000)
                    _log("[WS] PONG")
                    continue
                if category in {"subscribe_ack", "system"}:
                    continue
                if category == "trade":
                    trade_event = parse_mexc_trade_message(msg)
                    if not trade_event:
                        counters["parse_fail_total"] += 1
                        parse_fail_samples.record("trade", "parse_fail", msg)
                        continue
                    counters["trade_total"] += 1
                    last_data_ts = trade_event.get("ts") or last_rx_ts
                    trade_sink.append_raw_event({"event_type": "trade", "payload": trade_event})
                    trade_sink.append_trade_event(trade_event)
                    if (
                        counters["kline_closed_total"] == 0
                        and not fallback_enabled
                        and connected_since is not None
                        and (time.monotonic() - connected_since) >= cfg.kline_fallback_after_sec
                    ):
                        fallback_enabled = True
                        _log("[WS] FALLBACK_AGG enabled (trade-based candles)")
                    if fallback_enabled and aggregator.interval_ms > 0:
                        agg_candle = aggregator.update_trade(trade_event)
                        if agg_candle:
                            msg = agg_candle
                            category = "kline"
                        else:
                            continue
                    else:
                        continue

                if category == "kline":
                    if isinstance(msg, dict) and "open_ts" in msg and "close_ts" in msg:
                        kline = msg
                    else:
                        kline, reason = parse_mexc_kline_message_with_reason(
                            msg, expected_symbol=cfg.symbol, expected_interval=cfg.interval
                        )
                    if not kline:
                        counters["parse_fail_total"] += 1
                        parse_fail_samples.record("kline", reason or "parse_fail", msg)
                        continue
                    fallback_enabled = False
                    counters["kline_total"] += 1
                    last_kline_rx_ts = int(time.time() * 1000)
                    if not is_kline_closed(kline, int(time.time() * 1000)):
                        continue
                    if not should_accept_closed_kline(kline, last_close_ts, int(time.time() * 1000)):
                        continue
                    trade_sink.append_raw_event({"event_type": "kline", "payload": kline})
                    trade_sink.append_candle(kline)
                    last_close_ts = kline.get("close_ts")
                    last_data_ts = last_close_ts
                    counters["kline_closed_total"] += 1
                    if cfg.mode == "paper" and paper_engine and strategy:
                        signal = normalize_strategy_output(strategy.on_candle(kline, {}))
                        paper_engine.on_candle(kline, signal)
                    _write_json(
                        cfg.health_path,
                        {
                            "connected": True,
                            "rx_total": counters["rx_total"],
                            "kline_total": counters["kline_total"],
                            "kline_closed_total": counters["kline_closed_total"],
                            "parse_fail_total": counters["parse_fail_total"],
                            "unknown_total": counters["unknown_total"],
                            "last_close_ts": last_close_ts,
                            "pong_total": counters["pong_total"],
                            "last_pong_ts": last_pong_ts,
                        },
                    )
                else:
                    counters["unknown_total"] += 1
                    unknown_samples.record(category, reason, msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log(f"[WS] ERROR {exc}")
        finally:
            if ws is not None:
                with contextlib.suppress(Exception):
                    await ws.close()
            if heartbeat_task:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
            if ping_task:
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task
        attempt += 1

    _cleanup_pid(cfg.pid_path)


def parse_args() -> RunConfig:
    parser = argparse.ArgumentParser(description="Shadow runner (data or paper mode)")
    parser.add_argument("--exchange", default=os.getenv("EXCHANGE", "mexc"))
    parser.add_argument("--ws_url", default=os.getenv("WS_URL", "wss://contract.mexc.com/edge"))
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "SOLUSDT"))
    parser.add_argument("--tf", dest="interval", default=os.getenv("TF", "5m"))
    parser.add_argument("--mode", default=os.getenv("MODE", os.getenv("PAPER_TRADING", "0")))
    parser.add_argument("--out_dir", default=os.getenv("OUT_DIR", "./results/shadow"))
    parser.add_argument("--log_dir", default=os.getenv("LOG_DIR", "./logs"))
    parser.add_argument("--stop_file", default=os.getenv("STOP_FILE", "./results/STOP_SHADOW.txt"))
    parser.add_argument("--status_path", default=os.getenv("STATUS_PATH", "./results/shadow/status.json"))
    parser.add_argument("--health_path", default=os.getenv("HEALTH_PATH", "./results/shadow/ws_health.json"))
    parser.add_argument("--manifest_path", default=os.getenv("MANIFEST_PATH", "./results/shadow/run_manifest.json"))
    parser.add_argument("--pid_path", default=os.getenv("PID_PATH", ""))
    parser.add_argument("--start_equity", type=float, default=float(os.getenv("START_EQUITY", "5000")))
    parser.add_argument("--fee_taker", type=float, default=float(os.getenv("FEE_TAKER", "0.0002")))
    parser.add_argument("--fee_maker", type=float, default=float(os.getenv("FEE_MAKER", "0.0")))
    parser.add_argument("--fee_units", default=os.getenv("FEE_UNITS", "rate"), choices=["rate", "percent", "string"])
    parser.add_argument("--risk_per_trade", type=float, default=float(os.getenv("RISK_PER_TRADE", os.getenv("RISK_PCT", "0.03"))))
    parser.add_argument(
        "--strategy_module",
        default=os.getenv("STRATEGY_MODULE", "scripts.strategies.demo_crossover_5m"),
    )
    parser.add_argument(
        "--prefill_bars",
        type=int,
        default=int(os.getenv("PREFILL_BARS", "-1")),
        help="History candles to prefill before WS loop (paper mode auto=300, disable=0)",
    )
    parser.add_argument(
        "--prefill_write_candles",
        type=int,
        default=int(os.getenv("PREFILL_WRITE_CANDLES", "1")),
        help="If 1, write prefill candles into shadow_candles.csv",
    )
    parser.add_argument(
        "--prefill_seed_paper",
        type=int,
        default=int(os.getenv("PREFILL_SEED_PAPER", "0")),
        help="If 1, also feed prefill candles into PaperEngine (usually unnecessary)",
    )

    args = parser.parse_args()
    repo_root = Path(os.getenv("ROOT", Path(__file__).resolve().parents[1])).resolve()
    if not repo_root.exists():
        raise RuntimeError(f"Repo root not found: {repo_root}")

    def _resolve_path(raw_path: str) -> str:
        if not raw_path:
            return ""
        path = Path(raw_path)
        if not path.is_absolute():
            path = repo_root / path
        return str(path.resolve())

    mode = str(args.mode).lower()
    if mode in {"1", "true", "yes", "paper"}:
        mode = "paper"
    else:
        mode = "data"
    # Prefill: if PREFILL_BARS=-1 (default), enable 300 bars for paper mode, else 0.
    auto_prefill_bars = int(args.prefill_bars)
    if auto_prefill_bars < 0:
        auto_prefill_bars = 300 if mode == "paper" else 0
    if auto_prefill_bars < 0:
        auto_prefill_bars = 0
    out_dir = _resolve_path(args.out_dir)
    log_dir = _resolve_path(args.log_dir)
    stop_file = _resolve_path(args.stop_file)
    status_path = _resolve_path(args.status_path)
    health_path = _resolve_path(args.health_path)
    manifest_path = _resolve_path(args.manifest_path)
    log_path = os.path.join(log_dir, "shadow_run.log")
    _init_logging(log_path)
    _clear_stale_stopfile(stop_file)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    _write_json(
        manifest_path,
        {
            "exchange": args.exchange,
            "ws_url": args.ws_url,
            "symbol": args.symbol,
            "interval": args.interval,
            "mode": mode,
            "out_dir": out_dir,
            "log_dir": log_dir,
            "stop_file": stop_file,
            "start_equity": args.start_equity,
            "fee_taker": args.fee_taker,
            "fee_maker": args.fee_maker,
            "fee_units": args.fee_units,
            "risk_per_trade": args.risk_per_trade,
            "strategy_module": args.strategy_module,
            "prefill_bars": auto_prefill_bars,
            "prefill_write_candles": int(args.prefill_write_candles),
            "prefill_seed_paper": int(args.prefill_seed_paper),
            "repo_root": str(repo_root),
        },
    )
    pid_path = _resolve_path(args.pid_path) if args.pid_path else os.path.join(out_dir, "shadow.pid")
    return RunConfig(
        exchange=args.exchange,
        ws_url=args.ws_url,
        symbol=args.symbol,
        interval=args.interval,
        mode=mode,
        out_dir=out_dir,
        log_dir=log_dir,
        stop_file=stop_file,
        status_path=status_path,
        health_path=health_path,
        manifest_path=manifest_path,
        pid_path=pid_path,
        unknown_samples_path=_resolve_path(os.getenv("WS_UNKNOWN_SAMPLES", "./logs/ws_unknown_samples.jsonl")),
        parse_fail_samples_path=_resolve_path(os.getenv("WS_PARSE_FAIL_SAMPLES", "./logs/ws_parse_fail_samples.jsonl")),
        samples_limit=int(os.getenv("WS_SAMPLES_LIMIT", "50")),
        ping_sec=int(os.getenv("PING_SEC", "15")),
        ping_timeout_sec=int(os.getenv("PING_TIMEOUT_SEC", "45")),
        ping_log_every=int(os.getenv("PING_LOG_EVERY", "4")),
        start_equity=args.start_equity,
        strategy_module=args.strategy_module,
        fee_taker=args.fee_taker,
        fee_maker=args.fee_maker,
        fee_units=args.fee_units,
        risk_per_trade=args.risk_per_trade,
        prefill_bars=auto_prefill_bars,
        prefill_write_candles=int(args.prefill_write_candles),
        prefill_seed_paper=int(args.prefill_seed_paper),
    )


def main() -> None:
    cfg = parse_args()
    try:
        asyncio.run(run_shadow(cfg))
    except KeyboardInterrupt:
        _log("[WS] KeyboardInterrupt")
    finally:
        _cleanup_pid(cfg.pid_path)
        _close_logging()


if __name__ == "__main__":
    main()


