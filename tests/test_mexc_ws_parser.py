import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from adapters.mexc_ws import parse_mexc_kline_message_with_reason


def test_parse_mexc_kline_from_sample_with_amount_qty():
    raw = """
    {"symbol":"SOL_USDT","data":{"symbol":"SOL_USDT","interval":"Min5","t":1770174300,"o":98.11,"c":98.35,"h":98.4,"l":98.11,"a":2322734.44,"q":236245,"ro":98.11,"rc":98.35,"rh":98.4,"rl":98.11},"channel":"push.kline","ts":1770174432306}
    """.strip()
    msg = json.loads(raw)
    payload, reason = parse_mexc_kline_message_with_reason(
        msg, expected_symbol="SOLUSDT", expected_interval="5m"
    )
    assert reason == "ok"
    assert payload is not None
    assert payload["symbol"] == "SOLUSDT"
    assert payload["interval"] == "5m"
    assert payload["open_ts"] == 1770174300 * 1000
    assert payload["close_ts"] == payload["open_ts"] + 5 * 60 * 1000 - 1
    assert payload["volume"] == 2322734.44
    assert payload["open"] == 98.11
    assert payload["high"] == 98.4
    assert payload["low"] == 98.11
    assert payload["close"] == 98.35


def test_parse_mexc_kline_doc_like_verbose_interval():
    msg = {
        "channel": "push.kline",
        "symbol": "SOL_USDT",
        "data": {
            "symbol": "SOL_USDT",
            "interval": "Min15",
            "t": 1700000000,
            "o": "10",
            "h": "12",
            "l": "9",
            "c": "11",
            "v": "100",
        },
    }
    payload, reason = parse_mexc_kline_message_with_reason(
        msg, expected_symbol="SOL_USDT", expected_interval="Min15"
    )
    assert reason == "ok"
    assert payload is not None
    assert payload["symbol"] == "SOL_USDT"
    assert payload["interval"] == "Min15"
    assert payload["open_ts"] == 1700000000 * 1000


def test_parse_mexc_kline_marks_intrabar_open():
    msg = {
        "channel": "push.kline",
        "symbol": "SOL_USDT",
        "data": {
            "interval": "Min5",
            "t": 1770177000,
            "o": 98.96,
            "c": 98.99,
            "h": 98.99,
            "l": 98.93,
            "a": 170768.603,
            "q": 17258,
        },
        "ts": 1770177299639,
    }
    payload, reason = parse_mexc_kline_message_with_reason(
        msg, expected_symbol="SOLUSDT", expected_interval="5m"
    )
    assert reason == "ok"
    assert payload is not None
    assert payload["symbol"] == "SOLUSDT"
    assert payload["interval"] == "5m"
    assert payload["open_ts"] == 1770177000 * 1000
    assert payload["is_closed"] is False
