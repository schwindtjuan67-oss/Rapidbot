import pytest

from scripts.solusdt_vwap_bot import compute_risk_usd, parse_fee_rate


def test_fee_conversion():
    assert parse_fee_rate("0.02%", "string", "fee_taker") == pytest.approx(0.0002)
    assert parse_fee_rate(0.02, "percent", "fee_taker") == pytest.approx(0.0002)
    assert parse_fee_rate(0.0002, "rate", "fee_taker") == pytest.approx(0.0002)
    with pytest.raises(ValueError, match="fee_taker parece porcentaje"):
        parse_fee_rate(0.02, "rate", "fee_taker")


def test_fee_amount_sanity():
    notional = 1000.0
    taker_fee = notional * parse_fee_rate(0.0002, "rate", "fee_taker")
    maker_fee = notional * parse_fee_rate(0.0, "rate", "fee_maker")
    assert taker_fee == pytest.approx(0.2)
    assert maker_fee == pytest.approx(0.0)


def test_risk_per_trade():
    assert compute_risk_usd(5000, 0.03) == pytest.approx(150.0)
    assert compute_risk_usd(750, 0.03) == pytest.approx(22.5)
