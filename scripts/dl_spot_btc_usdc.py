from __future__ import annotations

from scripts.dl_spot_ohlcv import run_download


def main() -> None:
    run_download(
        exchange="mexc",
        symbol="BTC/USDC",
        timeframe="5m",
        since="",
        until="",
        out="Datasets/BTCUSDC/5m/BTCUSDC_5m_full.csv",
        limit=500,
        sleep_ms=200,
    )


if __name__ == "__main__":
    main()
