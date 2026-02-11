@echo off
setlocal

REM Run from repo root. Dataset from: Datasets\BINANCE\BTCUSDT\5m\BTCUSDT_5m_full.csv
set CSV_PATH=Datasets\BINANCE\BTCUSDT\5m\BTCUSDT_5m_full.csv

if not exist "%CSV_PATH%" (
  echo [ERROR] CSV not found: %CSV_PATH%
  exit /b 1
)

echo [1/2] LEGACY execution...
python -m scripts.solusdt_vwap_bot_spot --bt --csv "%CSV_PATH%" --outdir "results\exec_compare\btcusdt_legacy" --exec_policy legacy --slip_bps_entry 2 --slip_bps_stop 4 --tp_bidask_half_spread_bps 0.5
if errorlevel 1 exit /b %errorlevel%

echo [2/2] maker_first_fast execution...
python -m scripts.solusdt_vwap_bot_spot --bt --csv "%CSV_PATH%" --outdir "results\exec_compare\btcusdt_maker_first_fast" --exec_policy maker_first_fast --maker_try_enabled 1 --maker_try_ttl_steps 1 --fallback_limit_aggr_enabled 1 --market_last_resort_enabled 1 --market_vol_k 0.15 --aggr_extra_bps 0.1 --slip_bps_entry 2 --slip_bps_stop 4 --tp_bidask_half_spread_bps 0.5 --exec_self_check 1
if errorlevel 1 exit /b %errorlevel%

echo Done. Check:
echo   results\exec_compare\btcusdt_legacy
echo   results\exec_compare\btcusdt_maker_first_fast

endlocal
