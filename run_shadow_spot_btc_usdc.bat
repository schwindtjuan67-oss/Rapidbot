@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "PYTHON=python"
if exist ".venv\Scripts\python.exe" set "PYTHON=.venv\Scripts\python.exe"

set "MODE=paper"
set "SYMBOL=BTCUSDC"
set "TF=5m"
set "START_EQUITY=1000"
set "FEE_TAKER=0.0"
set "FEE_MAKER=0.0"
set "FEE_UNITS=rate"
set "RISK_PER_TRADE=0.03"
set "STRATEGY_MODULE=scripts.solusdt_vwap_bot_spot"
set "OUT_DIR=%CD%\results\shadow_spot_btc_usdc"
set "STOP_FILE=%CD%\results\STOP_SHADOW_SPOT_BTC_USDC.txt"

if not exist "%CD%\results" mkdir "%CD%\results"
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

%PYTHON% -u -m scripts.run_shadow --exchange mexc --symbol "%SYMBOL%" --tf "%TF%" --mode "%MODE%" --start_equity "%START_EQUITY%" --strategy_module "%STRATEGY_MODULE%" --fee_taker "%FEE_TAKER%" --fee_maker "%FEE_MAKER%" --fee_units "%FEE_UNITS%" --risk_per_trade "%RISK_PER_TRADE%" --out_dir "%OUT_DIR%" --log_dir "%CD%\logs" --stop_file "%STOP_FILE%"

endlocal
