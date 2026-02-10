@echo off
setlocal EnableExtensions EnableDelayedExpansion
rem ---------------------------------------------
rem run_shadow_mexc_sol_5m.bat
rem Shadow WS runner plug-and-play (SOLUSDT 5m).
rem ---------------------------------------------

rem ===== User toggles =====
set "MODE=paper"
set "SYMBOL=SOLUSDT"
set "TF=5m"
set "START_EQUITY=5000"
set "FEE_TAKER=0.0002"
set "FEE_MAKER=0.0"
set "FEE_UNITS=rate"
set "RISK_PER_TRADE=0.03"
set "STRATEGY_MODULE=scripts.strategies.orchestrator"
set "ORCH_STRATEGY_LIST=scripts.solusdt_vwap_bot,scripts.strategies.vpc_retest"
set "ORCH_POLICY=best_confidence"
set "ORCH_COOLDOWN_BARS=3"
set "ORCH_ALLOW_EXIT=0"
rem ========================

set "SCRIPT_DIR=%~dp0"
if not defined SCRIPT_DIR (
  echo [FATAL] SCRIPT_DIR vacio. No se puede continuar.
  exit /b 1
)

set "REPO_DIR=%SCRIPT_DIR%"
if not exist "%REPO_DIR%\.git" (
  set "REPO_DIR=%SCRIPT_DIR%.."
)

cd /d "%REPO_DIR%"
if errorlevel 1 (
  echo [FATAL] No se pudo cambiar al repo: "%REPO_DIR%"
  exit /b 1
)

set "ROOT=%CD%"
set "PYTHONPATH=%ROOT%"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"
set "RUN_MODE=PIPELINE"

set "STOPFILE=%ROOT%\results\STOP_SHADOW.txt"
set "LOGFILE=%ROOT%\logs\shadow_console.log"
set "OUTDIR=%ROOT%\results\shadow"
set "PIDFILE=%OUTDIR%\shadow.pid"
rem === CODEX_PATCH_BEGIN: BAT_STATUS_WIRING ===
set "SHADOW_PREFILL_LOG=1"
set "STRATEGY_STATUS_FILE=%OUTDIR%\strategy_status.json"
rem === CODEX_PATCH_END: BAT_STATUS_WIRING ===

if not exist "%ROOT%\results" mkdir "%ROOT%\results"
if not exist "%ROOT%\results\health" mkdir "%ROOT%\results\health"
if not exist "%ROOT%\results\robust" mkdir "%ROOT%\results\robust"
if not exist "%ROOT%\results\promotions" mkdir "%ROOT%\results\promotions"
if not exist "%OUTDIR%" mkdir "%OUTDIR%"
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"

set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
if not exist "%PYTHON%" (
  set "PYTHON=python"
)

if not defined WS_URL set "WS_URL=wss://contract.mexc.com/edge"
if not defined OUT set "OUT=%OUTDIR%"
if not defined LOG set "LOG=%LOGFILE%"
if not defined STOPFILE set "STOPFILE=%STOPFILE%"

echo ---------------------------------------------
echo [INFO] SCRIPT_DIR=%SCRIPT_DIR%
echo [INFO] ROOT=%ROOT%
echo [INFO] PYTHON=%PYTHON%
echo [INFO] PYTHONPATH=%PYTHONPATH%
echo [INFO] RUN_MODE=%RUN_MODE%
echo [INFO] WS_URL=%WS_URL%
echo [INFO] SYMBOL=%SYMBOL%
echo [INFO] TF=%TF%
echo [INFO] MODE=%MODE%
echo [INFO] START_EQUITY=%START_EQUITY%
echo [INFO] FEE_TAKER=%FEE_TAKER%
echo [INFO] FEE_MAKER=%FEE_MAKER%
echo [INFO] FEE_UNITS=%FEE_UNITS%
echo [INFO] RISK_PER_TRADE=%RISK_PER_TRADE%
echo [INFO] STRATEGY_MODULE=%STRATEGY_MODULE%
echo [INFO] OUT=%OUT%
echo [INFO] LOG=%LOG%
echo [INFO] STOPFILE=%STOPFILE%
echo [INFO] PIDFILE=%PIDFILE%
echo ---------------------------------------------

if exist "%STOPFILE%" del /f /q "%STOPFILE%"

"%PYTHON%" --version
if errorlevel 1 (
  echo [FATAL] Python no disponible.
  pause
  exit /b 1
)

echo [INFO] Ejecutando runner...
powershell -NoProfile -ExecutionPolicy Bypass -Command "& { & '%PYTHON%' -u -m scripts.run_shadow --exchange mexc --ws_url '%WS_URL%' --symbol '%SYMBOL%' --tf '%TF%' --mode '%MODE%' --start_equity '%START_EQUITY%' --strategy_module '%STRATEGY_MODULE%' --fee_taker '%FEE_TAKER%' --fee_maker '%FEE_MAKER%' --fee_units '%FEE_UNITS%' --risk_per_trade '%RISK_PER_TRADE%' --out_dir '%OUT%' --log_dir '%ROOT%\\logs' --stop_file '%STOPFILE%' *>&1 | Tee-Object -FilePath '%LOG%' -Append; exit $LASTEXITCODE }"

set "RUN_EXIT=%ERRORLEVEL%"
if not "%RUN_EXIT%"=="0" (
  echo [ERROR] Runner termino con code %RUN_EXIT%
  echo Revisa log: %LOG%
  pause
  exit /b %RUN_EXIT%
)

echo [OK] Shadow finalizo. Logs: %LOG%
echo Para cortar: kill_shadow.bat

endlocal
