@echo off
setlocal EnableExtensions EnableDelayedExpansion
rem ---------------------------------------------
rem run_shadow_mexc_sol_5m_telegram.bat
rem One-click wrapper: shadow paper + Telegram VIP.
rem ---------------------------------------------

rem ===== Telegram (plug and play) =====
set "TELEGRAM_ENABLED=1"
set "FEE_TAKER=0.0002"
set "FEE_MAKER=0.0"
set "FEE_UNITS=rate"
set "RISK_PER_TRADE=0.03"
set "TELEGRAM_BOT_TOKEN=8534978315:AAFBInntCYbcQaJYGUoMgJz73I6h9DPvdiI"
set "TELEGRAM_CHAT_ID=-1003755847413"
rem ====================================

rem ===== Prefill observability (optional, no spam) =====
set "SHADOW_PREFILL_LOG=1"
set "STRATEGY_STATUS_FILE=%ROOT%\results\shadow\strategy_status.json"
rem =====================================================

if "%TELEGRAM_BOT_TOKEN%"=="" (
  echo [FATAL] TELEGRAM_BOT_TOKEN vacio.
  exit /b 1
)
if "%TELEGRAM_CHAT_ID%"=="" (
  echo [FATAL] TELEGRAM_CHAT_ID vacio.
  exit /b 1
)

set "SCRIPT_DIR=%~dp0"
if not exist "%SCRIPT_DIR%run_shadow_mexc_sol_5m.bat" (
  echo [FATAL] No se encontro run_shadow_mexc_sol_5m.bat en %SCRIPT_DIR%
  exit /b 1
)

echo [INFO] TELEGRAM_ENABLED=%TELEGRAM_ENABLED%
echo [INFO] TELEGRAM_CHAT_ID=%TELEGRAM_CHAT_ID%
echo [INFO] Ejecutando shadow con Telegram VIP activo...

call "%SCRIPT_DIR%run_shadow_mexc_sol_5m.bat"
set "RUN_EXIT=%ERRORLEVEL%"

if not "%RUN_EXIT%"=="0" (
  echo [ERROR] Shadow finalizo con code %RUN_EXIT%
  exit /b %RUN_EXIT%
)

echo [OK] Shadow + Telegram finalizado correctamente.
endlocal
