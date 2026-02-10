@echo off
setlocal EnableExtensions EnableDelayedExpansion
rem ---------------------------------------------
rem kill_shadow.bat
rem Solicita STOP + intenta cierre suave, luego fallback kill.
rem ---------------------------------------------

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
set "ROOT=%CD%"

set "SHADOW_DIR=%ROOT%\results\shadow"
if not exist "%ROOT%\results" mkdir "%ROOT%\results"
if not exist "%SHADOW_DIR%" mkdir "%SHADOW_DIR%"
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"

set "STOP_FILE=%ROOT%\results\STOP_SHADOW.txt"
set "PID_FILE=%SHADOW_DIR%\shadow.pid"
set "LOG_FILE=%ROOT%\logs\kill_shadow.log"

echo [%DATE% %TIME%] STOP solicitado > "%STOP_FILE%"
echo [%DATE% %TIME%] STOP file creado: %STOP_FILE% >> "%LOG_FILE%"
echo [OK] STOP file creado: %STOP_FILE%

set "PID="
if exist "%PID_FILE%" (
  for /f "usebackq delims=" %%P in ("%PID_FILE%") do set "PID=%%P"
)

if defined PID (
  echo [INFO] PID detectado: %PID%
  echo [%DATE% %TIME%] PID detectado: %PID% >> "%LOG_FILE%"
  powershell -NoProfile -Command "Start-Sleep -Seconds 5; $p=Get-Process -Id %PID% -ErrorAction SilentlyContinue; if($p){Stop-Process -Id %PID% -Force; exit 1} else {exit 0}"
  if errorlevel 1 (
    echo [WARN] Proceso %PID% seguia vivo, se envio Stop-Process.
    echo [%DATE% %TIME%] Stop-Process enviado a %PID% >> "%LOG_FILE%"
  ) else (
    echo [OK] Proceso %PID% ya se habia detenido.
    echo [%DATE% %TIME%] Proceso %PID% detenido. >> "%LOG_FILE%"
  )
) else (
  echo [WARN] PID file no encontrado. Buscando por comando...
  echo [%DATE% %TIME%] PID file no encontrado. >> "%LOG_FILE%"
  powershell -NoProfile -Command "$procs=Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'scripts.shadow_ws_runner|scripts.run_shadow' }; foreach($p in $procs){ Stop-Process -Id $p.ProcessId -Force; Write-Host ('Killed PID ' + $p.ProcessId); }"
  echo [%DATE% %TIME%] Fallback kill aplicado (si habia procesos). >> "%LOG_FILE%"
)

if exist "%SHADOW_DIR%\ws_health.json" (
  echo ---- Ultimo ws_health.json ----
  type "%SHADOW_DIR%\ws_health.json"
)

pause

endlocal
