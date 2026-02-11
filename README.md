# Retail

## Shadow MVP Quickstart (Windows)

### Run
1) Start the shadow runner:
   ```powershell
   .\run_shadow_mexc_sol_5m.bat
   ```

   By default it runs in paper-trading mode. To run data-only, set `MODE=data` near the top of the bat.

2) Tail logs:
   ```powershell
   Get-Content .\logs\shadow_console.log -Tail 80 -Wait
   ```

3) Check outputs:
   ```powershell
   dir .\results\shadow
   ```

### Outputs Produced
* `results\shadow\shadow_events.jsonl` — raw-ish event stream (JSONL).
* `results\shadow\shadow_candles.csv` — normalized candle stream (CSV).
* `results\shadow\shadow_trade_events.csv` — normalized trade events (CSV, if trade stream is enabled).
* `results\shadow\ws_health.json` — status/health snapshot.
* `results\shadow\shadow.pid` — PID for graceful stop.
* `logs\paper_orders.csv` — simulated orders (paper mode).
* `logs\paper_trades.csv` — simulated trades (paper mode).
* `logs\paper_equity.csv` — simulated equity curve (paper mode).

### Modes
* **Data-only:** set `MODE=data` in `run_shadow_mexc_sol_5m.bat`. This connects and writes candles but never simulates trades.
* **Paper trading:** set `MODE=paper` (default). Uses `STRATEGY_MODULE` (default `scripts.strategies.demo_crossover_5m`) to generate signals and logs simulated trades/equity to `logs\`.

### Stop
* Create STOP file manually:
  ```powershell
  ni .\results\STOP_SHADOW.txt
  ```
* Or run the helper:
  ```powershell
  .\kill_shadow.bat
  ```

### Optional Telegram Notifications
If you want Telegram messages for entries/exits, set these environment variables before running:
* `TELEGRAM_BOT_TOKEN`
* `TELEGRAM_CHAT_ID`

### Troubleshooting
* Logs are written to `logs\shadow_console.log` (stdout) and `logs\shadow_run.log` (runner file log).
* If the runner hangs, check `results\shadow\ws_health.json` for last heartbeat.
* Smoke test imports from repo root:
  ```powershell
  python -c "import scripts.shadow_trade_sink as s; print('OK')"
  ```


### Smoke test: Shadow -> Trade Event -> Telegram VIP
Ejecuta el pipeline paper/shadow sin WebSocket, usa CSV histórico y valida que se generen trades + mensajes VIP contextuales.

Dry run (solo consola):
```powershell
set TELEGRAM_ENABLED=1
set TELEGRAM_DRY_RUN=1
.\.venv\Scripts\python.exe .\scripts\smoke_shadow_to_telegram.py --csv .\Datasets\SOLUSDT\5m\SOLUSDT_5m_um.csv --symbol SOLUSDT --tf 5m
```

Envío real:
```powershell
set TELEGRAM_ENABLED=1
set TELEGRAM_DRY_RUN=0
set TELEGRAM_BOT_TOKEN=xxxx
set TELEGRAM_CHAT_ID=yyyy
.\.venv\Scripts\python.exe .\scripts\smoke_shadow_to_telegram.py --csv .\Datasets\SOLUSDT\5m\SOLUSDT_5m_um.csv --symbol SOLUSDT --tf 5m
```


### Smoke test E2E: shadow(paper) -> trade -> Telegram
PowerShell env:
```powershell
$env:TELEGRAM_ENABLED="1"
$env:TELEGRAM_BOT_TOKEN="..."
$env:TELEGRAM_CHAT_ID="-100..."
```

Run:
```powershell
.\.venv\Scripts\python.exe .\scripts\smoke_test_telegram_e2e.py --csv .\Datasets\SOLUSDT\5m\SOLUSDT_5m_um.csv
```

### One-click .bat: shadow + Telegram VIP
Editá este archivo una sola vez con tu token/chat y luego corré:
```powershell
.\run_shadow_mexc_sol_5m_telegram.bat
```

Archivo:
* `run_shadow_mexc_sol_5m_telegram.bat`


### Backtest BTCUSDT (dataset BINANCE 5m) con comparación de ejecución
> Recomendación: usar `--exec_policy legacy` para research hasta validar estadísticamente cualquier política alternativa de ejecución.

Dataset esperado:
* `Datasets/BINANCE/BTCUSDT/5m/BTCUSDT_5m_full.csv`

Comando LEGACY (baseline):
```powershell
python -m scripts.solusdt_vwap_bot_spot --bt --csv .\Datasets\BINANCE\BTCUSDT\5m\BTCUSDT_5m_full.csv --outdir .\results\exec_compare\btcusdt_legacy --exec_policy legacy --slip_bps_entry 2 --slip_bps_stop 4 --tp_bidask_half_spread_bps 0.5
```

Comando maker_first_fast:
```powershell
python -m scripts.solusdt_vwap_bot_spot --bt --csv .\Datasets\BINANCE\BTCUSDT\5m\BTCUSDT_5m_full.csv --outdir .\results\exec_compare\btcusdt_maker_first_fast --exec_policy maker_first_fast --maker_try_enabled 1 --maker_try_ttl_steps 1 --fallback_limit_aggr_enabled 1 --market_last_resort_enabled 1 --market_vol_k 0.15 --aggr_extra_bps 0.1 --slip_bps_entry 2 --slip_bps_stop 4 --tp_bidask_half_spread_bps 0.5 --exec_self_check 1
```

Atajo Windows (.bat):
```powershell
.\run_bt_spot_btcusdt_exec_compare.bat
```
