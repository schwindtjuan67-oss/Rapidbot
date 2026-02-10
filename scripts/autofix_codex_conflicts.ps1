# scripts/autofix_codex_conflicts.ps1
# Auto-resuelve 2 conflictos específicos:
# 1) run_shadow_mexc_sol_5m_telegram.bat -> deja placeholders (NO secrets)
# 2) scripts/run_shadow.py -> integra INIT + notifier wiring y quita markers

$ErrorActionPreference = "Stop"

function Get-ConflictBlocks([string]$text) {
  # Devuelve array de objetos con: full, head, incoming
  $pattern = '(?s)<<<<<<<.*?\r?\n(.*?)\r?\n=======\r?\n(.*?)\r?\n>>>>>>>.*?\r?\n'
  $matches = [regex]::Matches($text, $pattern)
  $blocks = @()
  foreach ($m in $matches) {
    $blocks += [pscustomobject]@{
      Full     = $m.Value
      Head     = $m.Groups[1].Value
      Incoming = $m.Groups[2].Value
    }
  }
  return $blocks
}

function Replace-First([string]$text, [string]$old, [string]$new) {
  $idx = $text.IndexOf($old)
  if ($idx -lt 0) { return $text }
  return $text.Substring(0, $idx) + $new + $text.Substring($idx + $old.Length)
}

function Fix-Bat([string]$path) {
  if (!(Test-Path $path)) { Write-Host "SKIP (no existe): $path"; return }

  $t = Get-Content $path -Raw
  if ($t -notmatch '<<<<<<<') { Write-Host "OK (sin conflicto): $path"; return }

  $blocks = Get-ConflictBlocks $t
  if ($blocks.Count -ne 1) {
    throw "Esperaba 1 bloque de conflicto en $path, encontré $($blocks.Count). Abort."
  }

  $b = $blocks[0]

  # Preferimos el lado HEAD (placeholders). Si por alguna razón el head tiene secrets, los sanitizamos igual.
  $resolved = $b.Head

  # Sanitizar: si quedó un token real, lo reemplazamos por placeholder
  $resolved = $resolved -replace '(?im)^\s*set\s+"?TELEGRAM_BOT_TOKEN=.*$', 'set "TELEGRAM_BOT_TOKEN=REEMPLAZAR_POR_TU_TOKEN"'
  $resolved = $resolved -replace '(?im)^\s*set\s+"?TELEGRAM_CHAT_ID=.*$', 'set "TELEGRAM_CHAT_ID=REEMPLAZAR_POR_TU_CHAT_ID"'

  $t2 = Replace-First $t $b.Full $resolved

  # Verificación: no deben quedar markers
  if ($t2 -match '<<<<<<<|=======|>>>>>>>') { throw "Quedaron markers en $path" }

  Set-Content -Path $path -Value $t2 -Encoding UTF8
  Write-Host "FIXED: $path"
}

function Fix-RunShadowPy([string]$path) {
  if (!(Test-Path $path)) { Write-Host "SKIP (no existe): $path"; return }

  $t = Get-Content $path -Raw
  if ($t -notmatch '<<<<<<<') { Write-Host "OK (sin conflicto): $path"; return }

  $blocks = Get-ConflictBlocks $t
  if ($blocks.Count -ne 1) {
    throw "Esperaba 1 bloque de conflicto en $path, encontré $($blocks.Count). Abort."
  }

  $b = $blocks[0]

  # Armamos un bloque “mergeado” que:
  # - mantiene el INIT message
  # - define on_event (si querés usarlo después)
  # - y deja listo el wiring para pasar notifier al engine
  #
  # Nota: NO tocamos el resto del archivo: solo reemplazamos el conflicto puntual.
  $merged = @"
if notifier is not None:
    try:
        notifier.send_message(
            f"bot inicializado|mode=paper symbol={cfg.symbol} tf={cfg.interval}",
            event_type="INIT",
        )
    except Exception as exc:
        _log(f"[TG] init notification failed: {exc}")

on_event = notifier.notify_vip_event if notifier is not None else None
"@

  $t2 = Replace-First $t $b.Full $merged

  # Ahora aseguramos que PaperEngine reciba notifier=notifier (y no notifier=None)
  # Sólo si encontramos el patrón notifier=None dentro de PaperEngine(...)
  $t2 = $t2 -replace '(?s)(PaperEngine\s*\(.*?)(\s*notifier\s*=\s*)None(\s*,)', '$1$2notifier$4'

  # Verificación final
  if ($t2 -match '<<<<<<<|=======|>>>>>>>') { throw "Quedaron markers en $path" }

  Set-Content -Path $path -Value $t2 -Encoding UTF8
  Write-Host "FIXED: $path"
}

# Paths según tu screenshot
Fix-Bat "run_shadow_mexc_sol_5m_telegram.bat"
Fix-RunShadowPy "scripts/run_shadow.py"

Write-Host ""
Write-Host "Listo. Ahora corré:"
Write-Host "  git status"
Write-Host "  git diff"
Write-Host "  git add run_shadow_mexc_sol_5m_telegram.bat scripts/run_shadow.py"
Write-Host "  git commit -m `"Resolve Codex patch conflicts (Telegram init + safe env placeholders)`""
