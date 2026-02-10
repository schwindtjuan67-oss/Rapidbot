# === MEXC Futures: Zero-fee extractor (public endpoints) ===
# Fuente de datos:
# - Contract info: GET https://api.mexc.com/api/v1/contract/detail
# - Ticker:        GET https://api.mexc.com/api/v1/contract/ticker
# (Docs oficiales) :contentReference[oaicite:1]{index=1}

$details = Invoke-RestMethod "https://api.mexc.com/api/v1/contract/detail"
$contracts = $details.data

# Normalizar a array por si te viene como objeto
if ($contracts -eq $null) { throw "No vino details.data" }
if ($contracts -isnot [System.Collections.IEnumerable] -or $contracts -is [string]) { $contracts = @($contracts) }

$tickerResp = Invoke-RestMethod "https://api.mexc.com/api/v1/contract/ticker"
$tickers = $tickerResp.data
if ($tickers -eq $null) { throw "No vino ticker.data" }
if ($tickers -isnot [System.Collections.IEnumerable] -or $tickers -is [string]) { $tickers = @($tickers) }

# Map ticker por symbol
$map = @{}
foreach ($t in $tickers) { if ($t.symbol) { $map[$t.symbol] = $t } }

# Filtrar: zero-fee + API allowed (importante para bots) + armar salida
$zero = foreach ($c in $contracts) {
  $isZero = ($c.isZeroFeeSymbol -eq $true) -or ($c.isZeroFeeRate -eq $true)
  $apiOk  = ($c.apiAllowed -eq $true)

  if ($isZero -and $apiOk) {
    $t = $map[$c.symbol]
    [PSCustomObject]@{
      symbol       = $c.symbol
      makerFeeRate = [double]$c.makerFeeRate
      takerFeeRate = [double]$c.takerFeeRate
      amount24     = if ($t) { [double]$t.amount24 } else { 0.0 }
      lastPrice    = if ($t) { [double]$t.lastPrice } else { $null }
      isZeroFeeSym = [bool]$c.isZeroFeeSymbol
      isZeroFeeRate= [bool]$c.isZeroFeeRate
      apiAllowed   = [bool]$c.apiAllowed
      contractId   = $c.id
      baseCoin     = $c.baseCoin
      quoteCoin    = $c.quoteCoin
    }
  }
}

# Top por "amount24" (turnover 24h)
$top = $zero | Sort-Object amount24 -Descending

$top | Select-Object -First 50 | Format-Table -Auto

# Guardar lista completa
$csvOut = ".\mexc_zero_fee_futures_ranked.csv"
$top | Export-Csv $csvOut -NoTypeInformation -Encoding UTF8
"Saved: $csvOut"
