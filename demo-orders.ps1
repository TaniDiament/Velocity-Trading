<#
.SYNOPSIS
  Demo driver: continuously submits crossing orders against the live MM
  quotes so you can watch fills flow through the system end-to-end (positions
  updating in the UI, exposure being decremented, MMs replacing quotes, etc).

.DESCRIPTION
  Assumes the k3s stack is already up and the exchange NodePort (30181) is
  reachable from this machine. Picks a random symbol every tick, reads the
  current quote from /quotes/{symbol}, and submits a BUY at the ask or a
  SELL at the bid so the order is guaranteed to cross. Mixes both sides
  randomly so positions stay roughly balanced rather than walking to the
  ±100 cap.

  Stop with Ctrl+C. Prints a one-line summary every ~5s so you can tell
  it's still alive without flooding the console.

.PARAMETER ExchangeHost
  Node IP that exposes the exchange NodePort. Default: 192.168.8.11 (cp1).

.PARAMETER RatePerSecond
  Approximate orders per second. Default: 2. Set higher (e.g. 5-10) to drive
  more activity; lower (0.5) for a slow demo.

.PARAMETER Symbols
  Symbols to drive orders against. Default matches the cluster's seed set.

.PARAMETER Seed
  If passed, calls /publisher/seed-quotes first so the symbols have quotes
  in the exchange before we start. Safe to use even when MM quotes already
  exist — bootstrap quotes get replaced on the next MM refresh cycle.

.EXAMPLE
  .\demo-orders.ps1
  .\demo-orders.ps1 -RatePerSecond 5
  .\demo-orders.ps1 -ExchangeHost localhost -Seed
  .\demo-orders.ps1 -Symbols AAPL,MSFT
#>
param(
    [string]$ExchangeHost = "192.168.8.11",
    [int]$ExchangePort = 30181,
    [int]$PublisherPort = 30183,
    [string[]]$Symbols = @("AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META"),
    [double]$RatePerSecond = 2.0,
    [switch]$Seed
)

$ErrorActionPreference = "Continue"

function Invoke-Json {
    param([string]$Uri, [string]$Method = "GET", [string]$Body = $null)
    $params = @{
        Uri = $Uri
        Method = $Method
        TimeoutSec = 5
        SkipHttpErrorCheck = $true
    }
    if ($Body) {
        $params.Body = $Body
        $params.ContentType = "application/json"
    }
    return Invoke-WebRequest @params
}

if ($Seed) {
    Write-Host ">>> Seeding bootstrap quotes for $($Symbols -join ', ')..." -ForegroundColor Yellow
    $seedBody = ConvertTo-Json @($Symbols) -Compress
    try {
        $r = Invoke-Json -Uri "http://${ExchangeHost}:${PublisherPort}/publisher/seed-quotes" -Method "POST" -Body $seedBody
        if ($r.StatusCode -eq 200) {
            Write-Host "    seed-quotes OK" -ForegroundColor DarkGreen
        } else {
            Write-Warning "seed-quotes returned HTTP $($r.StatusCode): $($r.Content)"
        }
    } catch {
        Write-Warning "seed-quotes threw: $_"
    }
}

$sleepMs = [int](1000.0 / $RatePerSecond)
$sent = 0
$accepted = 0
$rejected = 0
$noQuote = 0
$lastSummary = Get-Date

Write-Host ""
Write-Host ">>> Driving crossing orders to http://${ExchangeHost}:${ExchangePort} at ${RatePerSecond}/sec" -ForegroundColor Cyan
Write-Host "    symbols: $($Symbols -join ', ')"
Write-Host "    Ctrl+C to stop."
Write-Host ""

while ($true) {
    $symbol = Get-Random -InputObject $Symbols

    $quoteResp = Invoke-Json -Uri "http://${ExchangeHost}:${ExchangePort}/quotes/$symbol" -Method "GET"
    if ($quoteResp.StatusCode -ne 200) {
        $noQuote++
        Start-Sleep -Milliseconds $sleepMs
        continue
    }
    $quote = $quoteResp.Content | ConvertFrom-Json

    $nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    if ($quote.expiresAt -le $nowMs) {
        $noQuote++
        Start-Sleep -Milliseconds $sleepMs
        continue
    }

    # Decide side: prefer randomness when both sides have depth, otherwise
    # take whichever side the MM is showing depth on. Skipping the symbol
    # entirely (both qty=0) is rare but handled.
    $side = $null
    $limitPrice = $null
    if ($quote.askQuantity -gt 0 -and $quote.bidQuantity -gt 0) {
        if ((Get-Random -Maximum 2) -eq 0) {
            $side = "BUY"; $limitPrice = $quote.askPrice
        } else {
            $side = "SELL"; $limitPrice = $quote.bidPrice
        }
    } elseif ($quote.askQuantity -gt 0) {
        $side = "BUY"; $limitPrice = $quote.askPrice
    } elseif ($quote.bidQuantity -gt 0) {
        $side = "SELL"; $limitPrice = $quote.bidPrice
    } else {
        Start-Sleep -Milliseconds $sleepMs
        continue
    }

    $quantity = Get-Random -Minimum 1 -Maximum 4

    $orderBody = @{
        id = [Guid]::NewGuid().ToString()
        symbol = $symbol
        quantity = $quantity
        limitPrice = $limitPrice
        side = $side
    } | ConvertTo-Json -Compress

    $sent++
    $orderResp = Invoke-Json -Uri "http://${ExchangeHost}:${ExchangePort}/orders" -Method "POST" -Body $orderBody
    if ($orderResp.StatusCode -eq 200) {
        $accepted++
        $stamp = Get-Date -Format "HH:mm:ss"
        $priceFmt = "{0,7:N2}" -f $limitPrice
        Write-Host "$stamp  $symbol  $side  qty=$quantity  @ $priceFmt" -ForegroundColor Green
    } else {
        $rejected++
    }

    if (((Get-Date) - $lastSummary).TotalSeconds -ge 5) {
        $skipNote = if ($noQuote -gt 0) { "  noquote=$noQuote" } else { "" }
        Write-Host "  -- sent=$sent accepted=$accepted rejected=$rejected$skipNote --" -ForegroundColor DarkGray
        $lastSummary = Get-Date
    }

    Start-Sleep -Milliseconds $sleepMs
}