# Distributes the laptop's SSH public key to every cluster node so subsequent
# scripts (start/stop/distribute-images) can run non-interactively.
#
# Prereq: run `ssh-keygen -t rsa -b 4096` once on this laptop. You will be
# prompted for the `sack` password ONCE per node here; after that all SSH
# is key-based.

$controlPlanes = @("192.168.8.11", "192.168.8.12", "192.168.8.13")
$workerNodes = @(
    "192.168.8.101", "192.168.8.102", "192.168.8.103",
    "192.168.8.104", "192.168.8.105", "192.168.8.106",
    "192.168.8.109", "192.168.8.110",
    "192.168.8.111", "192.168.8.112"
)
$nodes = $controlPlanes + $workerNodes

$pubKeyPath = "$env:USERPROFILE\.ssh\id_rsa.pub"
if (-not (Test-Path $pubKeyPath)) {
    Write-Host "ERROR: $pubKeyPath not found. Run 'ssh-keygen -t rsa -b 4096' first." -ForegroundColor Red
    exit 1
}
$pubKey = (Get-Content $pubKeyPath -Raw).Trim()

foreach ($ip in $nodes) {
    Write-Host "`n>>> Authorizing key on $ip..." -ForegroundColor Cyan

    # Append the key only if it isn't already present. Idempotent: re-running
    # the script never duplicates the entry.
    $remoteCmd = @"
mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && grep -qxF '$pubKey' ~/.ssh/authorized_keys || echo '$pubKey' >> ~/.ssh/authorized_keys
"@

    ssh "sack@$ip" $remoteCmd
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to authorize key on $ip" -ForegroundColor Red
    } else {
        Write-Host "OK: $ip" -ForegroundColor Green
    }
}

Write-Host "`n>>> Verifying passwordless SSH..." -ForegroundColor Cyan
foreach ($ip in $nodes) {
    ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "sack@$ip" "hostname"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Verification FAILED on $ip" -ForegroundColor Red
    }
}

Write-Host "`n>>> Key distribution complete." -ForegroundColor Green