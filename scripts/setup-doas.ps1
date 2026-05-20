# Configures passwordless `doas` for the `sack` user on every cluster node so
# scripts like start-cluster.ps1 / distribute-images.ps1 can run k3s and
# rc-service commands without prompting.
#
# Prereq: setup-keys.ps1 has already been run (we use key-based SSH here).
# This still prompts for the `sack` password ONCE per node — that's the
# password `doas` itself asks for to elevate while writing the conf file.

$controlPlanes = @("192.168.8.11", "192.168.8.12", "192.168.8.13")
$workerNodes = @(
    "192.168.8.101", "192.168.8.102", "192.168.8.103",
    "192.168.8.104", "192.168.8.105", "192.168.8.106",
    "192.168.8.109", "192.168.8.110",
    "192.168.8.111", "192.168.8.112"
)
$nodes = $controlPlanes + $workerNodes

# The rule we want in /etc/doas.d/doas.conf. Idempotent: only appended if
# missing. We pin to the `sack` user so other accounts aren't affected.
$doasRule = "permit nopass sack"

foreach ($ip in $nodes) {
    Write-Host "`n>>> Configuring doas on $ip..." -ForegroundColor Cyan

    # -t allocates a TTY so doas can prompt for the password on the first
    # invocation per node. After this script completes, future doas calls
    # are nopass.
    $remoteCmd = @"
sh -c "grep -qxF '$doasRule' /etc/doas.d/doas.conf 2>/dev/null || echo '$doasRule' | doas tee -a /etc/doas.d/doas.conf >/dev/null && doas chmod 600 /etc/doas.d/doas.conf"
"@

    ssh -t "sack@$ip" $remoteCmd
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to configure doas on $ip" -ForegroundColor Red
    } else {
        Write-Host "OK: $ip" -ForegroundColor Green
    }
}

Write-Host "`n>>> Verifying passwordless doas..." -ForegroundColor Cyan
foreach ($ip in $nodes) {
    # If this prompts for a password, the rule didn't take.
    ssh -o BatchMode=yes "sack@$ip" "doas -n true"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Verification FAILED on $ip (doas still requires a password)" -ForegroundColor Red
    } else {
        Write-Host "OK: $ip" -ForegroundColor Green
    }
}

Write-Host "`n>>> doas configuration complete." -ForegroundColor Green