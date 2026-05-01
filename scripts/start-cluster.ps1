# Define IP Addresses
$controlPlanes = @("192.168.8.11", "192.168.8.12", "192.168.8.13")
$workerNodes = @(
    "192.168.8.101", "192.168.8.102", "192.168.8.103",
    "192.168.8.104", "192.168.8.105", "192.168.8.106",
    "192.168.8.108", "192.168.8.109", "192.168.8.110",
    "192.168.8.111", "192.168.8.112"
)

Write-Host ">>> Starting Control Plane Nodes..." -ForegroundColor Cyan
foreach ($ip in $controlPlanes) {
    Write-Host "Connecting to $ip..."
    ssh -t "sack@$ip" "doas rc-service k3s start"
}

Write-Host "`n>>> Starting Worker Nodes..." -ForegroundColor Cyan
foreach ($ip in $workerNodes) {
    Write-Host "Connecting to $ip..."
    # We try k3s-agent first; if that service doesn't exist, it falls back to k3s
    ssh -t "sack@$ip" "doas rc-service k3s-agent start || doas rc-service k3s start"
}

Write-Host "`n>>> All start commands sent. Waiting 10 seconds for sockets to initialize..." -ForegroundColor Green
Start-Sleep -Seconds 10