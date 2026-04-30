# Define IP Addresses
$controlPlanes = @("192.168.8.11", "192.168.8.12", "192.168.8.13")
$workerNodes = @(
    "192.168.8.101", "192.168.8.102", "192.168.8.103",
    "192.168.8.104", "192.168.8.105", "192.168.8.106",
    "192.168.8.108", "192.168.8.109", "192.168.8.110",
    "192.168.8.111", "192.168.8.112"
)

Write-Host ">>> Stopping Worker Nodes..." -ForegroundColor Cyan
foreach ($ip in $workerNodes) {
    Write-Host "Connecting to $ip..."
    # Try k3s-agent first, fall back to k3s
    ssh "sack@$ip" "doas rc-service k3s-agent stop || doas rc-service k3s stop"
}

Write-Host "`n>>> Stopping Control Plane Nodes..." -ForegroundColor Cyan
foreach ($ip in $controlPlanes) {
    Write-Host "Connecting to $ip..."
    ssh "sack@$ip" "doas rc-service k3s stop"
}

Write-Host "`n>>> All stop commands sent. Cluster is shutting down." -ForegroundColor Green