# Remove a container image from every node's containerd k8s.io namespace.
# Default target is market-maker:1.0.0 so a fresh build can be re-distributed
# without IfNotPresent silently keeping the old layers.
#
# Usage:
#   .\scripts\remove-image.ps1                          # removes market-maker:1.0.0
#   .\scripts\remove-image.ps1 -Image foo:2.0.0         # removes a different tag
#   .\scripts\remove-image.ps1 -DeleteMmPods            # also kubectl-delete mm pods on cp1
param(
    [string]$Image = "docker.io/library/market-maker:1.0.0",
    [switch]$DeleteMmPods
)

$nodes = @(
    "192.168.8.11", "192.168.8.12", "192.168.8.13",
    "192.168.8.101", "192.168.8.102", "192.168.8.103",
    "192.168.8.104", "192.168.8.105", "192.168.8.106",
    "192.168.8.109", "192.168.8.110",
    "192.168.8.111", "192.168.8.112"
)

if ($DeleteMmPods) {
    Write-Host "`n>>> Deleting mm pods on cp1 so containerd releases the image..." -ForegroundColor Cyan
    ssh "sack@192.168.8.11" "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl delete pods -l app=mm -n market-maker --wait=false"
}

foreach ($node in $nodes) {
    Write-Host "`n>>> Removing $Image from $node" -ForegroundColor Cyan
    $output = ssh "sack@${node}" "doas k3s ctr -n k8s.io images rm $Image" 2>&1 | Out-String
    Write-Host $output
    if ($output -match "connection refused" -or $output -match "containerd\.sock") {
        Write-Host "ERROR: containerd unreachable on $node -- is K3s running? Run .\scripts\start-cluster.ps1 first." -ForegroundColor Red
    } elseif ($LASTEXITCODE -ne 0 -and $output -notmatch "not found") {
        Write-Host "WARNING: unexpected failure on $node." -ForegroundColor Yellow
    }
}

Write-Host "`n>>> Image removal complete." -ForegroundColor Green
Write-Host "Verify with:  ssh sack@192.168.8.11 'doas k3s ctr -n k8s.io images ls | grep market-maker'" -ForegroundColor Yellow