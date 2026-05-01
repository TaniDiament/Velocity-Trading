# Define all node IP addresses
$nodes = @(
    "192.168.8.11", "192.168.8.12", "192.168.8.13",  # Control Planes
    "192.168.8.101", "192.168.8.102", "192.168.8.103",
    "192.168.8.104", "192.168.8.105", "192.168.8.106",
    "192.168.8.108", "192.168.8.109", "192.168.8.110",
    "192.168.8.111", "192.168.8.112"                 # Workers
)

$sourceFile = "dist/images.tar"
$destinationPath = "/home/sack/images.tar"

foreach ($node in $nodes) {
    Write-Host "`n>>> Processing node: $node" -ForegroundColor Cyan

    # 1. Transfer the bundle
    Write-Host "Transferring $sourceFile..."
    scp $sourceFile "sack@${node}:${destinationPath}"

    if ($LASTEXITCODE -eq 0) {
        # 2. Import the images into the k8s.io namespace
        Write-Host "Importing images on $node..." -ForegroundColor Yellow
        ssh -t "sack@${node}" "doas k3s ctr -n k8s.io images import $destinationPath"
    } else {
        Write-Host "Failed to transfer to $node. Skipping import." -ForegroundColor Red
    }
}

Write-Host "`n>>> Distribution and Import Complete!" -ForegroundColor Green