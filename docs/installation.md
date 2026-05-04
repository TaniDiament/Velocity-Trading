# Market-Maker Cluster Deployment Guide

This document serves as the comprehensive manual for deploying and testing the **Market-Maker** distributed system on a 14-node air-gapped K3s cluster.

## Overview of the Environment
* **Infrastructure**: 3 Control Plane nodes (cp1-cp3), 11 Worker nodes (n1-n12).
* **Networking**: Isolated via Mango Router (192.168.8.1 subnet).
* **Deployment**: Managed via K3s (Alpine Linux) using imported image bundles.

---

## Step 1: Physical Setup & Connectivity
1.  **Network Connection**: Connect the nodes to the Mango Router switch. Connect your laptop to a LAN port or the router's Wi-Fi.
2.  **Laptop Configuration**: Ensure your laptop is on the same subnet (`192.168.8.x`).
3.  **IP Map**:
    * **Control Planes**: `192.168.8.11` (cp1), `192.168.8.12` (cp2), `192.168.8.13` (cp3)
    * **Workers**: `192.168.8.101` – `192.168.8.112` (Note: `.107` is skipped)

---

## Step 2: One-Time Security & Automation Setup
Perform these steps from PowerShell on your laptop to enable silent script execution.

### A. SSH Key-Based Authentication
1.  **Generate Keys**: `ssh-keygen -t rsa -b 4096` (Press Enter for all defaults).
2.  **Distribute Keys**: Run `.\scripts\setup-keys.ps1` to authorize your laptop on all 14 nodes.

### B. Passwordless Administrative Access
1.  **Configure `doas`**: Run `.\scripts\setup-doas.ps1`.
    * This adds `permit nopass sack` to `/etc/doas.d/doas.conf` on all nodes, allowing scripts to execute `k3s` and `rc-service` commands without prompts.

---

## Step 3: Cluster Lifecycle Management
The K3s services must be running for the container runtime socket (`containerd.sock`) to be active.

* **To Start the Cluster**: Run `.\scripts\start-cluster.ps1`.
    * *Note: Wait ~20 seconds after completion for services to fully initialize.*
* **To Stop the Cluster**: Run `.\scripts\stop-cluster.ps1`.
* **Hard Reset**: If networking is stuck, run `doas /usr/local/bin/k3s-killall.sh` on the affected nodes.

---

## Step 4: Air-Gapped Image Deployment
Since the nodes cannot reach Docker Hub, images must be bundled on a machine with internet and transferred manually.

### A. Create the Image Bundle (Laptop with Internet)
```powershell
docker pull rancher/mirrored-pause:3.6
docker pull rancher/local-path-provisioner:v0.0.35
docker pull rancher/mirrored-library-busybox:1.37.0
docker save -o dist/images.tar `
  market-maker:1.0.0 `
  eclipse-temurin:21-jre-alpine `
  postgres:16-alpine `
  zookeeper:3.9 `
  rancher/mirrored-pause:3.6 `
  rancher/local-path-provisioner:v0.0.35 `
  rancher/mirrored-library-busybox:1.37.0
```

> **Note:** `local-path-provisioner` and `mirrored-library-busybox` are required for the postgres PVC to bind on an air-gapped cluster — without them, every JPA service in the stack will CrashLoopBackOff because postgres never reaches Ready. The exact tags above match this cluster's installed K3s; if K3s is upgraded, re-run `kubectl get deploy -n kube-system local-path-provisioner -o jsonpath='{...}'` to confirm the tags before bundling.
### B. Distribute and Import (Connected to Cluster)
Run 
```powershell
.\scripts\distribute-images.ps1. 
```
This script:

1) scps the 500MB+ images.tar to every node.

2) Imports images into the k8s.io namespace using doas k3s ctr -n k8s.io images import.

## Step 5: Application Deployment
1) Transfer Manifests to cp1:
Run 
```powershell
ssh sack@192.168.8.11 "mkdir -p /home/sack/marketmaker"
scp -r ./k8s sack@192.168.8.11:/home/sack/marketmaker/k8s
```
2) Apply via Kustomize:
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -k /home/sack/marketmaker/k8s/"```

## Step 6: Verification & Quorum Troubleshooting
Monitor status from cp1:
```powershell
doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get pods -n market-maker -o wide
```

## Critical ZooKeeper Fixes in zookeeper.yaml:

### Parallel Startup: 
Set `podManagementPolicy: Parallel` in the `StatefulSet` to avoid the "OrderedReady" deadlock.

### DNS Resolution: 
Set `publishNotReadyAddresses: true` in the `zk-hs` service so peers can resolve each other's IPs before they are healthy.

### Anti-Affinity: 
Ensure ZK pods are pinned to `cp1`, `cp2`, and `cp3` to separate the coordination layer from worker traffic.

## Pod Reset:
If `mm` pods are in `CrashLoopBackOff` after ZK is ready, force a restart:
Run on cp1:
```powershell
doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl delete pods -l app=mm -n market-maker
```

## Step 7: Running the Integration Test
Execute `ClusterIntegrationWithSystemK8sTest.java`.

1) Network: Connect laptop to the Mango Router.

2) IntelliJ VM Options: You must point the test to the physical Master IPs. Add this to your Run Configuration:

Plaintext
`-Dzk.hosts=192.168.8.11:2181,192.168.8.12:2181,192.168.8.13:2181`

3) Run: The test will interface with the cluster, injecting orders and validating distributed state across the 14 nodes.