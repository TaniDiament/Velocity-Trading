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

**1. Build the application image.** `Dockerfile.offline` copies the Maven jar into a minimal `eclipse-temurin` image. Build the jar first, then the image:

```powershell
mvn -DskipTests clean package
docker build -f Dockerfile.offline -t market-maker:1.0.0 .
```

**2. Pull the third-party images** (only needed first time, or after a version bump):

```powershell
docker pull eclipse-temurin:21-jre-alpine
docker pull postgres:16-alpine
docker pull zookeeper:3.9
docker pull rancher/mirrored-pause:3.6
docker pull rancher/local-path-provisioner:v0.0.35
docker pull rancher/mirrored-library-busybox:1.37.0
```

Headlamp is **not** part of this bundle anymore — it has its own tarball and apply step. See **Step 8**.

**3. Save everything into one tarball:**

```powershell
docker save -o dist/images.tar `
  market-maker:1.0.0 `
  eclipse-temurin:21-jre-alpine `
  postgres:16-alpine `
  zookeeper:3.9 `
  rancher/mirrored-pause:3.6 `
  rancher/local-path-provisioner:v0.0.35 `
  rancher/mirrored-library-busybox:1.37.0
```

> **Shortcut:** `bash ./scripts/build-offline-bundle.sh` runs steps 1 + 2 + 3 in one shot (requires WSL or git-bash for the bash interpreter).

> **Note:** `local-path-provisioner` and `mirrored-library-busybox` are required for the postgres and ZK PVCs to bind on an air-gapped cluster — without them, every JPA service in the stack will CrashLoopBackOff because postgres never reaches Ready, and ZK will lose every znode on each pod restart because its `/data` mount has nothing to back it. The exact tags above match this cluster's installed K3s; if K3s is upgraded, re-run `kubectl get deploy -n kube-system local-path-provisioner -o jsonpath='{...}'` to confirm the tags before bundling.

### B. Distribute and Import (Connected to Cluster)

```powershell
.\scripts\distribute-images.ps1
```

This script:

1) scps the 500MB+ images.tar to every node.

2) Imports images into the k8s.io namespace using `doas k3s ctr -n k8s.io images import`.

## Step 5: Application Deployment
1) Transfer manifests to cp1:

```powershell
ssh sack@192.168.8.11 "mkdir -p /home/sack/marketmaker"
scp -r ./k8s sack@192.168.8.11:/home/sack/marketmaker/k8s
```

2) Apply via Kustomize:

```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -k /home/sack/marketmaker/k8s/"
```

> **Upgrading from a pre-PVC ZK install:** the `zk` StatefulSet now declares `volumeClaimTemplates` so znodes survive pod restarts. Kubernetes does **not** allow adding `volumeClaimTemplates` to an existing StatefulSet; if `kubectl apply` reports `Forbidden` on the `zk` resource, delete the StatefulSet first (no app data is at risk — ZK was non-persistent before, so its znodes are already ephemeral): `kubectl -n market-maker delete statefulset zk`, then re-apply.

---

## Step 6: Updating the Application Image (Code → Cluster)
Use this workflow whenever Java source under `src/main/` changes. Manifest-only changes (`k8s/*.yaml`) need only sub-steps D and E from this section; you can skip the rebuild.

### A. Rebuild the image on your laptop
```powershell
bash ./scripts/build-offline-bundle.sh
```
Same build + tarball workflow as Step 4.A, but the third-party images already exist locally so only the new `market-maker:1.0.0` layer is freshly written — the script finishes in seconds. (Equivalent to running the explicit `mvn package` + `docker build` + `docker save` block in Step 4.A by hand.)

### B. Purge the stale image on every node
This is the step that's easy to forget. The MM pods use `imagePullPolicy: IfNotPresent`, and the new tarball still tags the image as `market-maker:1.0.0`, so containerd happily keeps the *old* layer cached and ignores the new one. You must explicitly remove the cached image first:

```powershell
.\scripts\remove-image.ps1
```
This loops over every node and runs `doas k3s ctr -n k8s.io images rm docker.io/library/market-maker:1.0.0`. You can verify with:
```powershell
ssh sack@192.168.8.11 "doas k3s ctr -n k8s.io images ls | Select-String market-maker"
```

### C. Distribute the fresh tarball
```powershell
.\scripts\distribute-images.ps1
```
This scps `dist/images.tar` to every node and re-imports it. Imports are skipped silently for images already present, so only `market-maker:1.0.0` is actually re-loaded — the rest of the bundle is a cheap no-op.

### D. Re-apply manifests if any changed
Skip this step for pure code changes. For YAML or properties changes, ship them too:
```powershell
scp -r ./k8s sack@192.168.8.11:/home/sack/marketmaker/k8s
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -k /home/sack/marketmaker/k8s/"
```

### E. Restart pods so they pick up the new image
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n market-maker rollout restart statefulset/mm"
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n market-maker rollout status statefulset/mm"
```
For non-MM services that also use `market-maker:1.0.0` (`exchange`, `trading-state`, `exposure-reservation`, `external-publisher`), restart their Deployments too:
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n market-maker rollout restart deploy/exchange deploy/trading-state deploy/exposure-reservation deploy/external-publisher"
```

### F. Verify the new code is running
```powershell
# Confirm every node now has only the freshly imported image (single sha)
ssh sack@192.168.8.11 "doas k3s ctr -n k8s.io images ls | Select-String market-maker"

# Confirm no pods are stuck on a stale image (RestartCount=0 after rollout, all 1/1 Running)
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n market-maker get pods -o wide"
```

> **Common pitfall:** if pods come up but behavior looks unchanged, you almost certainly skipped step B. The new tarball was distributed and re-imported, but containerd kept using the old cached layer because the tag didn't change. Re-run `.\scripts\remove-image.ps1` and then `.\scripts\distribute-images.ps1`.

---

## Step 7: Verification & Quorum Troubleshooting
Monitor status from cp1:
```powershell
doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get pods -n market-maker -o wide
```

### Critical ZooKeeper fixes in zookeeper.yaml

- **Parallel startup**: `podManagementPolicy: Parallel` in the `StatefulSet` avoids the "OrderedReady" deadlock.
- **DNS resolution**: `publishNotReadyAddresses: true` in the `zk-hs` service lets peers resolve each other's IPs before they are healthy.
- **Anti-affinity**: ZK pods are pinned to `cp1`, `cp2`, and `cp3` to separate the coordination layer from worker traffic.

### Startup ordering (mm initContainers)
The `mm` StatefulSet runs two `initContainers` (`wait-for-zk`, `wait-for-postgres`, both using `rancher/mirrored-library-busybox:1.37.0`) that block the app from starting until:

1. All three ZK peers respond `imok` on port 2181 (quorum is reachable).
2. `postgres:5432` accepts TCP connections.

This eliminates the prior race where `mm` would CrashLoop until kubelet backoff happened to land after ZK election / postgres bind. If `mm` pods sit in `Init:0/2` for more than a minute, check `kubectl logs mm-0 -c wait-for-zk` (or `-c wait-for-postgres`) on cp1 — the loop prints which host it is still waiting on.

### Pod reset (fallback)
If `mm` pods are still in `CrashLoopBackOff` after ZK and postgres are Ready, force a restart on cp1:
```powershell
doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl delete pods -l app=mm -n market-maker
```

## Step 8: Headlamp Cluster UI
A web Kubernetes dashboard (Headlamp) is deployed from `k8s/headlamp/` into its own `headlamp` namespace and exposed on **NodePort 30090** of every node. It is shipped as a **separate image bundle** so the main `dist/images.tar` stays focused on application images.

### A. Build the Headlamp tarball (laptop with internet, one-time)

```powershell
docker pull ghcr.io/headlamp-k8s/headlamp:v0.39.0
docker save -o dist/headlamp.tar ghcr.io/headlamp-k8s/headlamp:v0.39.0
```

### B. Distribute and import on every node

```powershell
$nodes = @(
  "192.168.8.11","192.168.8.12","192.168.8.13",
  "192.168.8.101","192.168.8.102","192.168.8.103",
  "192.168.8.104","192.168.8.105","192.168.8.106",
  "192.168.8.109","192.168.8.110","192.168.8.111","192.168.8.112"
)
foreach ($n in $nodes) {
  ssh "sack@${n}" "mkdir -p /home/sack/marketmaker"
  scp dist/headlamp.tar "sack@${n}:/home/sack/marketmaker/headlamp.tar"
  ssh -t "sack@${n}" "doas k3s ctr -n k8s.io images import /home/sack/marketmaker/headlamp.tar"
}
```

### C. Apply the kustomization

```powershell
scp -r ./k8s sack@192.168.8.11:/home/sack/marketmaker/k8s
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -k /home/sack/marketmaker/k8s/headlamp/"
```

This creates the `headlamp` namespace, ServiceAccount `headlamp`, ClusterRoleBinding `headlamp-ns-admin` → `cluster-admin`, the Deployment, and the NodePort Service on 30090.

Browse to `http://192.168.8.11:30090` (or any other node IP). See **Appendix A** for the login flow, day-to-day workflow, and cluster-specific caveats.

## Step 9: Stopping the Application (Without Stopping the Cluster)
Use this when you want to free cluster resources or pause the app between dev sessions, but you want to keep K3s itself running so other workloads stay healthy and you don't have to wait for a full cluster restart.

### Option A: Scale every workload to zero (recommended)
Scales all Deployments and StatefulSets in the `market-maker` namespace to `replicas: 0`. Pods terminate, but Services, ConfigMaps, and PersistentVolumeClaims are preserved — so postgres rows, ZK znodes, and PVC bindings survive.

```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl scale -n market-maker statefulset --all --replicas=0"
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl scale -n market-maker deployment --all --replicas=0"
```

Confirm everything is gone:
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get pods -n market-maker"
```
Should report `No resources found in market-maker namespace.` once termination completes (a few seconds).

To bring it back up, re-apply the manifests — `kubectl apply` restores each workload's declared replica count from the YAML:
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -k /home/sack/marketmaker/k8s/"
```

### Option B: Stop only the MM JVMs (keep infra running)
When you want to recycle the seven `mm` pods but leave exchange, trading-state, exposure-reservation, external-publisher, postgres, and ZK alive:
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl scale -n market-maker statefulset/mm --replicas=0"
```
Bring them back:
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl scale -n market-maker statefulset/mm --replicas=7"
```

### Option C: Delete everything in the namespace (clean slate)
Tears down every workload, Service, and ConfigMap defined in the kustomization. PVCs are *retained* (the `mm` StatefulSet declares `persistentVolumeClaimRetentionPolicy: Retain` and the default storage class on this cluster keeps PVCs across deletes), so postgres and ZK data survive a restore.
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl delete -k /home/sack/marketmaker/k8s/"
```
If you also want to wipe persistent state (start fresh next time):
```powershell
ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl delete pvc -n market-maker --all"
```
Restore by re-running Step 5 — `kubectl apply -k /home/sack/marketmaker/k8s/`.

> **Note:** None of these options stops K3s itself. The kubelet, containerd, and the API server stay running on every node, so re-applying or scaling back up is fast (no node startup time). Use `.\scripts\stop-cluster.ps1` only when you need to power off the nodes themselves.

---

## Step 10: Running the Integration Test
Execute `ClusterIntegrationWithSystemK8sTest.java`.

1) Network: Connect laptop to the Mango Router.

2) IntelliJ VM options: point the test at a physical node IP by adding this to your Run Configuration:

```
-Dcluster.k8s.host=192.168.8.11
```

3) Run: the test will interface with the cluster, injecting orders and validating distributed state across the 14 nodes.

---

## Appendix A: Using Headlamp

Headlamp is a web-based Kubernetes dashboard. For this cluster it is exposed on **NodePort 30090** of every node and authenticates with a bearer token tied to the `headlamp` ServiceAccount in the `headlamp` namespace (granted `cluster-admin` via the `headlamp-ns-admin` ClusterRoleBinding).

### Logging In
1. Open `http://192.168.8.11:30090` (or any other node IP from the IP map) in a browser on a laptop attached to the Mango Router.
2. The login screen will ask for a **Cluster** and a **Token**. Leave the cluster picker on its default ("main"); it's the only entry the in-cluster build knows about.
3. Generate a token from cp1:
   ```powershell
   ssh sack@192.168.8.11 "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n headlamp create token headlamp"
   ```
   Tokens default to ~1 hour. For longer sessions, append `--duration=24h`.
4. Paste the token, click **Authenticate**.

### Day-to-day Workflow
After authenticating, the most useful views for this stack:

- **Workloads → Pods** (filter namespace to `market-maker`): live status of `mm-0..mm-6`, `trading-state`, `exchange`, `exposure-reservation`, `external-publisher`, `postgres-0`, `zk-0..zk-2`. Click a pod to open its **Logs** tab — the same output `kubectl logs` would give, streamed live, with a **Previous** toggle for inspecting the last crashed container during a CrashLoopBackOff.
- **Workloads → StatefulSets**: the `mm` StatefulSet has a **Scale** button — useful for shrinking to 1 replica during a debug session and scaling back to 7. Pod identity (`mm-0` … `mm-N`) is preserved, so the symbol-shard assignment in ZK stays stable.
- **Storage → Persistent Volume Claims**: confirms `data-postgres-0` is `Bound`. If it's `Pending`, see *Step 4.A* — usually the local-path-provisioner image isn't on the node yet.
- **Cluster → Nodes**: 14 entries (cp1-cp3, n1-n12 minus n107). A node with `NotReady` here is your first signal something is wrong with k3s itself before you start digging into application logs.
- **Network → Services**: shows the NodePorts (30081-30087 for `mm-N-np`, 30180-30183 for the support services, 30090 for Headlamp itself) — handy when you forget which port maps to which pod from outside the cluster.
- **Config → ConfigMaps**: `symbols`, `hazelcast-members`. Editing in-place from the UI takes effect on the next pod restart, useful for adding a symbol without re-applying the kustomization.

### Exec Into a Pod
Headlamp's pod page has a **Terminal** tab that runs `kubectl exec -it … -- sh` inside the browser. For the JVM pods this lands you in the Spring app container; the helpful diagnostic commands:

```sh
# inside an mm pod
ls /config                        # confirms the symbols.txt mount
echo ruok | nc -w 2 zk-0.zk-hs 2181   # confirms ZK reachability from this pod
wget -qO- http://localhost:8080/actuator/health   # what the kubelet probe sees
```

### Token Expired
If the UI suddenly reports 401s on every panel, the token expired. Re-run the `kubectl create token` command and paste the new value via the **Settings → Cluster Settings → Token** field — no need to log out fully.

### Caveats Specific to This Cluster
- **Air-gapped pulls**: Headlamp's image (`ghcr.io/headlamp-k8s/headlamp:v0.39.0`) is shipped in its **own** tarball (`dist/headlamp.tar`), separate from the main `dist/images.tar`. If the pod is `ErrImageNeverPull` or `ImagePullBackOff`, re-run **Step 8.A / 8.B** to rebuild and re-import the Headlamp tar on every node.
- **One Headlamp pod, one node**: there's only 1 replica. If the node hosting it goes down, NodePort 30090 on *other* nodes still routes correctly (k3s's klipper-lb forwards to wherever the pod actually is), but during the eviction/reschedule window the UI is briefly unavailable. Bumping `replicas: 2` in `k8s/headlamp/headlamp.yaml` is fine if you want HA — Headlamp itself is stateless.
- **No persistent settings**: Headlamp stores user preferences in the browser, so cluster bookmarks / saved filters disappear if you switch laptops. Nothing to back up.

---

## Appendix B: Connecting to the Position UI

The Position UI is a single-page dashboard (`src/main/resources/static/index.html`) served by the **`trading-state`** service. There is no separate `position-ui` deployment in the cluster — the standalone `position-ui` Spring profile (and its `compose.yml` service) is for local Docker Compose only. On k3s, the same web page is bundled with `trading-state` and reached through its NodePort.

### Address

Browse to NodePort **30180** on any cluster node:

```
http://192.168.8.11:30180/
```

Any node IP works equally — k3s's klipper-lb forwards to whichever pod the `trading-state` Service currently selects. Cp1 is convenient because it is already the SSH entry point; cp2/cp3 (`192.168.8.12`, `192.168.8.13`) and the workers (`192.168.8.101`–`.112`, skipping `.107`) all resolve to the same UI.

### What the page shows

A live table keyed by symbol with columns: **Symbol**, **Net Qty**, **Version**, **Last Trade**, **Bid**, **Ask**, **Last Fill ID**. New fills cause the affected row to flash. The status pill in the header reads `connected to leader` when the WebSocket is healthy and `disconnected — retrying` otherwise.

Backing endpoints on the same pod (handy for `curl` checks):

| Method | Path                  | Purpose                                                                             |
| ------ | --------------------- | ----------------------------------------------------------------------------------- |
| GET    | `/positions`          | All positions (JSON)                                                                |
| GET    | `/positions/{symbol}` | Single position                                                                     |
| GET    | `/quotes/{symbol}`    | Latest bid/ask (used to populate a row)                                             |
| GET    | `/state/fills`        | Fill history                                                                        |
| GET    | `/leader-info`        | Which `trading-state` replica is leader                                             |
| GET    | `/health`             | Liveness/readiness (same path the probe uses)                                       |
| WS     | `/ws` (SockJS+STOMP)  | `/app/positions.snapshot` for the initial dump, `/topic/positions` for live updates |

### Caveats specific to this cluster

- **Leader-only push**: only the elected `trading-state` leader publishes to `/topic/positions`. The NodePort balances across all 3 replicas, but each replica re-broadcasts the leader's snapshots, so the UI works identically no matter which pod the WebSocket lands on. If the status pill stays on `disconnected — retrying` after a few seconds, check `kubectl -n market-maker get pods -l app=trading-state` — if zero are Ready, ZK or Postgres is the upstream cause (see *Step 7*).
- **CDN dependencies**: the page loads `sockjs-client` and `@stomp/stompjs` from `cdn.jsdelivr.net`. The browser making the request needs outbound internet — the cluster itself doesn't. On an air-gapped laptop the table will render empty (the page loads from the cluster, the JS doesn't), and the browser console will show `net::ERR_*` for jsdelivr.
- **Symbol set**: rows appear only for symbols with at least one fill recorded. To populate the dashboard from a fresh deploy, drive traffic via the integration test (`-Dcluster.k8s.host=192.168.8.11`) or `POST` fills to `trading-state` directly.
- **No auth**: unlike Headlamp, the Position UI has no token — anyone on the `192.168.8.x` subnet who can reach `:30180` can view positions. The Mango Router boundary is the only access control.