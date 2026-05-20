# Headlamp (standalone)

Headlamp deployed into its own `headlamp` namespace with a ClusterRoleBinding to
`cluster-admin`, so the UI sees every namespace in the cluster.

This is a **separate kustomization** from `k8s/`. The root kustomization at
`../kustomization.yaml` rewrites all of its resources into the `market-maker`
namespace; this one targets `headlamp` instead.

Headlamp's image is shipped in its **own tarball** (`dist/headlamp.tar`),
independent of the main application bundle (`dist/images.tar`).

## Layout

- `namespace.yaml` — creates the `headlamp` namespace
- `headlamp.yaml` — ServiceAccount `headlamp`, ClusterRoleBinding
  `headlamp-ns-admin`, NodePort Service on 30090, Deployment
- `kustomization.yaml` — pins everything to the `headlamp` namespace

## Install

See **Step 8** in `docs/installation.md` for the full procedure. Summary:

1. Build the Headlamp tarball on a host with internet:
   ```powershell
   docker pull ghcr.io/headlamp-k8s/headlamp:v0.39.0
   docker save -o dist/headlamp.tar ghcr.io/headlamp-k8s/headlamp:v0.39.0
   ```
2. scp `dist/headlamp.tar` to every node and import with
   `doas k3s ctr -n k8s.io images import …` (loop in Step 8.B).
3. Apply this kustomization from cp1:
   ```bash
   doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -k k8s/headlamp/
   ```
4. Print a login token:
   ```bash
   doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n headlamp create token headlamp
   ```
5. Open `http://<any-node-ip>:30090` and paste the token.
