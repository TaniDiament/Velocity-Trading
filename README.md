<p align="center">
  <img src="docs/img/Velocitylogo.png" alt="Velocity Trading" width="220" />
</p>

# Velocity Trading

**Team Members:** Tani Diament, Shimmie Hahn, Yisroel Newmark, Matias Salem

## Overview

Market makers are the middle men in modern trading activity who provide liquidity to an exchange by quoting buy and sell prices on assets. The market maker profits off the bid-ask spread while managing the underlying risk of its positions. The system must handle massive volumes of orders while maintaining consistent state throughout the cluster.

What makes the system interesting:

- **14-node K3s cluster** (3 control plane nodes + 11 worker nodes) running 7 market-maker pods and three replicated upstream services (Exchange, Trading State, Exposure Reservation) each as 1 leader + 2 hot-backup replicas.
- **Durable in-memory state** — every IMap (`positions`, `fills`, `quotes`, `reservations`, `external-orders`) is write-through via a JPA `MapStore` to a single PostgreSQL StatefulSet, with `InitialLoadMode = EAGER` so a full cluster restart rebuilds state with no operator intervention.
- **ZooKeeper-coordinated failover** — Curator `LeaderLatch` elects the writable replica per service; clients resolve the current leader via an ephemeral `/mm/endpoints/{svc}` znode, so failover is invisible to callers.

## Quickstart (local Docker Compose)

The full stack — 3× Zookeeper, Postgres, 3× Trading State, 3× Exchange, 3× Exposure Reservation, 7× Market Maker, Position UI, external publisher, nginx LB — runs locally via [`compose.yml`](compose.yml).

```powershell
mvn -DskipTests clean package
docker build -t market-maker .
docker compose up
```

Once everything reports healthy:

- **Position UI** — http://localhost:18080
- **External publisher** — http://localhost:18083
- **Market Maker nodes** — http://localhost:8081 … http://localhost:8087

For the production K3s deployment (air-gapped image bundling, ZK quorum, Headlamp, integration tests), see [`docs/installation.md`](docs/installation.md).

## Repo layout

```
src/main/java/edu/yu/velocitytrading/
├── exchange/              # Order matching, fill emission (REST + RSocket)
├── state/                 # TradingStateService — positions/fills owner; state.stream
├── exposurereservation/   # Global exposure budget; reservation lifecycle
├── marketmaker/           # MM node: quote generation, freshness, position tracking
├── cluster/               # Coordinator, symbol-shard assignment, Zk-watched listeners
├── ha/                    # LeaderLatch, LeaderGuardFilter, LeaderAwareRSocketClient
├── persistence/           # JPA entities + Hazelcast MapStores
├── memory/                # HazelcastRepository wrapper around IMap
└── model/                 # Records: Position, Fill, Quote, Reservation, StateSnapshot

docs/                      # design docs (see index below)
k8s/                       # Kustomize manifests for the 14-node K3s deploy
scripts/                   # PowerShell + bash helpers (image bundling, SSH setup, …)
compose.yml                # Local Docker Compose equivalent of the cluster
```

## Project Plan

- [Scope (scenarios covered)](docs/scope.md)
- [System Logic](docs/logic.md)
- [Distributed System Challenges](docs/ds-challenges.md)
- [Workflow Diagrams](docs/workflows.md)
- [Error-Case Walkthroughs](docs/error-cases.md)
- [Software Architecture Diagrams](docs/architecture.md)
- [Memory Architecture](docs/memory-architecture.md)
- [Tools & Technologies](docs/technologies.md)

## Installation and Usage Guide

- [Installation Instructions](docs/installation.md)
- [API and Usage Documentation](docs/api.md)
