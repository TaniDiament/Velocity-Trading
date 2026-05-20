# Technologies

## [Hazelcast](https://hazelcast.com)
Embedded in every JVM as the distributed in-memory data grid. Each service-tier IMap (`positions`, `fills`, `quotes`, `external-orders`, `reservations`) is backed by a JPA-based `MapStore` so reads load from PostgreSQL on miss and writes flush synchronously through (`writeDelaySeconds=0`). This is what gives the system durable, partitioned state across the cluster without bolting on a separate cache layer.

## [PostgreSQL](https://www.postgresql.org)
Single durable backing store for every IMap. Runs as a `StatefulSet` with a PVC so it survives pod restarts — the foundation of the error-case-11 (full system restart) recovery story.

## [Apache ZooKeeper](https://zookeeper.apache.org)
Coordination layer. Hosts:
- **Leader election** for each stateful service (`trading-state`, `exchange`, `exposure-reservation`) and for the market-maker cluster coordinator — via Curator's `LeaderLatch`. Only the leader accepts mutating HTTP / RSocket writes; the `LeaderGuardFilter` rejects writes to standbys with **503**.
- **Service discovery**. Each leader publishes its address as an ephemeral znode under `/mm/endpoints/<service>`, watched via `CuratorCache`. Clients (e.g. the external publisher's `sendToExchangeLeader`) resolve the current leader on every call so failover is invisible.
- **Symbol-shard assignments**. The MM coordinator writes per-node assignment znodes under `/marketmaker/assignments/<nodeId>`; each `AssignmentListener` watches its own znode and reconciles symbols locally.

## [Spring Boot](https://spring.io/projects/spring-boot)
Glue framework. Per-service profiles (`exchange`, `trading-state`, `exposure-reservation`, `market-maker-node`, `external-publisher`, `position-ui`) select which controllers and beans are loaded from the single shared image.

### [RSocket (TCP, port 7000)](https://rsocket.io)
Used for inter-service messaging. Mostly request/response (`reservations`, `reservations.{symbol}.apply-fill`, `positions.{symbol}`) and one critical request-stream (`state.stream`, fed by `trading-state`'s multicast `Sinks.Many<StateSnapshot>`). Routed through `LeaderAwareRSocketClient` which evicts the cached requester on connection failure so callers heal after a leader change instead of looping on `ECONNREFUSED`.

### STOMP over SockJS WebSocket
Browser UI transport. The Position Display page (`static/index.html`, served by `trading-state`) subscribes via `/ws` and receives the same multicast `StateSnapshot` stream as the RSocket consumers.

## [Kubernetes (K3s)](https://k3s.io)
Production deployment target — a 14-node K3s cluster (3 control planes, 11 workers, see `docs/installation.md`). Each app tier is a `StatefulSet` (or `Deployment` for the publisher) so pod identity is stable, Hazelcast member identity is stable, and PVC-backed durable state (postgres, ZK) survives pod churn. Air-gapped image distribution is bundled in `dist/images.tar` and imported with `k3s ctr images import`.

## [Docker](https://www.docker.com)
Same image (`market-maker:1.0.0`) runs every role; `Dockerfile.offline` is the production build used by the K3s bundle. `compose.yml` provides a local laptop equivalent of the K3s stack for the `LocalError*` integration tests.

## Java 21
The system is implemented in Java 21 (`maven-compiler-plugin` release=21). Records used heavily for DTOs (`Position`, `Fill`, `Quote`, `StateSnapshot`, …); pattern-matching `switch` for side handling.

