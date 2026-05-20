# Market Maker Trading System: Technical Specification

## Distributed Systems Challenges & Solutions

### 1. Naming & Communication
**Challenge:** Ensuring diverse services can discover and interact with each other reliably.
* **Implementation:**
    * **REST/HTTP:** Used for synchronous external orders and quote management via Spring Boot.
    * **TCP:** Facilitates asynchronous, decoupled inter-service communication (e.g., Exchange notifying Trading State of fills).
    * **WebSockets:** Used by the UI and Market Maker nodes for real-time position updates.



### 2. Coordination
**Challenge:** Managing the interaction between autonomous nodes to avoid race conditions.
* **Implementation:**
    * **Leader Election:** Powered by **Zookeeper** to ensure only one Market Maker node manages a specific ticker symbol at a time.
    * **Version Monotony:** The Trading State Service attaches a `position_version` to every update. Consumers (UI and Market Makers) use this to ignore stale or out-of-order data.

### 3. Scheduling
**Challenge:** Efficiently distributing tasks across hardware resources.
* **Implementation:**
    * **Ticker Sharding:** Workload is split by symbol across the Market Maker cluster.
    * **Orchestration:** Managed via **K3s (Kubernetes)** to handle container deployment, scaling, and resource allocation.

### 4. Replication & Consistency
**Challenge:** Maintaining a single "Source of Truth" across distributed databases.
* **Implementation:**
    * **Atomic Reservations:** The Exposure service ensures all reservation requests are atomic and persistent.
    * **Idempotency:** Unique request IDs prevent duplicate fills or reservations from corrupting state during network retries.



### 5. Availability & Fault Tolerance
**Challenge:** Maintaining system uptime during individual component failures.
* **Implementation:**
    * **Durable Hazelcast MapStores:** Every IMap (`positions`, `fills`, `quotes`, `reservations`, `external-orders`) is write-through to PostgreSQL (`writeDelaySeconds=0`) and eager-loaded on startup (`InitialLoadMode.EAGER`). After a full system restart every service rebuilds in-memory state from the durable rows — see Error Case 11 in [`error-cases.md`](error-cases.md).
    * **Leader election with standby guard:** Each stateful tier (`trading-state`, `exchange`, `exposure-reservation`) runs N replicas behind a ZK `LeaderLatch`. `LeaderGuardFilter` rejects mutating HTTP / RSocket calls on standbys with **503** so a stale client can't race a write into a non-leader.
    * **Fail-safe quote expiration:** The exchange rejects orders against expired quotes (`FillOrderDispatcher` checks `expiresAt`); the market-maker treats an expired survivor as nonexistent when it regenerates (`ProductionQuoteGenerator`), so a stuck MM never republishes a stale quote.
    * **TTL'd quote freshness keeper:** `QuoteFreshnessKeeper` polls each MM-owned symbol and refreshes any quote within `quote-stale-threshold-ms` of expiry, breaking the chicken-and-egg of "no fill → no regen → quote expires → still no fill."