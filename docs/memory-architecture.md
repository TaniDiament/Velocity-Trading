# Memory Architecture

Velocity Trading keeps all hot trading state in an **embedded Hazelcast in-memory
data grid (IMDG)** that lives inside every stateful service JVM, and backs each
distributed map with **PostgreSQL** through a write-through `MapStore`. Services
never touch the database or Hazelcast directly — they go through a generic
`Repository<K, T>` abstraction, so the same code path runs against the live grid
in production and against simple in-memory stubs in tests.

The result is partitioned, replicated, durable state shared across the cluster
without a separate cache tier: reads are served from RAM, writes flush
synchronously to Postgres, and a cold start eager-loads everything back from
Postgres.

## Layered view

```mermaid
graph TD
    subgraph svc["Service tier — repository consumers"]
        TS["Trading State<br/>positions + fills"]
        EX["Exchange<br/>quotes + external orders"]
        MM["Market Maker<br/>quotes"]
        ER["Exposure Reservation<br/>reservations"]
    end

    subgraph repo["Repository abstraction (edu.yu.velocitytrading.memory)"]
        RIF["Repository&lt;K, T extends Identifiable&lt;K&gt;&gt;<br/>get / put / getAll / delete"]
        HR["HazelcastRepository&lt;K, T&gt;<br/>wraps one IMap"]
    end

    subgraph hz["Hazelcast IMDG — embedded in every JVM (profile: !external-publisher & !position-ui)"]
        M1["positions<br/>IMap String → Position"]
        M2["fills<br/>IMap UUID → Fill"]
        M3["quotes<br/>IMap String → Quote"]
        M4["external-orders<br/>IMap UUID → ExternalOrder"]
        M5["reservations<br/>IMap String → Reservation"]
    end

    subgraph ms["MapStore bridge — write-through (writeDelaySeconds=0)"]
        S1["PositionMapStore"]
        S2["FillMapStore"]
        S3["QuoteMapStore"]
        S4["ExternalOrderMapStore"]
        S5["ReservationMapStore"]
    end

    subgraph jpa["Spring Data JPA (edu.yu.velocitytrading.persistence)"]
        J["Jpa{Position,Fill,Quote,ExternalOrder,Reservation}Repository<br/>extends BaseJpaRepository&lt;E, K&gt;"]
        ENT["{Position,Fill,Quote,ExternalOrder,Reservation}Entity<br/>record ⇄ entity via fromRecord / toRecord"]
    end

    DB[("PostgreSQL<br/>StatefulSet + PVC<br/>single durable backing store")]

    TS --> RIF
    EX --> RIF
    MM --> RIF
    ER --> RIF

    RIF -.implemented by.-> HR

    HR --> M1
    HR --> M2
    HR --> M3
    HR --> M4
    HR --> M5

    M1 <-->|"store ↓ / load ↑"| S1
    M2 <-->|"store ↓ / load ↑"| S2
    M3 <-->|"store ↓ / load ↑"| S3
    M4 <-->|"store ↓ / load ↑"| S4
    M5 <-->|"store ↓ / load ↑"| S5

    S1 --> J
    S2 --> J
    S3 --> J
    S4 --> J
    S5 --> J

    J --> ENT
    ENT --> DB
```

## How the pieces fit

**Repository abstraction** — `Repository<K, T extends Identifiable<K>>` exposes
`get / put / getAll / delete`. In production every map is wrapped by a
`HazelcastRepository<K, T>` (configured in `HazelcastConfig`). Tests swap in
in-memory implementations (`StaticPositionRepository`, `StaticQuoteRepository`,
`InMemoryReservationRepository`) behind the same interface, so service logic is
storage-agnostic.

**Hazelcast IMDG** — five `IMap`s hold the live state, keyed as shown above
(`positions`, `quotes`, and `reservations` are keyed by symbol; `fills` and
`external-orders` by `UUID`). The grid is embedded in every stateful service, so
all members form one cluster and share the same partitioned data. Key map
settings from `HazelcastConfig`:

- **No eviction** (`EvictionPolicy.NONE`) — the full working set stays resident.
- **`backupCount = 2`** — three copies of each partition survive two simultaneous
  member failures (the worst case during the rolling restart in error case 11).
- **`InitialLoadMode.EAGER`** — on startup each map reloads from Postgres via
  `loadAllKeys` / `loadAll` before the service reports ready.

**MapStore bridge (write-through)** — each `*MapStore` implements Hazelcast's
`MapStore` interface. Writes flush synchronously (`writeDelaySeconds = 0`, batched
at `writeBatchSize = 100`, `writeCoalescing = true`); reads on a miss load from
Postgres. Each store converts between the in-memory record (`Position`, `Fill`, …)
and its JPA entity using `fromRecord` / `toRecord`.

**Spring Data JPA + PostgreSQL** — `BaseJpaRepository<E, K>` (a
`@NoRepositoryBean` extension of `JpaRepository`) provides standard CRUD; one
concrete repository and one `IdentifiableEntity` exist per type. PostgreSQL runs
as a `StatefulSet` with a PVC and is the single durable backing store for every
map, which is what makes full-system-restart recovery possible.

## Read and write paths

```mermaid
sequenceDiagram
    participant Svc as Service
    participant Repo as HazelcastRepository
    participant Map as Hazelcast IMap
    participant Store as MapStore
    participant DB as PostgreSQL

    Note over Svc,DB: Write (put)
    Svc->>Repo: put(entity)
    Repo->>Map: map.put(id, entity)
    Map->>Store: store(key, value)
    Store->>DB: JpaRepository.save(entity)  (synchronous, writeDelaySeconds=0)

    Note over Svc,DB: Read (get) — cache miss
    Svc->>Repo: get(id)
    Repo->>Map: map.get(id)
    alt key resident in grid
        Map-->>Repo: value from RAM
    else not resident
        Map->>Store: load(key)
        Store->>DB: JpaRepository.findById(id)
        DB-->>Store: entity
        Store-->>Map: value (cached in grid)
        Map-->>Repo: value
    end
    Repo-->>Svc: Optional<T>
```