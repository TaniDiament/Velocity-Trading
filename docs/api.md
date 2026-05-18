# Market-Maker HTTP API Reference

All endpoints are REST (JSON over HTTP) unless marked otherwise. Each service is activated by a Spring profile — the same Docker image runs every role; the active profile is selected via `SPRING_PROFILES_ACTIVE`.

Several services additionally expose **RSocket** (TCP, port 7000) and **STOMP-over-WebSocket** routes alongside their HTTP surface. Those are listed in each section.

---

## Exchange Service (`exchange` profile)

| Method | Path | Description |
|--------|------|-------------|
| **GET** | `/quotes/{symbol}` | Retrieve the current quote for a ticker. Returns **404** if the symbol has no active quote. |
| **PUT** | `/quotes/{symbol}` | Create or update a quote (used to bootstrap). Body: `Quote` JSON. Only inserts when no quote exists for the symbol — subsequent updates flow in through normal fill processing. |
| **POST** | `/orders` | Submit an external order for matching. Body: `ExternalOrder`. Returns **400** with `OrderValidationException` if the quote is missing, expired, the limit price doesn't cross, or remaining quote quantity is zero. |
| **GET** | `/health` | Health-check. Returns `ServiceHealth` (`{ healthy, uptime, name }`). |

### Error handling (`ExchangeServiceAdvice`)

| Exception | HTTP Status | Triggered by |
|-----------|-------------|--------------|
| `QuoteNotFoundException` | **404 Not Found** | `GET /quotes/{symbol}` for a symbol with no active quote. |
| `OrderValidationException` | **400 Bad Request** | `POST /orders` with any of: missing quote, expired quote, limit price not crossing, or zero executable quantity. |

---

## Exposure Reservation Service (`exposure-reservation` profile)

### HTTP

| Method | Path | Description |
|--------|------|-------------|
| **POST** | `/reservations` | Request exposure capacity for a quote on both bid and ask sides. Body: `Quote`. Returns `ReservationResponse` (`{ id, status, grantedBidQuantity, grantedAskQuantity }`) with status `GRANTED`, `PARTIAL`, or `DENIED`. A new request for an existing symbol atomically supersedes the prior reservation. |
| **POST** | `/reservations/{symbol}/apply-fill` | Apply a fill against an existing reservation (keyed by **symbol**, not id). Body: `Fill`. Reduces the granted side by `min(grantedSide, fill.quantity)`. Returns `FreedCapacityResponse` (`{ freedCapacity }`). |
| **POST** | `/reservations/{symbol}/release` | Manually release a reservation when a quote is cancelled or replaced without being filled. No body. Returns `FreedCapacityResponse`. |
| **GET** | `/exposure` | Current global exposure: `{ bidUsage, askUsage, totalCapacity, activeReservationCount }`. |
| **GET** | `/health` | Returns `ServiceHealth`. |

### RSocket (TCP port 7000)

Every HTTP endpoint above has an equivalent RSocket route with the same payload and semantics:

| Route | Pattern | Equivalent HTTP |
|-------|---------|-----------------|
| `reservations` | request-response | `POST /reservations` |
| `reservations.{symbol}.apply-fill` | request-response | `POST /reservations/{symbol}/apply-fill` |
| `reservations.{symbol}.release` | request-response | `POST /reservations/{symbol}/release` |
| `exposure` | request-response | `GET /exposure` |
| `health` | request-response | `GET /health` |

### Reservation status semantics

`ReservationStatus` is computed per-request in `ExposureReservationService.determineStatus`:

- **GRANTED** — both sides granted in full.
- **DENIED** — both sides requested > 0 but granted 0.
- **PARTIAL** — anything else (one side fully granted, one not; one side denied, the other partial; etc.).

---

## Trading State Service (`trading-state` profile)

### HTTP

| Method | Path | Description |
|--------|------|-------------|
| **POST** | `/state/fills` | Submit a fill to update system-wide positions. Body: `Fill`. Returns **200** on success, **400** for `IllegalArgumentException`, **500** for `HazelcastException`. Side effect: emits a `StateSnapshot` on the multicast sink (visible to STOMP and RSocket subscribers). |
| **GET** | `/state/fills` | Audit endpoint — returns every recorded `Fill`. Used by the end-to-end test to verify fill prices, sides, and quantities against the quotes that produced them. |
| **GET** | `/positions` | All current positions (`Collection<Position>`). |
| **GET** | `/positions/{symbol}` | Single position by symbol; returns `Optional<Position>`. |
| **GET** | `/quotes/{symbol}` | Same-origin proxy to the Exchange's `GET /quotes/{symbol}` so the Position UI avoids CORS. Returns **404** if the proxy call fails. |
| **GET** | `/leader-info` | Current `trading-state` leader endpoint from the ZK-backed `ServiceRegistry`. Returns `{ leaderHost, httpPort, rsocketPort }`, or `{ leaderHost: "" }` if indeterminate. Any replica (leader or standby) returns the same answer. |
| **GET** | `/health` | Returns `ServiceHealth`. |

### RSocket (TCP port 7000)

| Route | Pattern | Description |
|-------|---------|-------------|
| `state.fills` | request-response | Submit a fill (same logic as `POST /state/fills`). Rejected with `IllegalStateException("not leader")` on non-leader replicas. |
| `positions` | request-stream | Emit every current `Position`, then complete. |
| `positions.{symbol}` | request-response | Get a single `Position` by symbol. |
| `state.stream` | request-stream | Subscribe to a hot `Flux<StateSnapshot>` — initial snapshot for every existing position, then live deltas on every new fill. Stream never completes while connected. Rejected on non-leader replicas. Used by **market-maker pods** (`PositionTracker`). |

### STOMP over WebSocket (SockJS endpoint `/ws`)

Used by the browser-based Position Display UI (`src/main/resources/static/index.html`). Fed by the same multicast sink as `state.stream`.

| Destination | Direction | Description |
|-------------|-----------|-------------|
| `/app/positions.snapshot` | client → server SUBSCRIBE | Server replies once with `List<StateSnapshot>` for every current position so the UI can render an initial table. |
| `/topic/positions` | server → client | Live `StateSnapshot` per position update. One frame per fill. |

---

## External Order Publisher (`external-publisher` profile)

Test-driven traffic generator. The container boots idle and exposes endpoints the end-to-end test calls to drive the system deterministically. All writes target the current **exchange leader** (resolved via `ServiceRegistry`); the publisher retries briefly on 503 / `ConnectException` to absorb in-flight failovers.

| Method | Path | Description |
|--------|------|-------------|
| **POST** | `/publisher/seed-quotes` | PUT a fixed bootstrap quote per symbol on the exchange (bid 99.50/qty 1000, ask 100.50/qty 1000, 5-minute TTL). Body: `List<String>` of symbols. Returns the generated `List<UUID>` of `quoteId`s so the test can distinguish bootstrap quotes from market-maker–authored quotes later on. |
| **POST** | `/publisher/submit-orders?count=N` | Submit `N` orders per symbol with alternating BUY/SELL sides and limit prices that cross both the bootstrap spread (99.50/100.50) and the tighter production spread (99.95/100.05). Body: `List<String>` of symbols. Returns an `Integer` count of orders the exchange accepted (HTTP 200); non-200 responses are logged but not counted, since stale quotes / empty residual depth legitimately reject. |
| **GET** | `/health` | Returns `ServiceHealth`. |

---

## Market Maker Node (`market-maker-node` profile)

Each market-maker pod (`mm-0..mm-6` in the cluster) hosts two operator/test HTTP controllers.

### Market-Maker introspection (`MarketMakerStatusController`)

| Method | Path | Description |
|--------|------|-------------|
| **GET** | `/marketmaker/status` | This node's view of its own work: `{ assigned: Set<String>, forwardsBySymbol: Map<String, Long> }`. `assigned` is the symbols the coordinator has given this node; `forwardsBySymbol` counts how many snapshot frames the leader has forwarded for each symbol. Used by the cluster integration test to verify routing. |

### Cluster admin (`SymbolAdminController`, base path `/cluster`)

| Method | Path | Description |
|--------|------|-------------|
| **GET** | `/cluster/status` | Cluster state as seen from this node: `{ nodeId, leader, leaderId, members, symbols }`. Available on every node (handy for round-robin debugging). |
| **POST** | `/cluster/symbols` | Append a symbol; the znode change triggers a rebalance. Body: `{ symbol: String }`. Returns `{ symbol, added, symbols }`. **Leader-only**: non-leader returns **503** with body `{ error: "not leader", leader, thisNode }` and `X-Leader` response header. Empty/blank symbol returns **400**. |
| **DELETE** | `/cluster/symbols/{symbol}` | Remove a symbol; znode change triggers a rebalance. Returns `{ symbol, removed, symbols }`. **Leader-only**: same 503 + `X-Leader` semantics as POST. |

#### Leader-guard convention

Mutating endpoints under `/cluster` (and the exchange's mutating endpoints, via `LeaderGuardFilter`) reply **503 Service Unavailable** when called against a non-leader replica and include an `X-Leader` header naming the current leader so clients can retry against the correct pod.

---

## Fault Injection (`fault-injection` profile, base path `/test/fault-injection`)

Test-only surface for `FaultInjector`. Used exclusively by error-case 10 integration tests (`LocalError10MMCrashDuringQuoteReplaceTest` / `ClusterError10MMCrashDuringQuoteReplaceTest`). No production code path arms the injector.

| Method | Path | Description |
|--------|------|-------------|
| **POST** | `/test/fault-injection/arm-quote-replace-crash?symbol=…` | Arm this market-maker to crash the next time it processes a quote-replacement cycle for `symbol`. The crash releases the old reservation first (matching error case 10's documented sequence), then hard-halts the JVM via `Runtime#halt`. Idempotent — re-arming overwrites the prior armed symbol. Returns `ArmedStatus(armedSymbol)`. |
| **GET** | `/test/fault-injection/status` | Returns `ArmedStatus(armedSymbol)` — the currently armed symbol, or `null` if not armed. |

---

## Health endpoints across services

Every service exposes **`GET /health`** returning a `ServiceHealth` record. The kubelet readiness/liveness probes hit this path.

| Profile | `name` field returned |
|---------|----------------------|
| `exchange` | `"Exchange Service"` |
| `exposure-reservation` | `"Exposure Reservation Service"` |
| `trading-state` | `"Trading State Service"` |
| `external-publisher` | `"External Order Publisher"` |