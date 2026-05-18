# Workflows

## Submitting External Orders
```mermaid
sequenceDiagram
  participant user as User
  participant exchange as Exchange Service
  participant state as Trading State Service
  user->>exchange: Submit order
  alt Order Validation Exception
    exchange->>user: Report failure
  else Order is Valid
    exchange->>exchange: Generate fill
    exchange->>state: Send fill
    exchange->>user: Report success
  end
```

### Order submitted against an expired quote
```mermaid
sequenceDiagram
  participant user as External Order Publisher
  participant exchange as Exchange Service
  user->>exchange: Submit order (POST /orders)
  exchange->>exchange: Look up active quote
  exchange->>exchange: Check expiresAt < currentTime
  Note over exchange: Quote has expired
  exchange->>user: Reject: "Quote is expired"
```
**Outcome:** The exchange checks `expiresAt` before matching. Expired quotes are never filled. The publisher receives a 400 error and may retry later when a fresh quote is available.

### Order quantity exceeds remaining quote quantity (partial fill)
```mermaid
sequenceDiagram
  participant user as External Order Publisher
  participant exchange as Exchange Service
  participant state as Trading State Service
  user->>exchange: Submit order (qty=15)
  exchange->>exchange: Active quote has remaining qty=10
  exchange->>exchange: Partial fill: adjustedQty = min(15, 10) = 10
  exchange->>exchange: Quote remaining qty → 0
  exchange->>state: Send fill (qty=10)
  exchange->>user: Report success (partial fill)
```
**Outcome:** The order is partially filled against the remaining quote quantity. The fill reflects only the actually executed quantity. The quote's remaining quantity is decremented accordingly.

### Concurrent orders on same quote
```mermaid
sequenceDiagram
  participant user1 as Publisher Thread 1
  participant user2 as Publisher Thread 2
  participant exchange as Exchange Service
  participant state as Trading State Service
  user1->>exchange: Submit BUY order (qty=8)
  user2->>exchange: Submit BUY order (qty=8)
  Note over exchange: Quote askQuantity = 10
  Note over exchange: Per-symbol monitor (symbolLocks) serializes both threads
  exchange->>exchange: Thread 1 acquires lock for symbol
  exchange->>exchange: Thread 1: adjustedQty = min(8, 10) = 8
  exchange->>exchange: Thread 1: update quote askQty = 2
  exchange->>state: Send fill (qty=8)
  exchange->>exchange: Thread 1 releases lock
  exchange->>exchange: Thread 2 acquires lock, re-reads askQty = 2
  exchange->>exchange: Thread 2: adjustedQty = min(8, 2) = 2
  exchange->>exchange: Thread 2: update quote askQty = 0
  exchange->>state: Send fill (qty=2)
```
**Outcome:** `FillOrderDispatcher` serializes all order handling for a given symbol through a per-symbol `synchronized` monitor (`symbolLocks`). The second thread always observes the already-decremented remaining quantity, so its fill is clamped to whatever is left (possibly zero, in which case `OrderValidationException("Order could not be filled")` is thrown). Over-fills cannot occur.

---

## Updating Quote
```mermaid
sequenceDiagram
  participant state as Trading State Service
  participant maker as Market Maker Node
  participant reservation as Reservation Service
  participant exchange as Exchange Service
  state->>maker: Send position update
  maker->>maker: Generate new quote
  maker->>reservation: Update reservation
  reservation->>maker: Send actual reservation
  maker->>exchange: Send updated quote
```

### Reservation is partially granted
```mermaid
sequenceDiagram
  participant state as Trading State Service
  participant maker as Market Maker Node
  participant reservation as Exposure Reservation Service
  participant exchange as Exchange Service
  state->>maker: Position update (position = -80)
  maker->>maker: Generate quote: bid=10, ask=10
  maker->>reservation: Request reservation (ask=10)
  reservation->>reservation: Available capacity = 5
  reservation->>maker: Partial grant (granted=5, status=PARTIAL)
  maker->>maker: Reduce ask quantity to 5
  maker->>exchange: Publish quote (bid=10, ask=5)
  exchange->>exchange: Activate reduced quote
```
**Outcome:** When insufficient exposure capacity exists, the reservation service grants only what is available. The market maker deterministically reduces the quote quantity to match and publishes a smaller quote. This prevents exposure limit violations while still providing some liquidity.

### Reservation denied entirely
```mermaid
sequenceDiagram
  participant state as Trading State Service
  participant maker as Market Maker Node
  participant reservation as Exposure Reservation Service
  state->>maker: Position update
  maker->>maker: Generate quote: bid=10, ask=10
  maker->>reservation: Request reservation (ask=10)
  reservation->>reservation: Available capacity = 0
  reservation->>maker: Denied (granted=0, status=DENIED)
  Note over maker: Cannot publish quote
  Note over maker: Allow existing quote to expire
  Note over maker: Retry on next position update
```
**Outcome:** No exposure capacity is available. The market maker cannot publish a quote for this symbol. The existing quote (if any) expires naturally. The market maker will retry when it receives the next position update or when capacity becomes available.

### Stale position update arrives after a newer one
```mermaid
sequenceDiagram
  participant state as Trading State Service
  participant maker as Market Maker Node
  state->>maker: Position update (version=5)
  maker->>maker: Process and publish quote for version 5
  state->>maker: Position update (version=4, delayed/reordered)
  maker->>maker: Check: version 4 < last seen version 5
  maker->>maker: Discard stale update
  Note over maker: Quote from version 5 remains active
```
**Outcome:** The market maker tracks the last processed position version per symbol. Updates with older versions are discarded to prevent stale quotes from overriding newer state.

### Quote expires before it can be refreshed
```mermaid
sequenceDiagram
  participant keeper as QuoteFreshnessKeeper (in MM)
  participant maker as Market Maker Node
  participant reservation as Exposure Reservation Service
  participant exchange as Exchange Service
  Note over exchange: Active quote TTL = 30s
  Note over keeper: Periodic tick (refresh-interval-ms)
  keeper->>keeper: For each assigned symbol, check expiresAt - now < staleThresholdMs
  keeper->>maker: Trigger refresh for stale symbol
  maker->>maker: Generate fresh quote
  maker->>reservation: POST /reservations (atomically supersedes prior reservation for symbol)
  reservation->>maker: ReservationResponse (granted, status)
  maker->>exchange: PUT /quotes/{symbol}
```
**Outcome:** `QuoteFreshnessKeeper` polls each MM-owned symbol on a fixed interval and refreshes any quote whose `expiresAt - now` is under `marketmaker.quote-stale-threshold-ms` (default 15s). The new reservation request **atomically supersedes** the prior one for that symbol on the reservation service — no explicit release call is issued in the non-fault path. The explicit `POST /reservations/{symbol}/release` only fires from fault-injection paths (`ProductionQuoteGenerator`) and on fill via `apply-fill`.

---

## Streaming Position Data Updates

The `trading-state` service publishes position updates over two independent transports:

- **STOMP over SockJS WebSocket** (`/ws`) — used by the browser-based Position Display UI (`static/index.html`). Clients receive an initial snapshot via `SUBSCRIBE /app/positions.snapshot` and live deltas via `SUBSCRIBE /topic/positions`.
- **RSocket request-stream** on route `state.stream` (TCP port 7000) — used by **market-maker pods** (`PositionTracker.java`) to consume the same updates internally.

Both transports are fed from the same `Sinks.Many<StateSnapshot>` multicast publisher inside `TradingStateService`, so all subscribers see identical data.

```mermaid
sequenceDiagram
  participant ui as Position UI (browser)
  participant mm as Market Maker pod
  participant state as Trading State Service
  participant exchange as Exchange Service
  ui->>state: SockJS+STOMP connect (/ws), SUBSCRIBE /topic/positions
  mm->>state: RSocket request-stream (route state.stream)
  loop Until disconnect
    exchange->>state: Send fill
    state->>state: Update position, emit StateSnapshot to multicast sink
    state->>ui: STOMP frame on /topic/positions
    state->>mm: RSocket onNext(StateSnapshot)
  end
```

### UI connects but no positions exist yet
```mermaid
sequenceDiagram
  participant ui as Position Display UI
  participant state as Trading State Service
  ui->>state: SockJS+STOMP connect (/ws)
  ui->>state: SUBSCRIBE /app/positions.snapshot
  state->>state: No positions in repository
  state->>ui: Empty initial snapshot (JSON [])
  ui->>state: SUBSCRIBE /topic/positions
  Note over ui: UI shows empty state
  Note over state: Later, first fill arrives
  state->>state: Create first position, emit StateSnapshot
  state->>ui: STOMP frame on /topic/positions
  Note over ui: UI flashes new row
```
**Outcome:** The UI handles the empty state gracefully and updates dynamically as positions are created.

### Multiple UI clients connected simultaneously
```mermaid
sequenceDiagram
  participant ui1 as UI Client 1
  participant ui2 as UI Client 2
  participant state as Trading State Service
  ui1->>state: SockJS+STOMP connect, SUBSCRIBE /topic/positions
  ui2->>state: SockJS+STOMP connect, SUBSCRIBE /topic/positions
  state->>ui1: Initial snapshot via /app/positions.snapshot
  state->>ui2: Initial snapshot via /app/positions.snapshot
  Note over state: Fill arrives, position updated
  state->>state: Emit StateSnapshot to multicast sink
  state->>ui1: STOMP frame on /topic/positions
  state->>ui2: STOMP frame on /topic/positions
```
**Outcome:** The multicast `Sinks.Many<StateSnapshot>` fans out to every active STOMP subscriber (and every RSocket subscriber on `state.stream`). All UI clients see consistent, real-time position data.
