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
  exchange->>exchange: Acquire per-symbol monitor
  exchange->>exchange: Look up active quote
  exchange->>exchange: Check now >= expiresAt
  Note over exchange: Quote has expired (boundary is inclusive)
  exchange->>user: Reject: "Quote {symbol} is expired"
```
**Outcome:** `FillOrderDispatcher` rejects with `OrderValidationException` whenever `System.currentTimeMillis() >= quote.expiresAt()` — equality counts as expired. `ExchangeServiceAdvice` maps the exception to HTTP 400. The publisher may retry once a fresh quote is published.

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
  maker->>maker: Generate quote: bid=10, ask=10<br/>(after ±100 per-symbol clamp: bidMax=180, askMax=20)
  maker->>reservation: Request reservation (bid=10, ask=10)
  reservation->>reservation: Available ask capacity = 5<br/>(firm-wide ask budget nearly exhausted)
  reservation->>maker: Partial grant (grantedBid=10, grantedAsk=5, status=PARTIAL)
  maker->>maker: Overwrite quote quantities with granted amounts
  maker->>exchange: Publish quote (bid=10, ask=5) via shared Hazelcast map
```
**Outcome:** Every reservation request carries both sides. When the firm-wide budget on one side is short, the reservation service grants only what is available on that side. `ProductionQuoteGenerator` always overwrites the proposed `bidQuantity` / `askQuantity` with `reservation.grantedBidQuantity()` / `grantedAskQuantity()` before saving, so the published quote can never exceed what was granted.

### Reservation denied entirely
```mermaid
sequenceDiagram
  participant state as Trading State Service
  participant maker as Market Maker Node
  participant reservation as Exposure Reservation Service
  state->>maker: Position update
  maker->>maker: Generate quote: bid=10, ask=10
  maker->>reservation: Request reservation (bid=10, ask=10)
  reservation->>reservation: Atomically release any prior reservation for this symbol
  reservation->>reservation: Available bid=0, ask=0 (firm-wide budgets exhausted)
  reservation->>maker: Response (grantedBid=0, grantedAsk=0, status=DENIED)
  maker->>maker: Overwrite quote quantities with granted amounts (0/0)
  maker->>exchange: Publish quote (bid qty=0, ask qty=0) via shared Hazelcast map
  Note over exchange: Any incoming order will be rejected with<br/>"Order could not be filled" (adjustedQty == 0)
  Note over maker: Next fill-driven or freshness-tick refresh re-attempts
```
**Outcome:** `ProductionQuoteGenerator` does **not** branch on `status` — it unconditionally writes a quote with the granted quantities (zero on both sides here) to the shared quote map. The prior reservation for this symbol was already released atomically inside `createReservation`. The exchange still sees an active, unexpired quote, but every incoming order fails the `adjustedQty == 0` check in `FillOrderDispatcher` and is rejected with `"Order could not be filled"`. The MM tries again on the next position update or, in a quiet market, on the next `QuoteFreshnessKeeper` tick.

### Stale position update arrives after a newer one
```mermaid
sequenceDiagram
  participant state as Trading State Service
  participant maker as Market Maker Node
  state->>maker: Position update (version=5)
  maker->>maker: Process and publish quote for version 5
  state->>maker: Position update (version=4, delayed/reordered)
  maker->>maker: Check: incoming(4) > lastSeen(5)? — no
  maker->>maker: Discard update
  Note over maker: Quote from version 5 remains active
```
**Outcome:** `MarketMaker.newVersion` only accepts a snapshot when `incoming > prev`, so **both older and equal-version snapshots are discarded** — a re-delivery of the same version after a `state.stream` reconnect will not re-trigger quoting either. This prevents stale or duplicated snapshots from overriding the freshest known position.

### Quote expires before it can be refreshed
```mermaid
sequenceDiagram
  participant keeper as QuoteFreshnessKeeper (in MM)
  participant state as Trading State Service
  participant generator as ProductionQuoteGenerator
  participant reservation as Exposure Reservation Service
  participant exchange as Exchange Service
  Note over exchange: Active quote TTL = 30s
  Note over keeper: Periodic tick (quote-refresh-interval-ms, default 10s)
  keeper->>keeper: For each assigned symbol, check expiresAt - now < quote-stale-threshold-ms (default 15s)
  keeper->>state: Fetch latest position (RSocket "positions.{symbol}")
  state->>keeper: Position (or default with version=-1 if absent)
  keeper->>generator: generateQuote(position, lastFill=null)
  generator->>reservation: RSocket "reservations" (bid+ask)<br/>(atomically supersedes prior reservation for symbol)
  reservation->>generator: ReservationResponse (granted, status)
  generator->>exchange: Write reserved quote to shared Hazelcast map
```
**Outcome:** `QuoteFreshnessKeeper` polls each MM-owned symbol on a fixed interval and refreshes any quote whose `expiresAt - now` is under `marketmaker.quote-stale-threshold-ms` (default 15s) — including quotes already past expiry or missing entirely. It bypasses `MarketMaker.handlePosition` (so the fill-ordering version tracker isn't polluted) and instead fetches a fresh position from the leader and calls `QuoteGenerator.generateQuote` directly. The new reservation request **atomically supersedes** the prior one for that symbol — no explicit release call is issued in the non-fault path. The explicit `reservations.{symbol}.release` route only fires from fault-injection paths (`ProductionQuoteGenerator` Error Case 10), and fills are netted via `apply-fill` rather than full release.

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
