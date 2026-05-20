# Use Cases

Sequence diagrams for the happy paths and edge cases below live in [`workflows.md`](workflows.md); failure-mode sequence diagrams live in [`error-cases.md`](error-cases.md).

## Submitting External Orders

External orders are submitted to the exchange (`POST /orders`), matched against the active quote for the symbol, and fan out into a fill, a position update, and a reservation apply-fill.

### Error Cases
- Exchange service instance goes down before handling the order (Error Case 1)
- Exchange service instance goes down after sending the fill but before providing confirmation (Error Case 2)
- Trading state service instance goes down before handling the fill (Error Case 3)

## Updating Quote

The quote is updated by the market-maker in response to position changes (`state.stream` deltas) and via the `QuoteFreshnessKeeper` safety-net so quotes never lapse during quiet periods.

### Error Cases
- Market maker node goes down before handling the position update (Error Case 4)
- Market maker node goes down after sending updated reservation but before sending new quote (Error Case 5)
- Reservation service instance goes down before updating reservation (Error Case 6)
- Exchange service instance goes down before updating quote (Error Case 7)
- Market maker crashes during a quote replacement cycle (Error Case 10)

## Streaming Position Data Updates

Updated positions are streamed to both the browser UI (STOMP-over-WebSocket on `/ws`) and to market-maker pods (RSocket `state.stream`), both fed by the same multicast `Sinks.Many<StateSnapshot>` inside `trading-state`.

### Error Cases
- Connected trading state service instance goes down (Error Case 8)

## Full System Restart

Every service recovers from durable PostgreSQL storage via Hazelcast `MapStore.loadAll` on startup. Market makers reconnect to `state.stream`, see the recovered positions, and resume quoting. See Error Case 11 in [`error-cases.md`](error-cases.md).
