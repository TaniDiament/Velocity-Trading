package edu.yu.marketmaker.state;

import com.hazelcast.core.HazelcastException;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Position;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yu.marketmaker.model.Side;
import edu.yu.marketmaker.model.StateSnapshot;
import edu.yu.marketmaker.service.ServiceHealth;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import edu.yu.marketmaker.ha.LeaderElectionService;

/**
 * Trading state service controls system-wide positions.
 */
@RestController
@Profile("trading-state")
public class TradingStateService {

    private static final Logger logger = LoggerFactory.getLogger(TradingStateService.class);

    private final Repository<String, Position> positionRepository;
    private final Repository<UUID, Fill> fillRepository;
    private final LeaderElectionService leaderElection;
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();


    /**
     * Hot multicast sink – every call to {@code submitFill} that results in a
     * position change emits one {@link StateSnapshot} here.  All active
     * {@code streamState} subscribers receive the event in real time.
     * {@code onBackpressureBuffer} keeps a small history so a slow subscriber
     * does not block the publisher.
     */
    private final Sinks.Many<StateSnapshot> positionSink =
            Sinks.many().multicast().onBackpressureBuffer();

    /**
     * Constructor.
     * @param positionRepository
     * @param fillRepository
     */
public TradingStateService(Repository<String, Position> positionRepository,
                           Repository<UUID, Fill> fillRepository,
                           LeaderElectionService leaderElection) {
    this.positionRepository = positionRepository;
    this.fillRepository = fillRepository;
    this.leaderElection = leaderElection;
}

    /**
     * HTTP: submit a fill via POST /state/fills
     *
     * @param fill the fill to record
     */
    @PostMapping("/state/fills")
    ResponseEntity<Void> submitFill(@RequestBody Fill fill) {
        try {
            processFill(fill);
            return ResponseEntity.ok().build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (HazelcastException e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * TCP/RSocket: submit a fill via request-response on route {@code "state.fills"}.
     *
     * @param fill the fill to record
     * @return {@link Mono} that completes empty on success, or errors on invalid input
     */
@MessageMapping("state.fills")
public Mono<Void> submitFillRSocket(@Payload Fill fill) {
    if (!leaderElection.isLeader()) {
        logger.warn("Rejecting RSocket fill {} — this replica is not the leader", fill.getId());
        return Mono.error(new IllegalStateException("not leader"));
    }
    try {
        processFill(fill);
        return Mono.empty();
    } catch (IllegalArgumentException | HazelcastException e) {
        return Mono.error(e);
    }
}

    /**
     * Shared logic for both HTTP and RSocket submitFill endpoints.
     * Persists the fill, updates the position, and broadcasts
     * a {@link StateSnapshot} to all active {@code state.stream} subscribers.
     * Per-symbol locking ensures concurrent fills on the same symbol do not lose updates.
     *
     * @param fill the fill to process
     * @throws HazelcastException if the underlying repository fails
     */
    private void processFill(Fill fill) {
        logger.info("Processing fill: id={}, symbol={}, side={}, quantity={}", fill.getId(), fill.symbol(), fill.side(), fill.quantity());
        String symbol = fill.symbol();
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            Optional<Position> position = positionRepository.get(symbol);
            fillRepository.put(fill);
            int quantity = fill.side() == Side.BUY ? fill.quantity() : -fill.quantity();
            Position updatedPosition;
            if (position.isPresent()) {
                int newQuantity = position.get().netQuantity() + quantity;
                updatedPosition = new Position(symbol, newQuantity, position.get().version() + 1, fill.getId());
                logger.info("Updated existing position: symbol={}, newNetQuantity={}, version={}", updatedPosition.symbol(), updatedPosition.netQuantity(), updatedPosition.version());
            } else {
                updatedPosition = new Position(symbol, quantity, 0, fill.getId());
                logger.info("Created new position: symbol={}, netQuantity={}", updatedPosition.symbol(), updatedPosition.netQuantity());
            }
            positionRepository.put(updatedPosition);
            logger.info("Persisted position for symbol={}, emitting StateSnapshot to sink", symbol);
            positionSink.tryEmitNext(new StateSnapshot(updatedPosition, fill));
        }
    }

    /**
     * HTTP: get all current positions via GET /positions
     *
     * @return a collection of positions
     */
    @GetMapping("/positions")
    Collection<Position> getAllPositions() {
        return positionRepository.getAll();
    }

    /**
     * TCP/RSocket: get all current positions via request-stream on route {@code "positions"}.
     * Each position is emitted as a separate item, then the stream completes.
     *
     * @return a {@link Flux} that emits every current {@link Position} and then completes
     */
    @MessageMapping("positions")
    public Flux<Position> getAllPositionsRSocket() {
        return Flux.fromIterable(positionRepository.getAll());
    }

    /**
     * HTTP: get a specific position via GET /positions/{symbol}
     *
     * @param symbol ticker symbol
     * @return the position, if present
     */
    @GetMapping("/positions/{symbol}")
    Optional<Position> getPosition(@PathVariable String symbol) {
        return positionRepository.get(symbol);
    }

    /**
     * TCP/RSocket: get a specific position via request-response on route
     * {@code "positions.{symbol}"}.
     * Example route: {@code "positions.AAPL"}
     *
     * @param symbol ticker symbol extracted from the route
     * @return a {@link Mono} emitting the {@link Position}, or empty if not found
     */
    @MessageMapping("positions.{symbol}")
    public Mono<Position> getPositionRSocket(@DestinationVariable String symbol) {
        return positionRepository.get(symbol)
                .map(Mono::just)
                .orElse(Mono.empty());
    }

    /**
     * RSocket request-stream endpoint.
     * Connect via TCP to port 7000 ({@code spring.rsocket.server.port}) and
     * send route {@code "state.stream"}.
     * <p>
     * The subscriber first receives a {@link StateSnapshot} for every position
     * that already exists at subscription time (the current snapshot), and then
     * continues to receive a new {@link StateSnapshot} every time a fill is
     * submitted that updates a position – i.e. the stream never completes while
     * the connection is open.
     *
     * @return a hot {@link Flux} of {@link StateSnapshot} items
     */
@MessageMapping("state.stream")
public Flux<StateSnapshot> streamPositions() {
    if (!leaderElection.isLeader()) {
        return Flux.error(new IllegalStateException("not leader — reconnect to current leader"));
    }
    Flux<StateSnapshot> currentState = Flux.fromIterable(positionRepository.getAll())
                .map(position -> {
                    Fill lastFill = position.lastFillId() != null
                            ? fillRepository.get(position.lastFillId()).orElse(null)
                            : null;
                    return new StateSnapshot(position, lastFill);
                });

        return currentState.concatWith(positionSink.asFlux());
    }

    /**
     * HTTP: get every recorded fill. Used by the end-to-end test to audit
     * fill prices, sides, and quantities against the quotes that produced them.
     */
    @GetMapping("/state/fills")
    Collection<Fill> getAllFills() {
        return fillRepository.getAll();
    }

    /**
     * Get health of the trading state service.
     * @return
     */
    @GetMapping("/health")
    ServiceHealth getHealth() {
        return new ServiceHealth(true, 0, "Trading State Service");
    }

}
