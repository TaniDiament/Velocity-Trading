package edu.yu.marketmaker.state;

import com.hazelcast.core.HazelcastException;
import edu.yu.marketmaker.memory.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yu.marketmaker.model.*;
import edu.yu.marketmaker.service.ServiceHealth;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import edu.yu.marketmaker.ha.LeaderElectionService;
import edu.yu.marketmaker.ha.ServiceRegistry;

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
    private final ServiceRegistry serviceRegistry;
    private final ObjectProvider<SimpMessagingTemplate> messagingProvider;
    private final RestClient exchangeClient;
    private volatile Disposable bridgeSubscription;


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
                               LeaderElectionService leaderElection,
                               ServiceRegistry serviceRegistry,
                               ObjectProvider<SimpMessagingTemplate> messagingProvider,
                               @Value("${exchange.base-url:http://exchange:8080}") String exchangeBaseUrl) {
        this.positionRepository = positionRepository;
        this.fillRepository = fillRepository;
        this.leaderElection = leaderElection;
        this.serviceRegistry = serviceRegistry;
        this.messagingProvider = messagingProvider;
        this.exchangeClient = RestClient.builder().baseUrl(exchangeBaseUrl).build();
    }

    /**
     * Bridges the in-memory {@link #positionSink} to the STOMP topic
     * {@code /topic/positions} so browser UIs can receive live updates.
     * <p>
     * Only the leader replica actually publishes. The bridge is (re)started on
     * leader acquisition and torn down on leader loss, so non-leader replicas
     * never push state to UI clients even though their sinks would still emit.
     */
    @PostConstruct
    void initWebSocketBridge() {
        SimpMessagingTemplate messaging = messagingProvider.getIfAvailable();
        if (messaging == null) {
            logger.info("No SimpMessagingTemplate present — WebSocket bridge disabled");
            return;
        }
        leaderElection.addListener(new LeaderElectionService.LeadershipListener() {
            @Override public void onLeaderAcquired() { startBridge(messaging); }
            @Override public void onLeaderLost()    { stopBridge(); }
        });
        if (leaderElection.isLeader()) {
            startBridge(messaging);
        }
    }

    private synchronized void startBridge(SimpMessagingTemplate messaging) {
        if (bridgeSubscription != null && !bridgeSubscription.isDisposed()) return;
        logger.info("Starting WebSocket position bridge — /topic/positions");
        bridgeSubscription = positionSink.asFlux()
                .publishOn(Schedulers.boundedElastic())
                .subscribe(
                        snap -> messaging.convertAndSend("/topic/positions", snap),
                        err  -> logger.error("WebSocket bridge errored", err));
    }

    private synchronized void stopBridge() {
        if (bridgeSubscription != null) {
            logger.info("Stopping WebSocket position bridge — leader lost");
            bridgeSubscription.dispose();
            bridgeSubscription = null;
        }
    }

    @PreDestroy
    void shutdownBridge() { stopBridge(); }

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

    /**
     * STOMP: served when a browser subscribes to {@code /app/positions.snapshot}.
     * Returns every current position once so the UI can render its initial table
     * before live deltas start arriving on {@code /topic/positions}.
     */
    @SubscribeMapping("/positions.snapshot")
    public java.util.List<StateSnapshot> snapshot() {
        return positionRepository.getAll().stream()
                .map(p -> new StateSnapshot(
                        p,
                        p.lastFillId() != null
                                ? fillRepository.get(p.lastFillId()).orElse(null)
                                : null))
                .toList();
    }

    /**
     * HTTP: proxy to the Exchange service so the UI can fetch the latest quote
     * for a symbol from the same origin (avoids CORS). Refreshed by the UI on
     * each position update — quote freshness is therefore bounded by fill rate,
     * not true real-time.
     */
    @GetMapping("/quotes/{symbol}")
    ResponseEntity<Quote> getQuoteProxy(@PathVariable String symbol) {
        try {
            Quote quote = exchangeClient.get()
                    .uri("/quotes/{symbol}", symbol)
                    .retrieve()
                    .body(Quote.class);
            return ResponseEntity.ok(quote);
        } catch (Exception e) {
            logger.debug("Quote proxy failed for {}: {}", symbol, e.getMessage());
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * HTTP: return the current trading-state leader's advertised hostname.
     * Sourced from {@link ServiceRegistry}'s ZK-backed cache so any replica
     * (leader or standby) returns the same answer.
     */
    @GetMapping("/leader-info")
    Map<String, String> getLeaderInfo() {
        return serviceRegistry.getLeaderAddress("trading-state")
                .map(ep -> Map.of(
                        "leaderHost", ep.host,
                        "httpPort", String.valueOf(ep.httpPort),
                        "rsocketPort", String.valueOf(ep.rsocketPort)))
                .orElse(Map.of("leaderHost", ""));
    }
}
