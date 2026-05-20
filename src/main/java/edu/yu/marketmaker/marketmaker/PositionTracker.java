package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.cluster.ClusterNode;
import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.model.StateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@Component
@Profile("market-maker-node & !test-position-tracker")
public class PositionTracker implements SnapshotTracker {

    private static final Logger log = LoggerFactory.getLogger(PositionTracker.class);
    private static final String TRADING_STATE_SERVICE = "trading-state";
    private static final Duration STREAM_RECONNECT_DELAY = Duration.ofSeconds(2);

    // Thread-safe set of symbols we're tracking. Use ConcurrentHashMap's keySet for efficiency.
    private final Set<String> trackedSymbols = ConcurrentHashMap.newKeySet();

    private final LeaderAwareRSocketClient client;
    private final ClusterNode clusterNode;

    public PositionTracker(LeaderAwareRSocketClient client, ClusterNode clusterNode) {
        this.client = client;
        this.clusterNode = clusterNode;
    }

    @Override
    public boolean addSymbol(String symbol) {
        if (symbol == null) return false;
        return trackedSymbols.add(symbol);
    }

    @Override
    public boolean removeSymbol(String symbol) {
        if (symbol == null) return false;
        return trackedSymbols.remove(symbol);
    }

    @Override
    public boolean handlesSymbol(String symbol) {
        if (symbol == null) return false;
        return trackedSymbols.contains(symbol);
    }

    @Override
    public Set<String> handledSymbols() {
        return Set.copyOf(trackedSymbols);
    }

    public Flux<StateSnapshot> getPositions() {
        // The cluster leader coordinates assignments and is not assigned symbols; don't subscribe.
        if (clusterNode.isLeader()) {
            log.debug("Skipping state.stream subscription: this node is the leader");
            return Flux.empty();
        }

        // Flux.defer so retries actually re-resolve the leader via the registry
        // cache instead of riding a dead TCP connection.
        return Flux.defer(() ->
                        client.requestStream(TRADING_STATE_SERVICE, "state.stream", StateSnapshot.class))
                .filter(Objects::nonNull)
                .filter(snapshot -> snapshot.position() != null)
                .filter(snapshot -> snapshot.position().symbol() != null)
                .filter(snapshot -> handlesSymbol(snapshot.position().symbol()))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, STREAM_RECONNECT_DELAY)
                        .doBeforeRetry(sig -> log.warn(
                                "state.stream subscription error, retrying: {}",
                                sig.failure().toString())))
                // Hop off the RSocket event loop. Downstream calls
                // ProductionQuoteGenerator.generateQuote which uses .block()
                // for the reservation roundtrip — Reactor refuses blocking
                // on netty's epoll threads.
                .publishOn(Schedulers.boundedElastic());
    }
}
