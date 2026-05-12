package edu.yu.marketmaker.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.marketmaker.MarketMaker;
import edu.yu.marketmaker.model.Position;
import edu.yu.marketmaker.model.StateSnapshot;
import jakarta.annotation.PreDestroy;
import reactor.core.scheduler.Schedulers;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Worker-side glue between ZK and the local {@link MarketMaker}.
 *
 * Each JVM watches its own assignment znode at
 * {@code /marketmaker/assignments/<node-id>} and translates every change
 * into add/remove calls on the local {@link MarketMaker}. Tracks accepted
 * symbols so updates compute a precise diff (no spurious add/remove pairs).
 *
 * On the leader, the znode is set to an empty list, so this component
 * naturally drains the local market-maker — no "I'm the leader" branch
 * needed in MarketMaker itself.
 */
@Component
@Profile("market-maker-node")
public class AssignmentListener implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AssignmentListener.class);
    private static final TypeReference<List<String>> LIST_OF_STRING = new TypeReference<>() {};
    private static final String TRADING_STATE_SERVICE = "trading-state";

    private final CuratorFramework curator;
    private final ZkPaths paths;
    private final ClusterNode clusterNode;
    private final MarketMaker marketMaker;
    private final LeaderAwareRSocketClient rsocketClient;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Set<String> currentAssigned = Collections.synchronizedSet(new HashSet<>());
    private CuratorCache cache;

    public AssignmentListener(CuratorFramework curator,
                              ZkPaths paths,
                              ClusterNode clusterNode,
                              MarketMaker marketMaker,
                              LeaderAwareRSocketClient rsocketClient) {
        this.curator = curator;
        this.paths = paths;
        this.clusterNode = clusterNode;
        this.marketMaker = marketMaker;
        this.rsocketClient = rsocketClient;
    }

    /**
     * Register a {@link CuratorCache} listener on the per-node assignment
     * znode. The cache fires an event for the current value on start, so
     * no separate initial read is needed (and doing one would race the
     * listener).
     */
    @Override
    public void run(ApplicationArguments args) {
        String myPath = paths.assignmentFor(clusterNode.getNodeId());
        this.cache = CuratorCache.build(curator, myPath);
        this.cache.listenable().addListener((type, oldData, data) -> {
            byte[] bytes = (data != null) ? data.getData() : null;
            applyDesired(parse(bytes));
        });
        this.cache.start();
    }

    /**
     * Decode the assignment znode's JSON payload. Null/empty/errors become
     * an empty list (logged) so a malformed znode can't wedge the worker.
     */
    private List<String> parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return List.of();
        }
        try {
            return mapper.readValue(bytes, LIST_OF_STRING);
        } catch (Exception e) {
            log.error("failed to parse assignment payload; treating as empty", e);
            return List.of();
        }
    }

    /**
     * Reconcile the local symbol set against the desired list: remove any
     * symbol no longer assigned, add any newly assigned. Both defer to the
     * {@link MarketMaker} API so this class never touches market-making
     * state directly.
     *
     * Synchronized so overlapping ZK callbacks can't interleave their diffs.
     */
    private synchronized void applyDesired(List<String> desiredList) {
        Set<String> desired = new TreeSet<>(desiredList);
        Set<String> toRemove = new TreeSet<>(currentAssigned);
        toRemove.removeAll(desired);
        Set<String> toAdd = new TreeSet<>(desired);
        toAdd.removeAll(currentAssigned);

        for (String s : toRemove) {
            if (marketMaker.removeSymbol(s)) {
                currentAssigned.remove(s);
                log.info("released symbol {}", s);
            }
        }
        for (String s : toAdd) {
            if (marketMaker.addSymbol(s)) {
                currentAssigned.add(s);
                log.info("accepted symbol {}", s);
                bootstrapQuoteForNewlyAssigned(s);
            }
        }
    }

    /**
     * On a fresh assignment, fetch the current position for {@code symbol}
     * from the trading-state leader and feed a synthetic snapshot through
     * the {@link MarketMaker} so it publishes an opening quote immediately —
     * without waiting for a fill it could not otherwise produce.
     *
     * <p>Why this matters: the {@code state.stream} RSocket subscriptions
     * driving {@link MarketMaker#handlePosition} (in {@code PositionTracker})
     * and {@link MarketMaker#handleForwardedSnapshot} (via
     * {@code LeaderForwarder}) emit an initial snapshot of every existing
     * position <em>once, at subscribe time</em>, then only on fills. After
     * HA failover a surviving worker is given a new symbol but its existing
     * subscription doesn't replay, and no fill can land against an expired
     * quote — chicken-and-egg. This method breaks the cycle by issuing an
     * RSocket request-response against {@code positions.<symbol>} on the
     * trading-state leader and feeding the returned position (or a zero
     * position when no fills have ever happened) into the same path a
     * leader-forwarded snapshot would take.
     */
    private void bootstrapQuoteForNewlyAssigned(String symbol) {
        rsocketClient.requestResponse(TRADING_STATE_SERVICE,
                        "positions." + symbol, "", Position.class)
                // version=-1 so the first real position update (which starts
                // at version=0) beats the synthetic in MarketMaker.newVersion.
                .defaultIfEmpty(new Position(symbol, 0, -1L, null))
                // Hop off the RSocket event loop before invoking the MM:
                // handleForwardedSnapshot eventually calls
                // ProductionQuoteGenerator.generateQuote, which .block()s on
                // the reservation RSocket call. Reactor refuses .block() on
                // netty epoll threads.
                .publishOn(Schedulers.boundedElastic())
                .subscribe(
                        position -> {
                            try {
                                marketMaker.handleForwardedSnapshot(new StateSnapshot(position, null));
                            } catch (Exception e) {
                                log.warn("bootstrap quote for newly-assigned {} failed: {}",
                                        symbol, e.toString());
                            }
                        },
                        err -> log.warn("could not fetch position to bootstrap newly-assigned {}: {}",
                                symbol, err.toString()));
    }

    /** Shutdown hook: close the {@link CuratorCache}. */
    @PreDestroy
    public void shutdown() {
        if (cache != null) {
            cache.close();
        }
    }
}
