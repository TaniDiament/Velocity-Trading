package edu.yu.velocitytrading.marketmaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import edu.yu.velocitytrading.model.Position;
import edu.yu.velocitytrading.model.StateSnapshot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Profile("market-maker-node")
public class MarketMaker implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MarketMaker.class);

    private final SnapshotTracker positionTracker;
    private final QuoteGenerator quoteGenerator;
    private final Map<String, Long> lastProcessedVersionBySymbol = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> forwardedCountBySymbol = new ConcurrentHashMap<>();

    public MarketMaker(SnapshotTracker positionTracker, QuoteGenerator quoteGenerator) {
        this.positionTracker = positionTracker;
        this.quoteGenerator = quoteGenerator;
    }

    private void handlePosition(StateSnapshot snapshot) {
        if (snapshot == null || snapshot.position() == null || snapshot.position().symbol() == null) {
            return;
        }
        if (!positionTracker.handlesSymbol(snapshot.position().symbol()) || !newVersion(snapshot.position())) {
            return;
        }
        quoteGenerator.generateQuote(snapshot.position(), snapshot.fill());
    }

    private boolean newVersion(Position position) {
        long incoming = position.version();
        boolean[] isNew = {false};
        lastProcessedVersionBySymbol.compute(position.symbol(), (k, prev) -> {
            if (prev == null || incoming > prev) {
                isNew[0] = true;
                return incoming;
            }
            return prev;
        });
        return isNew[0];
    }

    /**
     * Entry point for leader-forwarded snapshots arriving over TCP from the
     * cluster leader. Increments the per-symbol forward counter (used by
     * {@link MarketMakerStatusController} to prove routing in tests) and then
     * runs the same processing path as locally-observed snapshots.
     */
    public void handleForwardedSnapshot(StateSnapshot snapshot) {
        if (snapshot == null || snapshot.position() == null || snapshot.position().symbol() == null) {
            return;
        }
        forwardedCountBySymbol
                .computeIfAbsent(snapshot.position().symbol(), k -> new AtomicLong())
                .incrementAndGet();
        handlePosition(snapshot);
    }

    /**
     * @return snapshot of the per-symbol count of forwarded frames this node
     *         has received from the leader. Symbols never forwarded aren't
     *         present. Used for cluster-routing assertions in tests.
     */
    public Map<String, Long> forwardedCounts() {
        Map<String, Long> out = new LinkedHashMap<>();
        forwardedCountBySymbol.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /** @return the symbols currently assigned to this node by the coordinator. */
    public Set<String> assignedSymbols() {
        return positionTracker.handledSymbols();
    }

    public boolean addSymbol(String symbol) {
        return positionTracker.addSymbol(symbol);
    }

    public boolean removeSymbol(String symbol) {
        boolean removed = positionTracker.removeSymbol(symbol);
        if (removed) {
            lastProcessedVersionBySymbol.remove(symbol);
        }
        return removed;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Subscribe once at startup so incoming snapshots are continuously processed.
        // Per-snapshot try/catch so a single failed handlePosition (e.g. a
        // transient reservation timeout) doesn't cancel the whole subscription
        // — Reactor's LambdaSubscriber turns a thrown onNext into a terminal
        // onError, and we need to keep processing subsequent snapshots.
        // PositionTracker.getPositions() also retries forever on upstream
        // errors, so a terminal onError reaching here is unexpected.
        positionTracker.getPositions().subscribe(
                snapshot -> {
                    try {
                        handlePosition(snapshot);
                    } catch (Exception e) {
                        String sym = (snapshot != null && snapshot.position() != null)
                                ? snapshot.position().symbol() : "?";
                        log.warn("error processing snapshot for {}: {}", sym, e.toString());
                    }
                },
                err -> log.error("position subscription terminated", err));
    }
}
