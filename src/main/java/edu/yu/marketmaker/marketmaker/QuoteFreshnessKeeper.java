package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.Position;
import edu.yu.marketmaker.model.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import jakarta.annotation.PreDestroy;

import java.time.Duration;

/**
 * Periodic safety-net that re-publishes near-expiry quotes for symbols
 * assigned to this market-maker.
 *
 * <p>In normal operation {@link MarketMaker} regenerates a quote in response
 * to every fill (via {@link PositionTracker}'s {@code state.stream}
 * subscription). During a quiet period — most commonly the multi-minute
 * window between MM startup and the cluster reaching full convergence — no
 * fills arrive, the 30-second-TTL bootstrap quote expires, and
 * {@link edu.yu.marketmaker.exchange.FillOrderDispatcher} starts rejecting
 * every incoming order with "Quote ... is expired". That deadlocks the
 * end-to-end pipeline: no orders accepted, so no fills, so the MM never
 * regenerates, so the quote stays expired.
 *
 * <p>This component breaks the deadlock by ticking on a fixed interval,
 * fetching the current {@link Position} for each assigned symbol from the
 * trading-state leader (the same path
 * {@link edu.yu.marketmaker.cluster.AssignmentListener} uses on a fresh
 * assignment), and calling {@link QuoteGenerator#generateQuote} directly
 * when the existing quote has less than {@code quote-stale-threshold-ms}
 * left of TTL. It bypasses {@link MarketMaker#handlePosition} on purpose:
 * the version-tracking there is intended for fill-driven updates and would
 * suppress a same-version refresh.
 *
 * <p>The leader is skipped implicitly — it has no assigned symbols, so
 * {@link MarketMaker#assignedSymbols()} returns empty and the per-tick loop
 * does nothing.
 */
@Component
@Profile("market-maker-node")
public class QuoteFreshnessKeeper implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(QuoteFreshnessKeeper.class);
    private static final String TRADING_STATE_SERVICE = "trading-state";

    private final MarketMaker marketMaker;
    private final QuoteGenerator quoteGenerator;
    private final Repository<String, Quote> quoteRepository;
    private final LeaderAwareRSocketClient rsocketClient;
    private final Duration refreshInterval;
    private final long staleThresholdMs;
    private final Duration positionFetchTimeout;

    private volatile Disposable subscription;

    public QuoteFreshnessKeeper(
            MarketMaker marketMaker,
            QuoteGenerator quoteGenerator,
            Repository<String, Quote> quoteRepository,
            LeaderAwareRSocketClient rsocketClient,
            @Value("${marketmaker.quote-refresh-interval-ms:10000}") long refreshIntervalMs,
            @Value("${marketmaker.quote-stale-threshold-ms:15000}") long staleThresholdMs,
            @Value("${marketmaker.quote-refresh-fetch-timeout-ms:5000}") long fetchTimeoutMs) {
        this.marketMaker = marketMaker;
        this.quoteGenerator = quoteGenerator;
        this.quoteRepository = quoteRepository;
        this.rsocketClient = rsocketClient;
        this.refreshInterval = Duration.ofMillis(refreshIntervalMs);
        this.staleThresholdMs = staleThresholdMs;
        this.positionFetchTimeout = Duration.ofMillis(fetchTimeoutMs);
        log.info("QuoteFreshnessKeeper configured: refreshInterval={}, staleThreshold={}ms",
                this.refreshInterval, this.staleThresholdMs);
    }

    @Override
    public void run(ApplicationArguments args) {
        // Wait one full interval before the first tick so AssignmentListener
        // has time to bootstrap quotes for fresh assignments — refreshing a
        // quote we're about to write is wasted work.
        this.subscription = Flux.interval(refreshInterval, refreshInterval)
                .publishOn(Schedulers.boundedElastic())
                .subscribe(
                        tick -> refreshStaleQuotes(),
                        err -> log.error("quote-freshness keeper terminated", err));
    }

    @PreDestroy
    void shutdown() {
        Disposable s = this.subscription;
        if (s != null && !s.isDisposed()) {
            s.dispose();
        }
    }

    /**
     * Walk every currently-assigned symbol and refresh its quote if it is
     * within {@code staleThresholdMs} of expiry (or already expired, or
     * missing entirely). Per-symbol failures don't abort the loop.
     */
    void refreshStaleQuotes() {
        long now = System.currentTimeMillis();
        for (String symbol : marketMaker.assignedSymbols()) {
            try {
                if (!isStale(symbol, now)) {
                    continue;
                }
                Position pos = fetchPosition(symbol);
                if (pos == null) {
                    log.warn("could not fetch position to refresh quote for {}", symbol);
                    continue;
                }
                quoteGenerator.generateQuote(pos, null);
                log.info("refreshed near-expiry quote for {} (position={})", symbol, pos.netQuantity());
            } catch (Exception e) {
                log.warn("failed to refresh quote for {}: {}", symbol, e.toString());
            }
        }
    }

    private boolean isStale(String symbol, long now) {
        Quote existing = quoteRepository.get(symbol).orElse(null);
        if (existing == null) return true;
        return existing.expiresAt() - now < staleThresholdMs;
    }

    private Position fetchPosition(String symbol) {
        // version=-1 mirrors AssignmentListener.bootstrapQuoteForNewlyAssigned:
        // the first real fill-driven update (version >= 0) supersedes it. We
        // intentionally don't feed this snapshot through MarketMaker so the
        // per-symbol version tracker in MarketMaker isn't polluted by
        // refresh-driven calls — the version tracker is fill-ordering state
        // and refreshes shouldn't bump it.
        return rsocketClient.requestResponse(TRADING_STATE_SERVICE,
                        "positions." + symbol, "", Position.class)
                .defaultIfEmpty(new Position(symbol, 0, -1L, null))
                .block(positionFetchTimeout);
    }
}