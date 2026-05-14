package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.Position;
import edu.yu.marketmaker.model.Quote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link QuoteFreshnessKeeper}. We drive {@code refreshStaleQuotes()}
 * directly instead of waiting on the Flux.interval subscription, which would
 * make the suite slow and flaky.
 *
 * <p>Coverage:
 * <ul>
 *   <li>Quotes with plenty of TTL are skipped.</li>
 *   <li>Quotes within {@code staleThresholdMs} of expiry are refreshed.</li>
 *   <li>Already-expired quotes are refreshed.</li>
 *   <li>Symbols with no quote in the repository are refreshed (fresh assignment safety).</li>
 *   <li>One failing symbol does not abort the per-tick loop.</li>
 *   <li>Position is fetched with the trading-state RSocket route and the result
 *       is fed into {@code generateQuote} verbatim (no MarketMaker indirection
 *       so the per-symbol version tracker isn't polluted by refreshes).</li>
 *   <li>An empty Mono from the position fetch falls back to a synthetic
 *       {@code (symbol, 0, -1L, null)} position.</li>
 * </ul>
 */
class QuoteFreshnessKeeperTest {

    private static final long STALE_THRESHOLD_MS = 15_000L;
    private static final long REFRESH_INTERVAL_MS = 10_000L;
    private static final long FETCH_TIMEOUT_MS = 5_000L;

    private MarketMaker marketMaker;
    private QuoteGenerator quoteGenerator;
    @SuppressWarnings("unchecked")
    private final Repository<String, Quote> quoteRepository = mock(Repository.class);
    private LeaderAwareRSocketClient rsocketClient;
    private QuoteFreshnessKeeper keeper;

    @BeforeEach
    void setUp() {
        marketMaker = mock(MarketMaker.class);
        quoteGenerator = mock(QuoteGenerator.class);
        rsocketClient = mock(LeaderAwareRSocketClient.class);
        reset(quoteRepository);
        keeper = new QuoteFreshnessKeeper(
                marketMaker, quoteGenerator, quoteRepository, rsocketClient,
                REFRESH_INTERVAL_MS, STALE_THRESHOLD_MS, FETCH_TIMEOUT_MS);
    }

    private Quote makeQuote(String symbol, long msUntilExpiry) {
        return new Quote(symbol, 99.95, 10, 100.05, 10,
                UUID.randomUUID(),
                System.currentTimeMillis() + msUntilExpiry);
    }

    @Test
    void freshQuoteIsNotRefreshed() {
        when(marketMaker.assignedSymbols()).thenReturn(Set.of("AAPL"));
        // 25s of life > 15s threshold → fresh
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(makeQuote("AAPL", 25_000L)));

        keeper.refreshStaleQuotes();

        verify(quoteGenerator, never()).generateQuote(any(), any());
        verify(rsocketClient, never()).requestResponse(any(), any(), any(), any());
    }

    @Test
    void nearExpiryQuoteIsRefreshed() {
        when(marketMaker.assignedSymbols()).thenReturn(Set.of("AAPL"));
        // 5s of life < 15s threshold → stale
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(makeQuote("AAPL", 5_000L)));
        Position fetched = new Position("AAPL", 7, 42L, UUID.randomUUID());
        when(rsocketClient.requestResponse("trading-state", "positions.AAPL", "", Position.class))
                .thenReturn(Mono.just(fetched));

        keeper.refreshStaleQuotes();

        verify(quoteGenerator).generateQuote(fetched, null);
    }

    @Test
    void expiredQuoteIsRefreshed() {
        when(marketMaker.assignedSymbols()).thenReturn(Set.of("AAPL"));
        // Already past expiry — keeper must treat as stale.
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(makeQuote("AAPL", -1_000L)));
        Position fetched = new Position("AAPL", -3, 7L, UUID.randomUUID());
        when(rsocketClient.requestResponse("trading-state", "positions.AAPL", "", Position.class))
                .thenReturn(Mono.just(fetched));

        keeper.refreshStaleQuotes();

        verify(quoteGenerator).generateQuote(fetched, null);
    }

    @Test
    void missingQuoteIsRefreshed() {
        // No quote at all (e.g. a fresh assignment whose bootstrap call lost its
        // race with reservation grant) — still refresh so we never sit on an
        // empty quote slot longer than one tick.
        when(marketMaker.assignedSymbols()).thenReturn(Set.of("AAPL"));
        when(quoteRepository.get("AAPL")).thenReturn(Optional.empty());
        Position fetched = new Position("AAPL", 0, 0L, null);
        when(rsocketClient.requestResponse("trading-state", "positions.AAPL", "", Position.class))
                .thenReturn(Mono.just(fetched));

        keeper.refreshStaleQuotes();

        verify(quoteGenerator).generateQuote(fetched, null);
    }

    @Test
    void emptyPositionFallsBackToSyntheticVersionMinusOne() {
        when(marketMaker.assignedSymbols()).thenReturn(Set.of("AAPL"));
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(makeQuote("AAPL", -1_000L)));
        // Trading-state has no record yet — empty Mono is a real case for a
        // brand-new symbol with zero fills. Keeper must fall back to a synthetic
        // position so generateQuote can still run.
        when(rsocketClient.requestResponse("trading-state", "positions.AAPL", "", Position.class))
                .thenReturn(Mono.empty());

        keeper.refreshStaleQuotes();

        ArgumentCaptor<Position> captor = ArgumentCaptor.forClass(Position.class);
        verify(quoteGenerator).generateQuote(captor.capture(), eq(null));
        Position used = captor.getValue();
        assertEquals("AAPL", used.symbol());
        assertEquals(0, used.netQuantity());
        assertEquals(-1L, used.version(),
                "synthetic bootstrap version must be -1 so the first real fill (version >= 0) supersedes it");
    }

    @Test
    void failureOnOneSymbolDoesNotAbortLoop() {
        when(marketMaker.assignedSymbols()).thenReturn(new java.util.LinkedHashSet<>(java.util.List.of("AAPL", "MSFT")));
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(makeQuote("AAPL", -1L)));
        when(quoteRepository.get("MSFT")).thenReturn(Optional.of(makeQuote("MSFT", -1L)));

        // AAPL fetch fails outright.
        when(rsocketClient.requestResponse("trading-state", "positions.AAPL", "", Position.class))
                .thenReturn(Mono.error(new RuntimeException("simulated trading-state outage")));
        // MSFT succeeds.
        Position msftPos = new Position("MSFT", 5, 1L, UUID.randomUUID());
        when(rsocketClient.requestResponse("trading-state", "positions.MSFT", "", Position.class))
                .thenReturn(Mono.just(msftPos));

        keeper.refreshStaleQuotes();

        verify(quoteGenerator, never()).generateQuote(argThat(p -> "AAPL".equals(p.symbol())), any());
        verify(quoteGenerator).generateQuote(msftPos, null);
    }

    @Test
    void noAssignedSymbolsIsANoop() {
        // Leader and any pod still bootstrapping fall down this path. Must not
        // touch trading-state or generateQuote.
        when(marketMaker.assignedSymbols()).thenReturn(Set.of());

        keeper.refreshStaleQuotes();

        verifyNoInteractions(rsocketClient);
        verifyNoInteractions(quoteGenerator);
    }

    @Test
    void generateQuoteThrowingDoesNotAbortLoop() {
        // generateQuote may throw if the reservation service is briefly down.
        // Other symbols on the same tick still need to be processed.
        when(marketMaker.assignedSymbols()).thenReturn(new java.util.LinkedHashSet<>(java.util.List.of("AAPL", "MSFT")));
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(makeQuote("AAPL", -1L)));
        when(quoteRepository.get("MSFT")).thenReturn(Optional.of(makeQuote("MSFT", -1L)));
        Position aaplPos = new Position("AAPL", 0, 0L, null);
        Position msftPos = new Position("MSFT", 0, 0L, null);
        when(rsocketClient.requestResponse("trading-state", "positions.AAPL", "", Position.class))
                .thenReturn(Mono.just(aaplPos));
        when(rsocketClient.requestResponse("trading-state", "positions.MSFT", "", Position.class))
                .thenReturn(Mono.just(msftPos));
        when(quoteGenerator.generateQuote(aaplPos, null))
                .thenThrow(new IllegalStateException("reservation service unavailable"));

        keeper.refreshStaleQuotes();

        verify(quoteGenerator).generateQuote(aaplPos, null);
        verify(quoteGenerator).generateQuote(msftPos, null);
    }
}