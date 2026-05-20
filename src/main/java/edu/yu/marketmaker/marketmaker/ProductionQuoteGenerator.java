package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

@Component
@Primary
@Profile("production-quote-generator")
public class ProductionQuoteGenerator implements QuoteGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ProductionQuoteGenerator.class);
    private static final String EXPOSURE_RESERVATION_SERVICE = "exposure-reservation";

    // Routes through the ZK-resolved leader and evicts the cached requester
    // on connection failure, so a stale pool that latched onto an unready
    // exposure-reservation pod at MM startup heals on the next call instead
    // of looping on ECONNREFUSED until the JVM restarts.
    private final LeaderAwareRSocketClient rsocketClient;
    private final int defaultQuantity;
    private final double targetSpread;
    private final Repository<String, Quote> quoteRepository;

    /**
     * Optional fault injector — only non-null when the {@code fault-injection}
     * Spring profile is active. See {@link FaultInjector} for the safety
     * gates. In all production deployments this is {@code null} and
     * {@link #maybeTriggerError10Crash(String)} is a no-op.
     */
    private final FaultInjector faultInjector;

    public ProductionQuoteGenerator(
            LeaderAwareRSocketClient rsocketClient,
            Repository<String, Quote> quoteRepository,
            @Value("${marketmaker.default-quote-quantity:10}") int defaultQuantity,
            @Value("${marketmaker.target-spread:0.10}") double targetSpread,
            ObjectProvider<FaultInjector> faultInjectorProvider
    ) {
        this.rsocketClient = rsocketClient;
        this.defaultQuantity = defaultQuantity;
        this.targetSpread = targetSpread;
        this.quoteRepository = quoteRepository;
        // ObjectProvider is Spring's documented pattern for optional
        // dependencies: returns null when the bean doesn't exist (e.g.
        // the fault-injection profile isn't active). Mirrors what
        // @Autowired(required=false) is supposed to do but works reliably
        // alongside @Value-annotated constructor parameters.
        this.faultInjector = faultInjectorProvider.getIfAvailable();
        logger.info("ProductionQuoteGenerator initialised: faultInjector={}",
                this.faultInjector == null ? "<not wired>" : "ARMED-CAPABLE");
    }

    /**
     * Generates a quote based on the current position and last fill.
     * Only thread safe assuming one market maker instance per symbol, 
     * as the quote repository is shared and not synchronized. 
     * In a real implementation, we would need to ensure thread safety 
     * at the repository level or use a more sophisticated state management approach.
     *
     * @param position The current position.
     * @param lastFill The last fill.
     * @return The generated quote.
     */
    @Override
    public Quote generateQuote(Position position, Fill lastFill) {
        String symbol = lastFill != null ? lastFill.symbol() : position.symbol();

        // Fault-injection hook for error case 10. Only fires when (a) the
        // fault-injection profile is active (so faultInjector is non-null)
        // AND (b) something has explicitly armed the injector for this
        // symbol. In all other cases this returns immediately.
        maybeTriggerError10Crash(symbol);

        Quote current = quoteRepository.get(symbol).orElse(null);
        // Error-case 11 (full-system restart): the quote IMap reloads
        // pre-restart quotes whose 30s TTL has long since lapsed during the
        // multi-minute kubectl rollout. The error-case-11 docs say the
        // exchange "expires old quotes" on restart, but no code prunes them.
        // Treat an expired survivor as nonexistent here so the bootstrap
        // regen falls into the cold-start branch (default quantities,
        // reference price of 100) instead of reusing the drifted/depleted
        // values it would otherwise carry forward.
        if (current != null && current.expiresAt() <= System.currentTimeMillis()) {
            current = null;
        }

        double referencePrice = current != null ? midPrice(current) : (lastFill != null ? lastFill.price() : 100.0);
        double halfSpread = targetSpread / 2.0;
        int bidQuantity = current != null ? Math.max(0, current.bidQuantity()) : Math.max(1, defaultQuantity);
        int askQuantity = current != null ? Math.max(0, current.askQuantity()) : Math.max(1, defaultQuantity);

        // Simple inventory-aware skew based on the last fill side.
        if (lastFill != null) {
            switch (lastFill.side()) {
                case SELL -> { // raise price
                    referencePrice += 0.01 * lastFill.quantity();
                    askQuantity += 2;
                    bidQuantity = Math.max(0, bidQuantity - 1);
                }
                case BUY -> { // lower price
                    referencePrice -= 0.01 * lastFill.quantity();
                    askQuantity = Math.max(0, askQuantity - 1);
                    bidQuantity += 2;
                }
            }
        }

        // --- NEW LOGIC: Enforce Individual Position Limits (±100) ---
        int maxAllowedBid = Math.max(0, 100 - position.netQuantity());
        bidQuantity = Math.min(bidQuantity, maxAllowedBid);

        int maxAllowedAsk = Math.max(0, 100 + position.netQuantity());
        askQuantity = Math.min(askQuantity, maxAllowedAsk);
        // -----------------------------------------------------------

        double bidPrice = Math.max(0.01, referencePrice - halfSpread);
        double askPrice = Math.max(bidPrice, referencePrice + halfSpread);

        Quote proposed = new Quote(
                symbol,
                bidPrice,
                bidQuantity,
                askPrice,
                askQuantity,
                UUID.randomUUID(),
                System.currentTimeMillis() + 30_000
        );

        ReservationResponse reservation = rsocketClient
                .requestResponse(EXPOSURE_RESERVATION_SERVICE, "reservations", proposed,
                        ReservationResponse.class)
                .block();

        if (reservation == null) {
            throw new IllegalStateException("Exposure reservation service returned no response");
        }

        // Fault-injection hook for error case 5. Same safety gates as
        // maybeTriggerError10Crash; fires only when the fault-injection
        // profile is active AND the operator has explicitly armed the
        // post-reservation crash for this symbol. The reservation has
        // already been recorded server-side at this point, so a halt here
        // leaves an orphan reservation with no corresponding active quote.
        maybeTriggerError5Crash(symbol);

        Quote reservedQuote = new Quote(
                proposed.symbol(),
                proposed.bidPrice(),
                reservation.grantedBidQuantity(),
                proposed.askPrice(),
                reservation.grantedAskQuantity(),
                proposed.quoteId(),
                proposed.expiresAt()
        );

        quoteRepository.put(reservedQuote);
        logger.info(
                "Generated reserved quote: symbol={}, bidQty={}, askQty={}, reservationStatus={}",
                reservedQuote.symbol(),
                reservedQuote.bidQuantity(),
                reservedQuote.askQuantity(),
                reservation.status()
        );

        return reservedQuote;
    }

    private double midPrice(Quote quote) {
        return (quote.bidPrice() + quote.askPrice()) / 2.0;
    }

    /**
     * If the fault injector is wired in and currently armed for {@code symbol},
     * faithfully reproduce error case 10's failure sequence:
     *   1. Explicitly release the existing reservation for the symbol via
     *      RSocket (the documented "release old reservation" step that
     *      production code normally skips because the reservation service
     *      releases atomically on the next createReservation).
     *   2. Hard-halt the JVM with {@link Runtime#halt(int)} — bypasses
     *      shutdown hooks so the crash mirrors a process kill, not a clean
     *      stop. The next reservation request is never sent, leaving the
     *      active exchange quote without a backing reservation until its
     *      TTL expires.
     *
     * <p>This method is a no-op unless the {@code fault-injection} profile
     * is active and the injector has been armed via
     * {@link FaultInjectionController}.
     */
    private void maybeTriggerError10Crash(String symbol) {
        if (faultInjector == null) {
            return;
        }
        String armed = faultInjector.currentlyArmedSymbol();
        if (armed != null) {
            // Loud log whenever the injector is armed and generateQuote runs.
            // Lets us tell apart "armed but generateQuote never fired for the
            // armed symbol" (no log) from "fired but symbol mismatched" (log
            // shows mismatch) when debugging error-case-10 test failures.
            logger.warn("[FAULT-INJECTION] generateQuote called for symbol={} (armed for {})",
                    symbol, armed);
        }
        if (!faultInjector.consumeIfArmed(symbol)) {
            return;
        }
        logger.error("[FAULT-INJECTION] error case 10: releasing reservation for {} then halting JVM", symbol);
        try {
            FreedCapacityResponse freed = rsocketClient
                    .requestResponse(EXPOSURE_RESERVATION_SERVICE,
                            "reservations." + symbol + ".release", "",
                            FreedCapacityResponse.class)
                    .block(Duration.ofSeconds(5));
            logger.error("[FAULT-INJECTION] release returned freed={} for symbol={}", freed, symbol);
        } catch (Exception e) {
            // Best-effort: even if the release fails we still halt so the test
            // observes a crashed MM. The recovery path (TTL expiry + restart)
            // must hold regardless.
            logger.error("[FAULT-INJECTION] release call failed for {}: {}", symbol, e.toString());
        }
        // 137 == 128 + SIGKILL(9), the conventional "killed" exit code.
        // Using halt() (not exit()) skips shutdown hooks so this looks like
        // an abrupt process death to the rest of the system.
        Runtime.getRuntime().halt(137);
    }

    /**
     * If the fault injector is wired in and currently armed for {@code symbol}
     * on the post-reservation slot, halt the JVM immediately. The reservation
     * for {@code symbol} has already been granted server-side at the call
     * site (right after the reservations RSocket response returned), and we
     * intentionally do NOT release it — the orphan reservation with no
     * corresponding active quote is exactly what
     * {@code ClusterError5MMCrashAfterReservationTest} asserts on.
     *
     * <p>This method is a no-op unless the {@code fault-injection} profile is
     * active and the injector has been armed via
     * {@link FaultInjectionController#armPostReservationCrash(String)}.
     */
    private void maybeTriggerError5Crash(String symbol) {
        if (faultInjector == null) {
            return;
        }
        String armed = faultInjector.currentlyArmedPostReservationSymbol();
        if (armed != null) {
            logger.warn("[FAULT-INJECTION] post-reservation hook reached for symbol={} (armed for {})",
                    symbol, armed);
        }
        if (!faultInjector.consumeIfPostReservationArmed(symbol)) {
            return;
        }
        logger.error("[FAULT-INJECTION] error case 5: halting JVM after reservation grant for {} "
                + "(reservation intentionally leaked, no quote will be published)", symbol);
        Runtime.getRuntime().halt(137);
    }
}
