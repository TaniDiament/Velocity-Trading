package edu.yu.marketmaker.marketmaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only fault injector for the two MM-side crash scenarios in error case
 * 10 (quote-replacement cycle, after releasing the old reservation but before
 * acquiring the new one) and error case 5 (after a fresh reservation is
 * granted but before the resulting quote is written to the quote repository).
 *
 * <p>This bean only exists when the {@code fault-injection} Spring profile is
 * active. {@link ProductionQuoteGenerator} consumes it via optional injection;
 * when the profile is absent (the default) the field is {@code null} and the
 * production code path is unaffected.
 *
 * <p>The two scenarios are armed independently — each maintains its own
 * single-shot armed-symbol slot. Arming one does not affect the other, so an
 * arm/consume mismatch in one scenario can't accidentally fire the other.
 *
 * <p>Two independent safety gates therefore stand between this code and a
 * production deployment:
 * <ol>
 *   <li>The {@code fault-injection} profile must be in
 *       {@code SPRING_PROFILES_ACTIVE}.</li>
 *   <li>An operator must POST to one of the arm endpoints with a specific symbol.</li>
 * </ol>
 */
@Component
@Profile("fault-injection")
public class FaultInjector {

    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);

    private final AtomicReference<String> armedSymbol = new AtomicReference<>(null);
    private final AtomicReference<String> armedPostReservationSymbol = new AtomicReference<>(null);

    /**
     * Arm the injector to crash the next time the quote generator processes
     * a replacement cycle for {@code symbol}. Overwrites any prior armed
     * symbol; passing {@code null} disarms.
     */
    public void armQuoteReplaceCrash(String symbol) {
        String previous = armedSymbol.getAndSet(symbol);
        if (previous != null) {
            log.warn("[FAULT-INJECTION] re-arm: overwriting previously armed symbol {} with {}",
                    previous, symbol);
        } else {
            log.warn("[FAULT-INJECTION] armed: will crash on next quote-replace cycle for symbol={}",
                    symbol);
        }
    }

    /** @return the currently armed quote-replace symbol, or {@code null} if disarmed. */
    public String currentlyArmedSymbol() {
        return armedSymbol.get();
    }

    /**
     * Consume the armed flag if it matches {@code symbol}.
     *
     * <p>Returns {@code true} (and clears the armed state) only when the
     * injector was armed for exactly this symbol. After a successful consume
     * the caller is expected to release the old reservation and halt the JVM
     * — see {@link ProductionQuoteGenerator}.
     */
    public synchronized boolean consumeIfArmed(String symbol) {
        if (symbol == null) {
            return false;
        }
        // Value comparison, not reference: AtomicReference.compareAndSet
        // uses == under the hood, which fails for equal-but-distinct String
        // instances (the symbol from HTTP query-string vs. the one from a
        // deserialized Position payload). equals() is what we actually want.
        if (symbol.equals(armedSymbol.get())) {
            armedSymbol.set(null);
            return true;
        }
        return false;
    }

    /**
     * Arm the injector to crash the next time the quote generator receives a
     * fresh reservation grant for {@code symbol}, before the resulting quote
     * is written to the quote repository. Overwrites any prior armed symbol
     * for this scenario; passing {@code null} disarms.
     *
     * <p>This is the error-case-5 hook: the reservation has already been
     * granted in {@code ExposureReservationService.reservations}, so the
     * crash leaves an orphan reservation with no corresponding active quote
     * — exactly the leak the test asserts on.
     */
    public void armPostReservationCrash(String symbol) {
        String previous = armedPostReservationSymbol.getAndSet(symbol);
        if (previous != null) {
            log.warn("[FAULT-INJECTION] re-arm (post-reservation): overwriting previously armed symbol {} with {}",
                    previous, symbol);
        } else {
            log.warn("[FAULT-INJECTION] armed: will crash on next post-reservation step for symbol={}",
                    symbol);
        }
    }

    /** @return the currently armed post-reservation symbol, or {@code null} if disarmed. */
    public String currentlyArmedPostReservationSymbol() {
        return armedPostReservationSymbol.get();
    }

    /**
     * Consume the post-reservation armed flag if it matches {@code symbol}.
     *
     * <p>Returns {@code true} (and clears the armed state) only when the
     * injector was armed for exactly this symbol. After a successful consume
     * the caller is expected to halt the JVM without writing the new quote —
     * see {@link ProductionQuoteGenerator#maybeTriggerError5Crash(String)}.
     */
    public synchronized boolean consumeIfPostReservationArmed(String symbol) {
        if (symbol == null) {
            return false;
        }
        if (symbol.equals(armedPostReservationSymbol.get())) {
            armedPostReservationSymbol.set(null);
            return true;
        }
        return false;
    }
}