package edu.yu.marketmaker.marketmaker;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Test-only REST surface for {@link FaultInjector}. Only registered when the
 * {@code fault-injection} Spring profile is active.
 *
 * <p>Used by the error-case 10 integration tests
 * ({@code LocalError10MMCrashDuringQuoteReplaceTest} /
 * {@code ClusterError10MMCrashDuringQuoteReplaceTest}) for the quote-replace
 * crash, and by {@code ClusterError5MMCrashAfterReservationTest} for the
 * post-reservation crash. No production code path arms the injector.
 */
@RestController
@RequestMapping("/test/fault-injection")
@Profile("fault-injection")
public class FaultInjectionController {

    private final FaultInjector injector;

    public FaultInjectionController(FaultInjector injector) {
        this.injector = injector;
    }

    /**
     * Arm the injector to crash this market-maker the next time it processes
     * a quote replacement cycle for {@code symbol}. The crash releases the
     * old reservation first (matching error case 10's documented sequence)
     * and then hard-halts the JVM via {@link Runtime#halt(int)}.
     *
     * <p>Idempotent: re-arming overwrites any prior armed symbol.
     */
    @PostMapping("/arm-quote-replace-crash")
    public ResponseEntity<ArmedStatus> armQuoteReplaceCrash(@RequestParam("symbol") String symbol) {
        injector.armQuoteReplaceCrash(symbol);
        return ResponseEntity.ok(new ArmedStatus(symbol));
    }

    /** Inspect the currently armed quote-replace symbol, if any. */
    @GetMapping("/status")
    public ResponseEntity<ArmedStatus> status() {
        return ResponseEntity.ok(new ArmedStatus(injector.currentlyArmedSymbol()));
    }

    /**
     * Arm the injector to crash this market-maker after the next reservation
     * grant for {@code symbol} but before the resulting quote is written to
     * the local quote repository. Hard-halts the JVM via
     * {@link Runtime#halt(int)} without releasing the just-granted
     * reservation — that's the orphan the error-case-5 test asserts on.
     *
     * <p>Idempotent: re-arming overwrites any prior armed symbol for this
     * scenario. Independent of {@code /arm-quote-replace-crash}.
     */
    @PostMapping("/arm-post-reservation-crash")
    public ResponseEntity<ArmedStatus> armPostReservationCrash(@RequestParam("symbol") String symbol) {
        injector.armPostReservationCrash(symbol);
        return ResponseEntity.ok(new ArmedStatus(symbol));
    }

    /** Inspect the currently armed post-reservation symbol, if any. */
    @GetMapping("/post-reservation-status")
    public ResponseEntity<ArmedStatus> postReservationStatus() {
        return ResponseEntity.ok(new ArmedStatus(injector.currentlyArmedPostReservationSymbol()));
    }

    public record ArmedStatus(String armedSymbol) {}
}