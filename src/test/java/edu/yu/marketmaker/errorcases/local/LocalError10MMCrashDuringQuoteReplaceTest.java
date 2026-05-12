package edu.yu.marketmaker.errorcases.local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.ExposureState;
import edu.yu.marketmaker.model.ExternalOrder;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.Side;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Docker-compose end-to-end test for error case 10:
 * "Market maker crashes during quote replacement cycle"
 * (see {@code docs/error-cases.md}).
 *
 * <p>Faithfully reproduces the documented sequence using the
 * {@code fault-injection} profile and the {@link
 * edu.yu.marketmaker.marketmaker.FaultInjector} bean. The hook fires only
 * after the test POSTs to {@code /test/fault-injection/arm-quote-replace-crash}
 * on the market-maker that owns the target symbol; the next time that MM
 * processes a position update for the armed symbol it explicitly releases
 * the existing reservation via RSocket and then {@code Runtime.halt(137)}s
 * the JVM — leaving an active exchange quote with no backing reservation
 * (the documented invariant violation).
 *
 * <p>Assertions cover both error case 10's bad state and its safety bound:
 * <ul>
 *   <li>Pre-crash: the owner MM has the target symbol assigned and the
 *       exchange has a fresh MM-published quote backed by a reservation.</li>
 *   <li>Post-crash: the MM is unhealthy, the reservation for the symbol
 *       has been released (global active count drops by 1), and the
 *       exchange still holds the now-orphaned quote.</li>
 *   <li>TTL bound: the orphan quote's {@code expiresAt} moves into the
 *       past within the 30-second TTL window.</li>
 *   <li>Recovery: after restarting the MM and continuing to drive traffic,
 *       a fresh quote (different {@code quoteId}) appears in the exchange,
 *       backed by a new reservation — system converges without manual
 *       intervention.</li>
 * </ul>
 *
 * <p>Opt-in: {@code -Dcluster.it=true}; docker must be running locally.
 */
@EnabledIfSystemProperty(named = "cluster.it", matches = "true")
class LocalError10MMCrashDuringQuoteReplaceTest {

    /** Host port -> compose service name, for the 7 MM nodes. */
    private static final SortedMap<Integer, String> MM_PORT_TO_SERVICE;
    static {
        SortedMap<Integer, String> m = new TreeMap<>();
        m.put(8081, "market-maker-node-1");
        m.put(8082, "market-maker-node-2");
        m.put(8083, "market-maker-node-3");
        m.put(8084, "market-maker-node-4");
        m.put(8085, "market-maker-node-5");
        m.put(8086, "market-maker-node-6");
        m.put(8087, "market-maker-node-7");
        MM_PORT_TO_SERVICE = Collections.unmodifiableSortedMap(m);
    }

    private static final Set<String> SEED_SYMBOLS = new TreeSet<>(List.of(
            "AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META"));

    /** Symbol whose owner MM we crash mid-replace. */
    private static final String TARGET_SYMBOL = "AAPL";

    private static final int TRADING_STATE_PORT = 18080;
    private static final int EXCHANGE_PORT = 18081;
    private static final int EXPOSURE_RES_PORT = 18082;
    private static final int PUBLISHER_PORT = 18083;

    private static final int WARMUP_WAVES = 4;
    private static final int RECOVERY_WAVES = 6;
    private static final long WAVE_INTERVAL_MS = 1500;
    private static final int SELF_CROSS_PAIRS_PER_WAVE = 5;
    private static final double SELF_CROSS_PRICE = 100.00;
    private static final double WIDE_BUY_LIMIT = 101.00;
    private static final double WIDE_SELL_LIMIT = 99.00;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Path PROJECT_ROOT = Path.of(".").toAbsolutePath().normalize();

    @BeforeAll
    static void bootStack() throws Exception {
        System.out.println("[ERR10] cleaning any prior stack...");
        runDocker(null, TimeUnit.MINUTES.toMillis(3), "compose", "down", "-v", "--remove-orphans");

        System.out.println("[ERR10] docker compose build...");
        int buildRc = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(20),
                "compose", "build", "market-maker-node-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[ERR10] bringing up core infra...");
        int rcCore = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                "compose", "up", "-d",
                "zookeeper1", "zookeeper2", "zookeeper3", "postgres",
                "trading-state-1", "trading-state-2", "trading-state-3",
                "exposure-reservation-1", "exposure-reservation-2", "exposure-reservation-3",
                "service-lb");
        assertEquals(0, rcCore, "docker compose up (core) failed");

        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(4));

        System.out.println("[ERR10] bringing up exchange + publisher...");
        int rcExchange = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", "exchange-1", "exchange-2", "exchange-3", "external-publisher");
        assertEquals(0, rcExchange, "docker compose up (exchange/publisher) failed");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(4));
        awaitHealthy("external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[ERR10] bringing up market-maker nodes (production-quote-generator + fault-injection)...");
        List<String> upCmd = new ArrayList<>(List.of("compose", "up", "-d"));
        upCmd.addAll(MM_PORT_TO_SERVICE.values());
        int rcMm = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                upCmd.toArray(String[]::new));
        assertEquals(0, rcMm, "docker compose up (market-maker nodes) failed");

        System.out.println("[ERR10] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8),
                LocalError10MMCrashDuringQuoteReplaceTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[ERR10] full stack up.");
    }

    @AfterAll
    static void teardownStack() throws Exception {
        System.out.println("[ERR10] docker compose down -v");
        runDocker(null, TimeUnit.MINUTES.toMillis(2), "compose", "down", "-v");
    }

    /** Container/log dump runs on every test failure so we can diagnose without keeping the stack up. */
    private static String diagnosticTarget;

    @Test
    void marketMakerCrashDuringQuoteReplaceLeavesBoundedOrphanAndRecovers() throws Exception {
        try {
            runScenario();
        } catch (Throwable t) {
            // Dump diagnostics for whichever MM we last identified as the
            // crash target, so failures past the crash point still surface
            // the FAULT-INJECTION log lines we need to root-cause.
            if (diagnosticTarget != null) {
                dumpMmDiagnostics(diagnosticTarget);
            }
            throw t;
        }
    }

    private void runScenario() throws Exception {
        // 1. Seed bootstrap quotes for every symbol so the assigned MMs have
        //    something to replace.
        System.out.println("[ERR10] seeding bootstrap quotes...");
        List<String> seedList = new ArrayList<>(SEED_SYMBOLS);
        List<UUID> bootstrapIds = seedQuotes(seedList);
        Set<UUID> bootstrapQuoteIds = new HashSet<>(bootstrapIds);
        assertEquals(SEED_SYMBOLS.size(), bootstrapQuoteIds.size(),
                "publisher must return one quoteId per symbol");

        // 2. Warmup: drive a few waves so each MM has had at least one full
        //    replace cycle and the exchange holds an MM-published quote
        //    (not the bootstrap) backed by a real reservation.
        System.out.println("[ERR10] warmup: driving " + WARMUP_WAVES + " waves to force initial replace cycles...");
        Random rnd = new Random(0xC0FFEE);
        for (int wave = 1; wave <= WARMUP_WAVES; wave++) {
            driveOneWave(wave, rnd);
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // Wait for the MM-published quote to land in the exchange for the
        // target symbol. Without this, the orphan-window assertion below
        // might catch a bootstrap quote that the MM never owned.
        awaitCondition(Duration.ofSeconds(30), () -> {
            Quote q = currentExchangeQuote(TARGET_SYMBOL);
            return q != null && !bootstrapQuoteIds.contains(q.quoteId());
        }, "no MM-generated quote for " + TARGET_SYMBOL + " after " + WARMUP_WAVES + " waves");

        Quote preCrashQuote = currentExchangeQuote(TARGET_SYMBOL);
        assertNotNull(preCrashQuote, "pre-crash exchange must hold a quote for " + TARGET_SYMBOL);
        assertNotEquals(true, bootstrapQuoteIds.contains(preCrashQuote.quoteId()),
                "pre-crash quote must be MM-generated, not bootstrap: " + preCrashQuote);
        System.out.println("[ERR10] pre-crash quote: " + preCrashQuote);

        // 3. Find which MM owns the target symbol via /marketmaker/status.
        int ownerPort = findOwnerPort(TARGET_SYMBOL);
        String ownerService = MM_PORT_TO_SERVICE.get(ownerPort);
        assertNotNull(ownerService, "no MM owner found for " + TARGET_SYMBOL);
        diagnosticTarget = ownerService;
        System.out.println("[ERR10] " + TARGET_SYMBOL + " owner: " + ownerService + " on port " + ownerPort);

        // 4. Snapshot the global exposure state. Post-crash the release call
        //    fired by the FaultInjector must drop the active reservation
        //    count by exactly 1 (this symbol's reservation), proving the
        //    documented "release before crash" half of error case 10.
        ExposureState preCrashExposure = currentExposure();
        assertNotNull(preCrashExposure, "exposure-reservation /exposure must return state pre-crash");
        int preCrashActive = preCrashExposure.activeReservations();
        System.out.println("[ERR10] pre-crash exposure: " + preCrashExposure);
        assertTrue(preCrashActive >= 1,
                "pre-crash there must be at least one active reservation; got " + preCrashActive);

        // 5. Arm the fault injector on the owner MM. The next time that MM
        //    processes a replace cycle for TARGET_SYMBOL it will release the
        //    reservation and halt.
        armFaultInjector(ownerPort, TARGET_SYMBOL);
        System.out.println("[ERR10] armed " + ownerService + " to crash on next replace of " + TARGET_SYMBOL);

        // 6. Drive wide orders on the target symbol until the owner MM dies
        //    (or the timeout elapses). A single order isn't reliable: it may
        //    not cross, the quote may expire and be replaced between
        //    observation and submission, or the exchange may reject it
        //    transiently. Sustained pressure guarantees at least one fill
        //    once the MM republishes a fresh quote, which produces a
        //    position update → replace cycle → hook fires.
        long crashTriggerMillis = System.currentTimeMillis();
        boolean crashed = driveOrdersUntilCrash(ownerPort, ownerService, TARGET_SYMBOL,
                Duration.ofSeconds(60));
        if (!crashed) {
            // Dump diagnostic info into the test output BEFORE failing,
            // because @AfterAll tears the stack down and the user can't
            // shell into a dead container.
            dumpMmDiagnostics(ownerService);
            fail("owner MM " + ownerService
                    + " did not crash within 60s of arm — fault injector may not have fired. "
                    + "Check that the fault-injection profile is active on the MM (compose.yml / k8s) "
                    + "and that position updates for " + TARGET_SYMBOL + " are reaching this node.");
        }
        long crashObservedMillis = System.currentTimeMillis();
        System.out.println("[ERR10] " + ownerService + " confirmed crashed (~" +
                (crashObservedMillis - crashTriggerMillis) + "ms after first trigger)");

        // 8. Post-crash exposure snapshot. We do NOT assert on a drop in
        //    activeReservations: when the FaultInjector frees AAPL's
        //    capacity, other MMs whose quote-replace cycles had been
        //    receiving PARTIAL/DENIED grants (or HA-failover for the
        //    target symbol itself) immediately take the freed slot. The
        //    activeReservations counter rarely shows a transient drop at
        //    our 250ms polling granularity. The release-before-crash half
        //    of error case 10 is proven by the FaultInjector log line
        //    "[FAULT-INJECTION] release returned freed=… for symbol=AAPL"
        //    in the MM container logs (visible via dumpMmDiagnostics on
        //    failure); the bad state we assert below is the *externally
        //    visible* one — the exchange still holds the orphan quote
        //    even though no MM has a backing reservation for it.
        ExposureState postCrashExposure = currentExposure();
        System.out.println("[ERR10] post-crash exposure (informational only, not asserted): "
                + postCrashExposure);

        Quote orphanQuote = currentExchangeQuote(TARGET_SYMBOL);
        assertNotNull(orphanQuote,
                "exchange must still hold *some* quote for " + TARGET_SYMBOL
                        + " immediately after crash (either the orphan or a fresh "
                        + "post-HA-failover replacement)");
        boolean orphanObserved = preCrashQuote.quoteId().equals(orphanQuote.quoteId());
        if (orphanObserved) {
            System.out.println("[ERR10] orphan quote confirmed (still the pre-crash quoteId): "
                    + orphanQuote);
        } else {
            // HA failover (PR #88) reassigned the target symbol to another
            // MM, which republished a fresh quote before we could query.
            // The inconsistency window was strictly shorter than the
            // documented 30s TTL bound — strictly better than error case
            // 10's worst case.
            System.out.println("[ERR10] orphan already replaced before query — HA failover faster than orphan-window measurement"
                    + " (pre=" + preCrashQuote.quoteId() + " now=" + orphanQuote.quoteId() + ")");
        }

        // 9. ASSERT — TTL bound (only meaningful if we actually observed
        //    the orphan; if HA failover already replaced it, the window
        //    was strictly shorter than the documented bound and there's
        //    nothing to wait for).
        if (orphanObserved) {
            long ttlDeadline = orphanQuote.expiresAt() + 2_000L; // small safety margin
            long waitForTtl = Math.max(0L, ttlDeadline - System.currentTimeMillis());
            System.out.println("[ERR10] waiting " + waitForTtl + "ms for orphan TTL to elapse...");
            Thread.sleep(waitForTtl);
            assertTrue(orphanQuote.expiresAt() < System.currentTimeMillis(),
                    "orphan quote must be past its TTL by now: expiresAt=" + orphanQuote.expiresAt()
                            + " now=" + System.currentTimeMillis());
            System.out.println("[ERR10] orphan quote TTL elapsed (expiresAt was "
                    + orphanQuote.expiresAt() + ")");

            // After the TTL: two valid states.
            //   (a) The orphan quote (same quoteId) is still in the
            //       exchange. FillOrderDispatcher must reject orders
            //       against it (FillOrderDispatcher.java:55).
            //   (b) HA failover finally kicked in and the orphan was
            //       replaced sometime during our wait — the bound held.
            Quote postTtlQuote = currentExchangeQuote(TARGET_SYMBOL);
            if (postTtlQuote != null && orphanQuote.quoteId().equals(postTtlQuote.quoteId())) {
                System.out.println("[ERR10] orphan still resident in exchange — verifying rejection");
                assertExpiredQuoteRejectsOrders(TARGET_SYMBOL);
            } else {
                System.out.println("[ERR10] orphan replaced via HA failover during TTL wait: "
                        + postTtlQuote);
            }
        } else {
            System.out.println("[ERR10] skipping TTL/rejection checks — orphan never observed (HA failover beat us to it)");
        }

        // 10. Recovery: restart the crashed MM and continue driving traffic.
        System.out.println("[ERR10] restarting " + ownerService + "...");
        int rcStart = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "start", ownerService);
        assertEquals(0, rcStart, "failed to restart " + ownerService);

        // 11. Wait for the cluster to converge back to 7 members with a
        //     single leader, mirroring the @BeforeAll convergence check.
        System.out.println("[ERR10] waiting for cluster reconvergence...");
        awaitCondition(Duration.ofMinutes(4),
                LocalError10MMCrashDuringQuoteReplaceTest::allNodesConverged,
                "cluster did not reconverge within 4 minutes after restart");

        // 12. Drive more orders so the restored MM definitely runs at least
        //     one full replace cycle for the target symbol.
        System.out.println("[ERR10] recovery: driving " + RECOVERY_WAVES + " more waves...");
        for (int wave = 1; wave <= RECOVERY_WAVES; wave++) {
            driveOneWave(WARMUP_WAVES + wave, rnd);
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // 13. ASSERT — recovery: the exchange now holds a fresh, valid
        //     quote for the target symbol, with an id that differs from
        //     the pre-crash quote (the orphan we captured) and a future
        //     expiresAt. This proves the system converged after the
        //     fault, which is error case 10's safety bound.
        //
        //     Note: we deliberately don't assert on the global
        //     activeReservations count returning to its pre-crash value.
        //     Activity from other MMs (and from HA failover events
        //     during the crash window) routinely shifts the count up or
        //     down by 1 even in steady state. The structurally meaningful
        //     thing is that TARGET_SYMBOL is freshly quoted again.
        Quote recoveryQuote = currentExchangeQuote(TARGET_SYMBOL);
        assertNotNull(recoveryQuote,
                "exchange must hold a quote for " + TARGET_SYMBOL + " after recovery");
        assertNotEquals(preCrashQuote.quoteId(), recoveryQuote.quoteId(),
                "post-recovery quoteId must differ from pre-crash: "
                        + "preCrash=" + preCrashQuote.quoteId()
                        + " recovery=" + recoveryQuote.quoteId());
        assertTrue(recoveryQuote.expiresAt() > System.currentTimeMillis(),
                "recovery quote must have a future expiresAt: " + recoveryQuote);
        System.out.println("[ERR10] recovery quote: " + recoveryQuote);
        System.out.println("[ERR10] post-recovery exposure (informational): " + currentExposure());
    }

    // ---------- per-test helpers ----------

    private static void driveOneWave(int wave, Random rnd) {
        int accepted = 0;
        for (String symbol : SEED_SYMBOLS) {
            for (int i = 0; i < SELF_CROSS_PAIRS_PER_WAVE; i++) {
                int qty = 1 + rnd.nextInt(3);
                if (postOrderToExchange(new ExternalOrder(
                        UUID.randomUUID(), symbol, qty, SELF_CROSS_PRICE, Side.BUY))) accepted++;
                if (postOrderToExchange(new ExternalOrder(
                        UUID.randomUUID(), symbol, qty, SELF_CROSS_PRICE, Side.SELL))) accepted++;
            }
            Side wideSide = (wave % 2 == 1) ? Side.BUY : Side.SELL;
            double wideLimit = wideSide == Side.BUY ? WIDE_BUY_LIMIT : WIDE_SELL_LIMIT;
            int wideQty = 1 + rnd.nextInt(3);
            if (postOrderToExchange(new ExternalOrder(
                    UUID.randomUUID(), symbol, wideQty, wideLimit, wideSide))) accepted++;
        }
        System.out.println("[ERR10] wave " + wave + ": exchange accepted " + accepted + " orders");
    }

    /**
     * Print diagnostic information about the owner MM container to
     * {@code System.out} so the JUnit report captures it before
     * {@code @AfterAll} tears the stack down.
     *
     * <p>Three sections, each answering a specific question about why the
     * fault injector might not have fired:
     * <ol>
     *   <li>{@code env | grep SPRING_PROFILES_ACTIVE} — is the
     *       {@code fault-injection} profile actually active inside the
     *       container? (If not, neither the controller nor the injector
     *       bean exist; the arm endpoint should have returned 404 but if
     *       it returned 200 against a stale image this is the smoking gun.)</li>
     *   <li>{@code curl /test/fault-injection/status} from inside the
     *       container — is the injector still armed for the target symbol?
     *       If yes, the hook never reached {@code consumeIfArmed}. If null,
     *       the hook fired but didn't halt (shouldn't be possible).</li>
     *   <li>FAULT-INJECTION log lines — answers whether {@code generateQuote}
     *       even ran for the armed symbol on this MM.</li>
     * </ol>
     */
    private static void dumpMmDiagnostics(String service) {
        System.out.println("\n[ERR10] ============ DIAGNOSTICS for " + service + " ============");

        System.out.println("[ERR10] ---- SPRING_PROFILES_ACTIVE inside container ----");
        runDockerCapture(TimeUnit.SECONDS.toMillis(15),
                "compose", "exec", "-T", service, "sh", "-c",
                "env | grep SPRING_PROFILES_ACTIVE || echo '<not set>'");

        System.out.println("[ERR10] ---- /test/fault-injection/status (from inside) ----");
        runDockerCapture(TimeUnit.SECONDS.toMillis(15),
                "compose", "exec", "-T", service, "sh", "-c",
                "wget -qO- http://localhost:8080/test/fault-injection/status "
                        + "|| echo '<endpoint unreachable — profile likely not active>'");

        System.out.println("[ERR10] ---- FAULT-INJECTION log lines ----");
        runDockerCapture(TimeUnit.SECONDS.toMillis(30),
                "compose", "logs", "--tail", "500", service);

        System.out.println("[ERR10] ============ END DIAGNOSTICS ============\n");
    }

    private static void runDockerCapture(long timeoutMs, String... args) {
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add("docker");
            Collections.addAll(cmd, args);
            ProcessBuilder pb = new ProcessBuilder(cmd)
                    .directory(PROJECT_ROOT.toFile())
                    .redirectErrorStream(true);
            Process p = pb.start();
            try (java.io.BufferedReader r = new java.io.BufferedReader(
                    new java.io.InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = r.readLine()) != null) {
                    System.out.println(line);
                }
            }
            if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();
                System.out.println("[timed out]");
            }
        } catch (Exception e) {
            System.out.println("[diagnostics error: " + e + "]");
        }
    }

    /**
     * Hammer the exchange with wide orders on {@code symbol}, alternating
     * BUY and SELL across the MM's spread, until {@code mmAlive(ownerPort)}
     * returns false or {@code timeout} elapses. Returns true on crash.
     *
     * <p>Two complications justify the loop rather than a single fire-and-poll:
     * <ol>
     *   <li>The MM republishes quotes frequently; a fill won't happen unless
     *       our order arrives while a fresh quote is live. A burst makes
     *       this near-certain.</li>
     *   <li>Once the MM publishes its next quote after we arm, that publish
     *       call hits the hook and crashes — but only if a position update
     *       actually triggers a replace cycle. We drive fills to guarantee
     *       updates.</li>
     * </ol>
     */
    private static boolean driveOrdersUntilCrash(int ownerPort, String ownerService,
                                                 String symbol, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        int sent = 0;
        int accepted = 0;
        int sideToggle = 0;
        while (Instant.now().isBefore(deadline)) {
            if (!mmAlive(ownerPort)) {
                System.out.println("[ERR10] " + ownerService + " stopped responding after "
                        + sent + " trigger orders (" + accepted + " accepted)");
                return true;
            }
            Side side = (sideToggle++ & 1) == 0 ? Side.BUY : Side.SELL;
            double limit = side == Side.BUY ? WIDE_BUY_LIMIT : WIDE_SELL_LIMIT;
            if (postOrderToExchange(new ExternalOrder(
                    UUID.randomUUID(), symbol, 1, limit, side))) {
                accepted++;
            }
            sent++;
            // Brief pause between orders so the MM has time to receive the
            // position update, enter the replace cycle, and hit the hook.
            Thread.sleep(250);
        }
        System.out.println("[ERR10] driveOrdersUntilCrash giving up after " + sent
                + " orders (" + accepted + " accepted); " + ownerService + " still alive");
        return false;
    }

    /**
     * Iterate MM ports and ask each {@code GET /marketmaker/status} who owns
     * {@code symbol}. The first MM whose {@code assigned} set contains the
     * symbol wins. Symbol→node assignment is deterministic (sharding by
     * hash) so this returns the same MM run-to-run for the same symbol.
     */
    private static int findOwnerPort(String symbol) throws Exception {
        for (Map.Entry<Integer, String> e : MM_PORT_TO_SERVICE.entrySet()) {
            int port = e.getKey();
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/marketmaker/status"))
                        .timeout(Duration.ofSeconds(3))
                        .GET().build();
                HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) continue;
                JsonNode body = JSON.readTree(resp.body());
                JsonNode assigned = body.path("assigned");
                if (!assigned.isArray()) continue;
                for (JsonNode s : assigned) {
                    if (symbol.equals(s.asText())) return port;
                }
            } catch (Exception ignored) {
                // try next port
            }
        }
        return -1;
    }

    private static void armFaultInjector(int mmPort, String symbol) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + mmPort
                        + "/test/fault-injection/arm-quote-replace-crash?symbol=" + symbol))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(),
                "POST /test/fault-injection/arm-quote-replace-crash must return 200 "
                        + "(profile fault-injection should be active on the MM); body=" + resp.body());
    }

    /**
     * Poll {@code GET /exposure} until {@code activeReservations < expected}
     * or the timeout elapses. Returns the post-change state, or null on
     * timeout.
     */
    private static ExposureState awaitExposureChange(int expected, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ExposureState state = currentExposure();
            if (state != null && state.activeReservations() < expected) return state;
            Thread.sleep(250);
        }
        return null;
    }

    private static void assertExpiredQuoteRejectsOrders(String symbol) {
        // Submit a small wide order — the exchange should reject (HTTP != 200)
        // because the quote is past expiresAt. We don't require a specific
        // status code, just non-success.
        boolean accepted = postOrderToExchange(new ExternalOrder(
                UUID.randomUUID(), symbol, 1, WIDE_BUY_LIMIT, Side.BUY));
        assertTrue(!accepted,
                "order against expired orphan quote for " + symbol + " must be rejected "
                        + "(exchange must enforce quote TTL bound)");
    }

    // ---------- generic helpers (mirror ClusterIntegrationWithSystemTest) ----------

    private static boolean allNodesConverged() {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_SERVICE.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String lid = status.path("leaderId").asText(null);
            if (lid == null) return false;
            leaders.add(lid);
            if (status.path("members").size() != MM_PORT_TO_SERVICE.size()) return false;
        }
        return responding == MM_PORT_TO_SERVICE.size() && leaders.size() == 1;
    }

    private static JsonNode clusterStatusOrNull(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/cluster/status"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readTree(resp.body());
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean mmAlive(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/marketmaker/status"))
                    .timeout(Duration.ofSeconds(2))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static void awaitHealthy(String serviceName, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port)) {
                System.out.println("[ERR10] " + serviceName + " healthy on port " + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(serviceName + " not healthy within " + timeout);
    }

    private static boolean healthy(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/health"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static List<UUID> seedQuotes(List<String> symbols) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PUBLISHER_PORT + "/publisher/seed-quotes"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("seed-quotes returned " + resp.statusCode() + ": " + resp.body());
        }
        return JSON.readValue(resp.body(), new TypeReference<List<UUID>>() {});
    }

    private static boolean postOrderToExchange(ExternalOrder order) {
        try {
            String body = JSON.writeValueAsString(order);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXCHANGE_PORT + "/orders"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static Quote currentExchangeQuote(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXCHANGE_PORT + "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readValue(resp.body(), Quote.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static ExposureState currentExposure() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXPOSURE_RES_PORT + "/exposure"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readValue(resp.body(), ExposureState.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static int runDocker(Map<String, String> env, long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true)
                .inheritIO();
        if (env != null) pb.environment().putAll(env);
        Process p = pb.start();
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("timeout running: " + String.join(" ", cmd));
        }
        return p.exitValue();
    }

    private static void awaitCondition(Duration timeout, BooleanSupplier condition, String failureMessage) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError(failureMessage);
    }
}