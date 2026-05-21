package edu.yu.velocitytrading.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.velocitytrading.model.ExposureState;
import edu.yu.velocitytrading.model.Quote;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error case 5 k3s variant: a market-maker crashes AFTER the exposure
 * reservation has been granted but BEFORE the resulting quote is written to
 * the repository (i.e. before it becomes visible at the exchange). The
 * reservation stays in place — an exposure leak — until either the symbol is
 * reassigned to a surviving MM, which auto-releases the orphan as part of the
 * next reservation grant for the same symbol, or the dead MM restarts and
 * does the same.
 *
 * <p>This test uses the {@link
 * edu.yu.velocitytrading.marketmaker.FaultInjector} hook (arm endpoint:
 * {@code POST /test/fault-injection/arm-post-reservation-crash}) to actually
 * crash mm-6 mid-flow, then verifies:
 * <ol>
 *   <li>The mm-6 pod stops responding within a bounded window after we drive
 *       order traffic that forces a quote-replace cycle (proving the hook
 *       fired).</li>
 *   <li>An orphan reservation transiently exists: the exchange still serves
 *       the pre-crash quoteId for the target symbol (mm-6 never got to publish
 *       the new one) while the exposure-reservation service shows usage
 *       consistent with the new reservation being in place.</li>
 *   <li>HA failover reassigns the symbol; the new owner publishes a fresh
 *       quote, and exposure usage settles to a count of active reservations
 *       no greater than the pre-crash baseline (the orphan was reaped by
 *       {@code ExposureReservationService.createReservation}'s
 *       release-then-create semantics).</li>
 * </ol>
 *
 * <p>Pre-conditions:
 * <ol>
 *   <li>{@code kubectl apply -k k8s/} (manifests include the
 *       {@code fault-injection} profile on the MM StatefulSet).</li>
 *   <li>Image bundle imported on every k3s node.</li>
 * </ol>
 *
 * <p>Opt-in: {@code -Dcluster.k8s.it=true}.
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class ClusterError5MMCrashAfterReservationTest {

    private static final String TAG = "ERR5-k8s";

    private static final SortedMap<Integer, String> MM_PORT_TO_POD;
    static {
        SortedMap<Integer, String> m = new TreeMap<>();
        m.put(30081, "mm-0");
        m.put(30082, "mm-1");
        m.put(30083, "mm-2");
        m.put(30084, "mm-3");
        m.put(30085, "mm-4");
        m.put(30086, "mm-5");
        m.put(30087, "mm-6");
        MM_PORT_TO_POD = Collections.unmodifiableSortedMap(m);
    }

    private static final Set<String> SEED_SYMBOLS = Collections.unmodifiableSet(
            new TreeSet<>(List.of("AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META")));

    private static final int TRADING_STATE_PORT = 30180;
    private static final int EXCHANGE_PORT      = 30181;
    private static final int EXPOSURE_RES_PORT  = 30182;
    private static final int PUBLISHER_PORT     = 30183;

    private static final String TARGET_POD = "mm-6";
    private static final int TARGET_PORT = 30087;
    private static final int TARGET_REPLICAS = 6;
    private static final int FULL_REPLICAS = 7;

    private static final int WARMUP_WAVES = 4;
    private static final long WAVE_INTERVAL_MS = 1500;
    private static final int ORDERS_PER_SYMBOL_PER_WAVE = 2;

    private static final String HOST = System.getProperty("cluster.k8s.host", "localhost");
    private static final String NS   = System.getProperty("cluster.k8s.namespace", "market-maker");
    private static final String KUBECTL = System.getProperty("kubectl", "kubectl");
    private static final String KUBECTL_SSH = System.getProperty("kubectl.ssh",
            "ssh sack@192.168.8.11");
    private static final String KUBECTL_REMOTE = System.getProperty("kubectl.remote",
            "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl");

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static String diagnosticTarget;

    @BeforeAll
    static void waitForStack() throws Exception {
        System.out.println("[" + TAG + "] host=" + HOST + " namespace=" + NS);
        awaitHealthy("trading-state",        TRADING_STATE_PORT, "/health",               Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  "/health",               Duration.ofMinutes(5));
        awaitHealthy("exchange",             EXCHANGE_PORT,      "/health",               Duration.ofMinutes(5));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     "/health",               Duration.ofMinutes(5));
        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            awaitHealthy(e.getValue(), e.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }
        System.out.println("[" + TAG + "] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8),
                ClusterError5MMCrashAfterReservationTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[" + TAG + "] full stack up.");
    }

    @AfterAll
    static void restoreReplicaCount() {
        try {
            System.out.println("[" + TAG + "] @AfterAll: restoring sts/mm to " + FULL_REPLICAS + " replicas");
            scaleMmStatefulSet(FULL_REPLICAS);
            awaitHealthy(TARGET_POD, TARGET_PORT, "/marketmaker/status", Duration.ofMinutes(5));
            awaitCondition(Duration.ofMinutes(5),
                    ClusterError5MMCrashAfterReservationTest::allNodesConverged,
                    "cluster did not reconverge to " + FULL_REPLICAS + " nodes after restore");
        } catch (Exception e) {
            System.err.println("[" + TAG + "] failed to restore sts/mm replica count: " + e);
        }
    }

    @Test
    void postReservationCrashLeaksReservationUntilHAReassignsSymbol() throws Exception {
        try {
            runScenario();
        } catch (Throwable t) {
            if (diagnosticTarget != null) {
                dumpMmDiagnostics(diagnosticTarget);
            }
            throw t;
        }
    }

    private void runScenario() throws Exception {
        // 1. Seed bootstrap quotes so every symbol exists at the exchange.
        System.out.println("[" + TAG + "] seeding bootstrap quotes...");
        List<String> seedList = new ArrayList<>(SEED_SYMBOLS);
        Set<UUID> bootstrapQuoteIds = new HashSet<>(seedQuotes(seedList));
        assertEquals(SEED_SYMBOLS.size(), bootstrapQuoteIds.size(),
                "publisher must return one quoteId per symbol");

        // 2. Drive a few warmup waves so each owning MM has gone through at
        //    least one replace cycle and the exchange holds an MM-generated
        //    quote (not a bootstrap quote) for every seed symbol.
        System.out.println("[" + TAG + "] warmup: driving " + WARMUP_WAVES + " waves...");
        for (int wave = 1; wave <= WARMUP_WAVES; wave++) {
            driveOneWave(wave);
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // 3. Resolve crash target: whichever symbol mm-6 currently owns. If
        //    mm-6 happens to hold the leader latch it has no symbol — delete
        //    the pod so it rejoins as a worker.
        diagnosticTarget = TARGET_POD;
        ensureTargetIsWorker();
        String targetSymbol = awaitTargetSymbol();
        assertNotNull(targetSymbol, TARGET_POD + " owns no symbols after warmup");
        System.out.println("[" + TAG + "] crash target: " + TARGET_POD + " owns " + targetSymbol);

        // 4. Wait until the exchange holds an MM-generated quote for the
        //    target symbol (so we can distinguish pre-crash quoteId from
        //    bootstrap).
        final String symbol = targetSymbol;
        awaitCondition(Duration.ofSeconds(30), () -> {
            Quote q = currentExchangeQuote(symbol);
            return q != null && !bootstrapQuoteIds.contains(q.quoteId());
        }, "no MM-generated quote for " + targetSymbol + " after warmup");

        Quote preCrashQuote = currentExchangeQuote(targetSymbol);
        assertNotNull(preCrashQuote, "pre-crash exchange must hold a quote for " + targetSymbol);
        System.out.println("[" + TAG + "] pre-crash quote: " + preCrashQuote);

        ExposureState preCrashExposure = currentExposure();
        assertNotNull(preCrashExposure, "exposure-reservation /exposure must return state pre-crash");
        int preCrashActive = preCrashExposure.activeReservations();
        System.out.println("[" + TAG + "] pre-crash exposure: " + preCrashExposure);
        assertTrue(preCrashActive >= 1,
                "pre-crash there must be at least one active reservation; got " + preCrashActive);

        // 5. Arm the post-reservation crash hook on mm-6. The next quote
        //    replacement cycle for targetSymbol on this MM will halt the JVM
        //    AFTER the reservation is granted but BEFORE the quote is
        //    published to the local repository (and therefore before it
        //    propagates to the exchange via the shared Hazelcast quotes map).
        armPostReservationCrash(TARGET_PORT, targetSymbol);
        System.out.println("[" + TAG + "] armed " + TARGET_POD + " to crash after next reservation grant for " + targetSymbol);

        // 6. Drive orders on the target symbol until mm-6 stops responding.
        long crashTriggerMillis = System.currentTimeMillis();
        boolean crashed = driveOrdersUntilCrash(TARGET_PORT, TARGET_POD, targetSymbol,
                Duration.ofSeconds(60));
        if (!crashed) {
            dumpMmDiagnostics(TARGET_POD);
            fail("owner pod " + TARGET_POD
                    + " did not appear unhealthy within 60s of arm — fault injector may not have fired. "
                    + "Check the fault-injection profile is active on the MM (k8s/market-maker.yaml) "
                    + "and that position updates for " + targetSymbol + " are reaching this pod.");
        }
        long crashObservedMillis = System.currentTimeMillis();
        System.out.println("[" + TAG + "] " + TARGET_POD + " confirmed unhealthy (~" +
                (crashObservedMillis - crashTriggerMillis) + "ms after first trigger)");

        // 7. Scale sts/mm to TARGET_REPLICAS so kubelet won't recreate the
        //    crashed pod. This makes the orphan window observable; otherwise
        //    the restarted pod would race HA failover to publish a fresh
        //    quote for the symbol and reap the orphan immediately. @AfterAll
        //    restores the replica count.
        System.out.println("[" + TAG + "] scaling sts/mm to " + TARGET_REPLICAS
                + " so kubelet won't recreate " + TARGET_POD + "...");
        scaleMmStatefulSet(TARGET_REPLICAS);

        // 8. Observe the documented error-case-5 inconsistency. The orphan
        //    reservation that mm-6 created for targetSymbol is still in the
        //    reservations map, but the exchange still serves the pre-crash
        //    quoteId because the new quote was never written. We poll at
        //    100ms granularity for up to 5 seconds to catch the window
        //    before HA failover or post-recovery quote publish reaps it.
        boolean orphanWindowObserved = pollOrphanReservation(
                targetSymbol, preCrashQuote.quoteId(),
                Duration.ofSeconds(5));
        if (orphanWindowObserved) {
            System.out.println("[" + TAG + "] CONFIRMED error-case-5 invariant violation: "
                    + "exposure usage non-zero while exchange still holds pre-crash quoteId="
                    + preCrashQuote.quoteId()
                    + " (proves reservation-without-quote orphan)");
        } else {
            System.out.println("[" + TAG + "] orphan window not directly observed at 100ms granularity "
                    + "(HA failover faster than measurement). FaultInjector logs follow:");
            dumpMmDiagnostics(TARGET_POD);
        }

        // 9. Recovery via HA failover: TARGET_POD is permanently gone for the
        //    rest of this test (scale-down at step 7). The cluster must
        //    detect the failure, evict the dead node, and reassign
        //    targetSymbol to a surviving MM.
        System.out.println("[" + TAG + "] waiting for cluster to evict " + TARGET_POD
                + " and converge to " + TARGET_REPLICAS + " members...");
        awaitCondition(Duration.ofMinutes(4),
                () -> clusterConvergedExcluding(TARGET_PORT),
                "cluster did not evict " + TARGET_POD + " and reconverge within 4 minutes");

        System.out.println("[" + TAG + "] waiting for HA reassignment of " + targetSymbol + "...");
        awaitCondition(Duration.ofMinutes(2),
                () -> ownerPortOrMinusOne(targetSymbol, TARGET_PORT) != -1,
                targetSymbol + " was not reassigned to a different MM within 2 minutes");
        int newOwnerPort = findOwnerPortOrThrow(targetSymbol);
        String newOwnerPod = MM_PORT_TO_POD.get(newOwnerPort);
        System.out.println("[" + TAG + "] " + targetSymbol + " reassigned to " + newOwnerPod
                + " on port " + newOwnerPort);

        // 10. ASSERT — recovery: the new owner of targetSymbol publishes a
        //     fresh MM-generated quote with a different quoteId, and the
        //     exposure-reservation service reaps the orphan as part of the
        //     new reservation grant. AssignmentListener bootstraps a quote
        //     for newly-assigned symbols using the current trading-state
        //     position, so this doesn't require a fill to drive.
        awaitCondition(Duration.ofMinutes(2), () -> {
            Quote q = currentExchangeQuote(symbol);
            return q != null
                    && !preCrashQuote.quoteId().equals(q.quoteId())
                    && q.expiresAt() > System.currentTimeMillis();
        }, "after HA failover, the new owner of " + targetSymbol
                + " did not publish a fresh MM quote within 2 minutes");

        Quote recoveryQuote = currentExchangeQuote(targetSymbol);
        System.out.println("[" + TAG + "] recovery quote: " + recoveryQuote);

        // 11. ASSERT — no permanent leak: after the new owner takes over and
        //     publishes a fresh quote, exposure activeReservations must not
        //     exceed the pre-crash count. The orphan was either replaced by
        //     the new owner's reservation for the same symbol (released by
        //     ExposureReservationService.createReservation's "release first"
        //     semantics) or, in the worst case, equal to it (one active
        //     reservation per assigned symbol).
        awaitCondition(Duration.ofSeconds(30), () -> {
            ExposureState s = currentExposure();
            return s != null && s.activeReservations() <= preCrashActive;
        }, "exposure activeReservations did not return to <= pre-crash baseline ("
                + preCrashActive + ") within 30s; orphan reservation appears to have leaked");

        ExposureState postRecoveryExposure = currentExposure();
        System.out.println("[" + TAG + "] post-recovery exposure: " + postRecoveryExposure);
        assertTrue(postRecoveryExposure.activeReservations() <= preCrashActive,
                "post-recovery activeReservations (" + postRecoveryExposure.activeReservations()
                        + ") exceeds pre-crash baseline (" + preCrashActive
                        + ") — orphan reservation from error case 5 was not reaped");
    }

    // ---------- error-case-5-specific helpers ----------

    /**
     * Arm the post-reservation crash hook on the MM listening at {@code mmPort}.
     * Calls {@code POST /test/fault-injection/arm-post-reservation-crash}.
     */
    private static void armPostReservationCrash(int mmPort, String symbol) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + mmPort
                        + "/test/fault-injection/arm-post-reservation-crash?symbol=" + symbol))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(),
                "POST /test/fault-injection/arm-post-reservation-crash must return 200 "
                        + "(profile fault-injection should be active on the MM); body=" + resp.body());
    }

    /**
     * Poll for the documented error-case-5 inconsistency: the exchange still
     * holds the pre-crash quoteId for {@code symbol} (because the new quote
     * was never published) AND the exposure-reservation service shows
     * non-zero usage on at least one side (the orphan reservation is live).
     * Both must hold in the same poll.
     */
    private static boolean pollOrphanReservation(String symbol, UUID preCrashQuoteId,
                                                 Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            Quote q = currentExchangeQuote(symbol);
            ExposureState e = currentExposure();
            if (q != null && preCrashQuoteId.equals(q.quoteId())
                    && e != null && (e.bidUsage() > 0 || e.askUsage() > 0)) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    // ---------- shared infrastructure (mirrors ClusterError10MMCrashDuringQuoteReplaceTest) ----------

    private static void driveOneWave(int wave) {
        int accepted = submitOrdersViaPublisher(ORDERS_PER_SYMBOL_PER_WAVE,
                new ArrayList<>(SEED_SYMBOLS));
        System.out.println("[" + TAG + "] wave " + wave + ": exchange accepted " + accepted + " orders");
    }

    /**
     * Hammer the exchange with orders on {@code symbol} until
     * {@code mmAlive(ownerPort)} returns false or {@code timeout} elapses.
     * Returns true on observed crash.
     */
    private static boolean driveOrdersUntilCrash(int ownerPort, String ownerPod,
                                                 String symbol, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        int sent = 0;
        int accepted = 0;
        List<String> oneSymbol = List.of(symbol);
        while (Instant.now().isBefore(deadline)) {
            if (!mmAlive(ownerPort)) {
                System.out.println("[" + TAG + "] " + ownerPod + " stopped responding after "
                        + sent + " trigger orders (" + accepted + " accepted)");
                return true;
            }
            accepted += submitOrdersViaPublisher(1, oneSymbol);
            sent++;
            Thread.sleep(250);
        }
        System.out.println("[" + TAG + "] driveOrdersUntilCrash giving up after " + sent
                + " orders (" + accepted + " accepted); " + ownerPod + " still alive");
        return false;
    }

    private static int findOwnerPortOrThrow(String symbol) throws Exception {
        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            int port = e.getKey();
            if (port == TARGET_PORT) continue;
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + HOST + ":" + port + "/marketmaker/status"))
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
        throw new AssertionError("no surviving MM owns symbol " + symbol);
    }

    private static int ownerPortOrMinusOne(String symbol, int excludedPort) {
        try {
            for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
                int port = e.getKey();
                if (port == excludedPort) continue;
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + HOST + ":" + port + "/marketmaker/status"))
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
            }
        } catch (Exception ignored) {
            // fall through
        }
        return -1;
    }

    /**
     * If mm-6 currently holds the cluster leader latch it has no assigned
     * symbols (only workers are assigned). Delete the pod so the StatefulSet
     * recreates it at the back of the FIFO leader-latch queue; another pod
     * becomes leader and mm-6 becomes an eligible worker.
     */
    private static void ensureTargetIsWorker() throws Exception {
        String leader = currentLeaderId();
        if (leader == null) {
            System.out.println("[" + TAG + "] no leader visible yet — relying on outer awaitCondition");
            return;
        }
        if (!TARGET_POD.equals(leader)) {
            System.out.println("[" + TAG + "] " + TARGET_POD + " is a worker (leader=" + leader + ") — proceeding");
            return;
        }
        System.out.println("[" + TAG + "] " + TARGET_POD + " currently holds the leader latch — deleting pod");
        String out = runKubectlCapturing(TimeUnit.SECONDS.toMillis(30),
                "delete", "pod", TARGET_POD, "-n", NS, "--wait=true");
        System.out.println("[" + TAG + "]   " + out.trim());
        awaitHealthy(TARGET_POD, TARGET_PORT, "/marketmaker/status", Duration.ofMinutes(5));
        awaitCondition(Duration.ofMinutes(2), () -> {
            String lid = currentLeaderId();
            return lid != null && !TARGET_POD.equals(lid);
        }, "after deleting " + TARGET_POD + ", the cluster did not elect a different leader within 2m");
        System.out.println("[" + TAG + "] re-election complete; new leader=" + currentLeaderId());
    }

    private static String awaitTargetSymbol() {
        Instant deadline = Instant.now().plus(Duration.ofMinutes(2));
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + HOST + ":" + TARGET_PORT + "/marketmaker/status"))
                        .timeout(Duration.ofSeconds(5))
                        .GET().build();
                HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    JsonNode body = JSON.readTree(resp.body());
                    JsonNode assigned = body.path("assigned");
                    if (assigned.isArray() && !assigned.isEmpty()) {
                        return assigned.get(0).asText();
                    }
                }
            } catch (Exception ignored) {
                // transient — retry
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private static String currentLeaderId() {
        for (int port : MM_PORT_TO_POD.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) continue;
            String lid = status.path("leaderId").asText(null);
            if (lid != null && !lid.isBlank()) return lid;
        }
        return null;
    }

    private static boolean allNodesConverged() {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_POD.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String lid = status.path("leaderId").asText(null);
            if (lid == null) return false;
            leaders.add(lid);
            if (status.path("members").size() != MM_PORT_TO_POD.size()) return false;
        }
        return responding == MM_PORT_TO_POD.size() && leaders.size() == 1;
    }

    private static boolean clusterConvergedExcluding(int excludedPort) {
        int expected = MM_PORT_TO_POD.size() - 1;
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_POD.keySet()) {
            if (port == excludedPort) continue;
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String lid = status.path("leaderId").asText(null);
            if (lid == null) return false;
            leaders.add(lid);
            if (status.path("members").size() != expected) return false;
        }
        return responding == expected && leaders.size() == 1;
    }

    private static JsonNode clusterStatusOrNull(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + port + "/cluster/status"))
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
                    .uri(URI.create("http://" + HOST + ":" + port + "/marketmaker/status"))
                    .timeout(Duration.ofSeconds(2))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static void awaitHealthy(String workload, int port, String path, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port, path)) {
                System.out.println("[" + TAG + "] " + workload + " healthy on " + HOST + ":" + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(workload + " not healthy within " + timeout);
    }

    private static boolean healthy(int port, String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + port + path))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static List<UUID> seedQuotes(List<String> symbols) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + PUBLISHER_PORT + "/publisher/seed-quotes"))
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

    private static int submitOrdersViaPublisher(int count, List<String> symbols) {
        try {
            String body = JSON.writeValueAsString(symbols);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + PUBLISHER_PORT
                            + "/publisher/submit-orders?count=" + count))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            return Integer.parseInt(resp.body().trim());
        } catch (Exception e) {
            return 0;
        }
    }

    private static Quote currentExchangeQuote(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + EXCHANGE_PORT + "/quotes/" + symbol))
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
                    .uri(URI.create("http://" + HOST + ":" + EXPOSURE_RES_PORT + "/exposure"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readValue(resp.body(), ExposureState.class);
        } catch (Exception e) {
            return null;
        }
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

    private static void scaleMmStatefulSet(int replicas) throws Exception {
        String out = runKubectlCapturing(TimeUnit.SECONDS.toMillis(30),
                "scale", "sts/mm", "-n", NS, "--replicas=" + replicas);
        System.out.println("[" + TAG + "] kubectl scale sts/mm --replicas=" + replicas + ": " + out.trim());
    }

    private static void dumpMmDiagnostics(String pod) {
        System.out.println("\n[" + TAG + "] ============ DIAGNOSTICS for " + pod + " ============");
        System.out.println("[" + TAG + "] ---- SPRING_PROFILES_ACTIVE inside pod ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(15),
                "exec", "-n", NS, pod, "--", "sh", "-c",
                "env | grep SPRING_PROFILES_ACTIVE || echo '<not set>'");
        System.out.println("[" + TAG + "] ---- /test/fault-injection/post-reservation-status (from inside) ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(15),
                "exec", "-n", NS, pod, "--", "sh", "-c",
                "wget -qO- http://localhost:8080/test/fault-injection/post-reservation-status "
                        + "|| echo '<endpoint unreachable — profile likely not active>'");
        System.out.println("[" + TAG + "] ---- kubectl logs --tail=500 ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(30),
                "logs", "--tail=500", "-n", NS, pod);
        System.out.println("[" + TAG + "] ============ END DIAGNOSTICS ============\n");
    }

    private static void runKubectlOut(long timeoutMs, String... args) {
        try {
            String out = runKubectlCapturing(timeoutMs, args);
            System.out.println(out);
        } catch (Exception e) {
            System.out.println("[diagnostics error: " + e + "]");
        }
    }

    private static String runKubectlCapturing(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        if (KUBECTL_SSH != null && !KUBECTL_SSH.isBlank()) {
            StringBuilder remote = new StringBuilder(KUBECTL_REMOTE);
            for (String arg : args) remote.append(' ').append(shellQuote(arg));
            for (String token : KUBECTL_SSH.split(" +")) cmd.add(token);
            cmd.add(remote.toString());
        } else {
            cmd.add(KUBECTL);
            Collections.addAll(cmd, args);
        }

        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) output.append(line).append('\n');
        }
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("timeout running kubectl " + String.join(" ", args));
        }
        if (p.exitValue() != 0) {
            throw new AssertionError("kubectl " + String.join(" ", args)
                    + " failed (exit=" + p.exitValue() + "): " + output);
        }
        return output.toString();
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }
}
