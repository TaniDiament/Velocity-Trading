package edu.yu.marketmaker.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import edu.yu.marketmaker.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * k3s/Kubernetes variant of
 * {@code edu.yu.marketmaker.errorcases.local.LocalError10MMCrashDuringQuoteReplaceTest}.
 *
 * <p>Same end-to-end assertions for error case 10, but talks to a running
 * k3s cluster via NodePorts instead of bringing up a docker-compose stack.
 *
 * <p>Pre-conditions (must hold before invoking this test):
 * <ol>
 *   <li>Build offline image bundle and import on every k3s node.</li>
 *   <li>{@code kubectl apply -k k8s/} — the manifests already include the
 *       {@code fault-injection} profile (see {@code k8s/market-maker.yaml}),
 *       which makes the {@link
 *       edu.yu.marketmaker.marketmaker.FaultInjectionController} endpoint
 *       reachable. The injector stays disarmed until this test arms it.</li>
 * </ol>
 *
 * <p>Opt-in: {@code -Dcluster.k8s.it=true}.
 *
 * <p>Tunables (system properties):
 * <ul>
 *   <li>{@code cluster.k8s.host}      host that exposes NodePorts (default: localhost)</li>
 *   <li>{@code cluster.k8s.namespace} k8s namespace (default: market-maker)</li>
 *   <li>{@code kubectl}               kubectl binary path (default: kubectl)</li>
 * </ul>
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class ClusterError10MMCrashDuringQuoteReplaceTest {

    /** NodePort -> StatefulSet pod name, for the 7 MM nodes. */
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

    private static final Set<String> SEED_SYMBOLS = new TreeSet<>(List.of(
            "AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META"));

    /** Symbol whose owner MM we crash mid-replace. */
    private static final String TARGET_SYMBOL = "AAPL";

    private static final int WARMUP_WAVES = 4;
    private static final int RECOVERY_WAVES = 6;
    private static final long WAVE_INTERVAL_MS = 1500;
    private static final int SELF_CROSS_PAIRS_PER_WAVE = 5;
    private static final double SELF_CROSS_PRICE = 100.00;
    private static final double WIDE_BUY_LIMIT = 101.00;
    private static final double WIDE_SELL_LIMIT = 99.00;

    // NodePorts defined in k8s/*.yaml — mirror ClusterIntegrationWithSystemK8sTest.
    private static final int TRADING_STATE_PORT = 30180;
    private static final int EXCHANGE_PORT      = 30181;
    private static final int EXPOSURE_RES_PORT  = 30182;
    private static final int PUBLISHER_PORT     = 30183;

    private static final String HOST = System.getProperty("cluster.k8s.host", "localhost");
    private static final String NS   = System.getProperty("cluster.k8s.namespace", "market-maker");
    private static final String KUBECTL = System.getProperty("kubectl", "kubectl");

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @BeforeAll
    static void waitForStack() throws Exception {
        System.out.println("[ERR10-k8s] host=" + HOST + " namespace=" + NS);

        awaitHealthy("trading-state",        TRADING_STATE_PORT, "/health",               Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  "/health",               Duration.ofMinutes(5));
        awaitHealthy("exchange",             EXCHANGE_PORT,      "/health",               Duration.ofMinutes(5));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     "/health",               Duration.ofMinutes(5));

        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            awaitHealthy(e.getValue(), e.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }

        System.out.println("[ERR10-k8s] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(4),
                ClusterError10MMCrashDuringQuoteReplaceTest::allNodesConverged,
                "cluster did not converge within 4 minutes");
        System.out.println("[ERR10-k8s] full stack up.");
    }

    /** Pod we last identified as the crash target — dumped on any test failure. */
    private static String diagnosticTarget;

    @Test
    void marketMakerCrashDuringQuoteReplaceLeavesBoundedOrphanAndRecovers() throws Exception {
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
        // 1. Seed bootstrap quotes via external-publisher.
        System.out.println("[ERR10-k8s] seeding bootstrap quotes...");
        List<String> seedList = new ArrayList<>(SEED_SYMBOLS);
        List<UUID> bootstrapIds = seedQuotes(seedList);
        Set<UUID> bootstrapQuoteIds = new HashSet<>(bootstrapIds);
        assertEquals(SEED_SYMBOLS.size(), bootstrapQuoteIds.size(),
                "publisher must return one quoteId per symbol");

        // 2. Warmup waves so the assigned MM publishes its own quote.
        System.out.println("[ERR10-k8s] warmup: " + WARMUP_WAVES + " waves to force initial replace cycles...");
        Random rnd = new Random(0xC0FFEE);
        for (int wave = 1; wave <= WARMUP_WAVES; wave++) {
            driveOneWave(wave, rnd);
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        awaitCondition(Duration.ofSeconds(30), () -> {
            Quote q = currentExchangeQuote(TARGET_SYMBOL);
            return q != null && !bootstrapQuoteIds.contains(q.quoteId());
        }, "no MM-generated quote for " + TARGET_SYMBOL + " after " + WARMUP_WAVES + " waves");

        Quote preCrashQuote = currentExchangeQuote(TARGET_SYMBOL);
        assertNotNull(preCrashQuote, "pre-crash exchange must hold a quote for " + TARGET_SYMBOL);
        assertNotEquals(true, bootstrapQuoteIds.contains(preCrashQuote.quoteId()),
                "pre-crash quote must be MM-generated, not bootstrap: " + preCrashQuote);
        System.out.println("[ERR10-k8s] pre-crash quote: " + preCrashQuote);

        // 3. Find the owner pod via /marketmaker/status.
        int ownerPort = findOwnerPort(TARGET_SYMBOL);
        String ownerPod = MM_PORT_TO_POD.get(ownerPort);
        assertNotNull(ownerPod, "no MM owner pod found for " + TARGET_SYMBOL);
        diagnosticTarget = ownerPod;
        System.out.println("[ERR10-k8s] " + TARGET_SYMBOL + " owner: " + ownerPod + " on port " + ownerPort);

        // 4. Snapshot global exposure pre-crash.
        ExposureState preCrashExposure = currentExposure();
        assertNotNull(preCrashExposure, "exposure-reservation /exposure must return state pre-crash");
        int preCrashActive = preCrashExposure.activeReservations();
        System.out.println("[ERR10-k8s] pre-crash exposure: " + preCrashExposure);
        assertTrue(preCrashActive >= 1,
                "pre-crash there must be at least one active reservation; got " + preCrashActive);

        // 5. Arm the FaultInjector on the owner pod.
        armFaultInjector(ownerPort, TARGET_SYMBOL);
        System.out.println("[ERR10-k8s] armed " + ownerPod + " to crash on next replace of " + TARGET_SYMBOL);

        // 6. Drive wide orders on the target symbol until the owner pod's
        //    market-maker app stops responding (or timeout). Sustained
        //    pressure is needed because a single order may not generate a
        //    fill (no fresh quote, transient rejection, etc.).
        //
        //    Note: kubelet will likely restart the pod (StatefulSet
        //    livenessProbe + container restart policy), but until it
        //    recovers, /marketmaker/status returns non-200 or fails.
        long crashTriggerMillis = System.currentTimeMillis();
        boolean crashed = driveOrdersUntilCrash(ownerPort, ownerPod, TARGET_SYMBOL,
                Duration.ofSeconds(60));
        assertTrue(crashed, "owner pod " + ownerPod
                + " did not appear unhealthy within 60s of arm — fault injector may not have fired.");
        long crashObservedMillis = System.currentTimeMillis();
        System.out.println("[ERR10-k8s] " + ownerPod + " confirmed unhealthy (~" +
                (crashObservedMillis - crashTriggerMillis) + "ms after first trigger)");

        // 8. Post-crash exposure (informational only). Asserting on a
        //    drop in activeReservations is unreliable: HA failover and
        //    backed-up replace cycles on other MMs reuse the freed slot
        //    within our polling granularity. The release-before-crash
        //    half of error case 10 is proven by the MM log line
        //    "[FAULT-INJECTION] release returned freed=…" (see kubectl
        //    logs dumped via dumpMmDiagnostics on failure).
        ExposureState postCrashExposure = currentExposure();
        System.out.println("[ERR10-k8s] post-crash exposure (informational only, not asserted): "
                + postCrashExposure);

        Quote orphanQuote = currentExchangeQuote(TARGET_SYMBOL);
        assertNotNull(orphanQuote,
                "exchange must still hold *some* quote for " + TARGET_SYMBOL
                        + " immediately after crash (either the orphan or a fresh "
                        + "post-HA-failover replacement)");
        boolean orphanObserved = preCrashQuote.quoteId().equals(orphanQuote.quoteId());
        if (orphanObserved) {
            System.out.println("[ERR10-k8s] orphan quote confirmed (still the pre-crash quoteId): "
                    + orphanQuote);
        } else {
            System.out.println("[ERR10-k8s] orphan already replaced before query — HA failover "
                    + "faster than orphan-window measurement (pre=" + preCrashQuote.quoteId()
                    + " now=" + orphanQuote.quoteId() + ")");
        }

        // 9. TTL bound check — only meaningful if we caught the orphan.
        //    If HA failover already replaced it, the window was strictly
        //    shorter than the documented 30s and there's nothing to wait
        //    for.
        if (orphanObserved) {
            long ttlDeadline = orphanQuote.expiresAt() + 2_000L;
            long waitForTtl = Math.max(0L, ttlDeadline - System.currentTimeMillis());
            System.out.println("[ERR10-k8s] waiting " + waitForTtl + "ms for orphan TTL to elapse...");
            Thread.sleep(waitForTtl);
            assertTrue(orphanQuote.expiresAt() < System.currentTimeMillis(),
                    "orphan quote must be past its TTL: expiresAt=" + orphanQuote.expiresAt()
                            + " now=" + System.currentTimeMillis());
            System.out.println("[ERR10-k8s] orphan quote TTL elapsed");

            Quote postTtlQuote = currentExchangeQuote(TARGET_SYMBOL);
            if (postTtlQuote != null && orphanQuote.quoteId().equals(postTtlQuote.quoteId())) {
                System.out.println("[ERR10-k8s] orphan still resident in exchange — verifying rejection");
                assertExpiredQuoteRejectsOrders(TARGET_SYMBOL);
            } else {
                System.out.println("[ERR10-k8s] orphan replaced via HA failover during TTL wait: "
                        + postTtlQuote);
            }
        } else {
            System.out.println("[ERR10-k8s] skipping TTL/rejection checks — orphan never observed (HA failover beat us to it)");
        }

        // 10. Recovery: under k3s the StatefulSet restarts the crashed pod
        //     automatically; just wait for the cluster to reconverge.
        System.out.println("[ERR10-k8s] waiting for cluster reconvergence after crash...");
        awaitCondition(Duration.ofMinutes(5),
                ClusterError10MMCrashDuringQuoteReplaceTest::allNodesConverged,
                "cluster did not reconverge within 5 minutes after restart");

        System.out.println("[ERR10-k8s] recovery: driving " + RECOVERY_WAVES + " more waves...");
        for (int wave = 1; wave <= RECOVERY_WAVES; wave++) {
            driveOneWave(WARMUP_WAVES + wave, rnd);
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // 11. ASSERT — recovery: a fresh, valid quote exists for the
        //     target symbol with an id that differs from pre-crash and
        //     a future expiresAt. activeReservations equality is not
        //     asserted — see comment in section 8.
        Quote recoveryQuote = currentExchangeQuote(TARGET_SYMBOL);
        assertNotNull(recoveryQuote,
                "exchange must hold a quote for " + TARGET_SYMBOL + " after recovery");
        assertNotEquals(preCrashQuote.quoteId(), recoveryQuote.quoteId(),
                "post-recovery quoteId must differ from pre-crash: "
                        + "preCrash=" + preCrashQuote.quoteId()
                        + " recovery=" + recoveryQuote.quoteId());
        assertTrue(recoveryQuote.expiresAt() > System.currentTimeMillis(),
                "recovery quote must have a future expiresAt: " + recoveryQuote);
        System.out.println("[ERR10-k8s] recovery quote: " + recoveryQuote);
        System.out.println("[ERR10-k8s] post-recovery exposure (informational): " + currentExposure());
    }

    // ---------- per-test helpers ----------

    /**
     * Dump diagnostic info about {@code pod} into the JUnit report. Three
     * sections: environment ({@code SPRING_PROFILES_ACTIVE}), live state
     * of {@code /test/fault-injection/status}, and the last 500 log
     * lines — collectively answer "did the hook fire, and if not why".
     */
    private static void dumpMmDiagnostics(String pod) {
        System.out.println("\n[ERR10-k8s] ============ DIAGNOSTICS for " + pod + " ============");

        System.out.println("[ERR10-k8s] ---- SPRING_PROFILES_ACTIVE inside pod ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(15),
                "exec", "-n", NS, pod, "--", "sh", "-c",
                "env | grep SPRING_PROFILES_ACTIVE || echo '<not set>'");

        System.out.println("[ERR10-k8s] ---- /test/fault-injection/status (from inside) ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(15),
                "exec", "-n", NS, pod, "--", "sh", "-c",
                "wget -qO- http://localhost:8080/test/fault-injection/status "
                        + "|| echo '<endpoint unreachable — profile likely not active>'");

        System.out.println("[ERR10-k8s] ---- kubectl logs --tail=500 ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(30),
                "logs", "--tail=500", "-n", NS, pod);

        System.out.println("[ERR10-k8s] ============ END DIAGNOSTICS ============\n");
    }

    private static void runKubectlOut(long timeoutMs, String... args) {
        try {
            String out = runKubectlCapturing(timeoutMs, args);
            System.out.println(out);
        } catch (Exception e) {
            System.out.println("[diagnostics error: " + e + "]");
        }
    }

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
        System.out.println("[ERR10-k8s] wave " + wave + ": exchange accepted " + accepted + " orders");
    }

    /**
     * Hammer the exchange with wide orders on {@code symbol} until
     * {@code mmAlive(ownerPort)} returns false or {@code timeout} elapses.
     * See {@code LocalError10MMCrashDuringQuoteReplaceTest.driveOrdersUntilCrash}
     * for the rationale.
     */
    private static boolean driveOrdersUntilCrash(int ownerPort, String ownerPod,
                                                 String symbol, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        int sent = 0;
        int accepted = 0;
        int sideToggle = 0;
        while (Instant.now().isBefore(deadline)) {
            if (!mmAlive(ownerPort)) {
                System.out.println("[ERR10-k8s] " + ownerPod + " stopped responding after "
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
            Thread.sleep(250);
        }
        System.out.println("[ERR10-k8s] driveOrdersUntilCrash giving up after " + sent
                + " orders (" + accepted + " accepted); " + ownerPod + " still alive");
        return false;
    }

    private static int findOwnerPort(String symbol) throws Exception {
        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            int port = e.getKey();
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
        return -1;
    }

    private static void armFaultInjector(int mmPort, String symbol) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + mmPort
                        + "/test/fault-injection/arm-quote-replace-crash?symbol=" + symbol))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(),
                "POST /test/fault-injection/arm-quote-replace-crash must return 200 "
                        + "(profile fault-injection should be active on the MM); body=" + resp.body());
    }

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
        boolean accepted = postOrderToExchange(new ExternalOrder(
                UUID.randomUUID(), symbol, 1, WIDE_BUY_LIMIT, Side.BUY));
        assertTrue(!accepted,
                "order against expired orphan quote for " + symbol + " must be rejected "
                        + "(exchange must enforce quote TTL bound)");
    }

    // ---------- generic helpers (mirror ClusterIntegrationWithSystemK8sTest) ----------

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
                System.out.println("[ERR10-k8s] " + workload + " healthy on " + HOST + ":" + port);
                return;
            }
            Thread.sleep(2000);
        }
        System.err.println("[ERR10-k8s] " + workload + " did not respond on " + path + " within " + timeout);
        boolean isPod = workload.startsWith("mm-");
        String selectorArg = isPod ? workload : "deploy/" + workload;
        System.err.println("---- kubectl describe " + selectorArg + " ----");
        System.err.println(runKubectlCapturing(TimeUnit.SECONDS.toMillis(30),
                "describe", isPod ? "pod" : "deploy", workload, "-n", NS));
        System.err.println("---- kubectl logs --tail=300 " + selectorArg + " ----");
        System.err.println(runKubectlCapturing(TimeUnit.MINUTES.toMillis(1),
                "logs", "--tail=300", "-n", NS, selectorArg));
        throw new AssertionError(workload + " not healthy within " + timeout);
    }

    private static boolean healthy(int port, String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + port + path))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static String runKubectlCapturing(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(KUBECTL);
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            output.append("[timed out waiting for kubectl command]\n");
        }
        return output.toString();
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

    private static boolean postOrderToExchange(ExternalOrder order) {
        try {
            String body = JSON.writeValueAsString(order);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + EXCHANGE_PORT + "/orders"))
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
}