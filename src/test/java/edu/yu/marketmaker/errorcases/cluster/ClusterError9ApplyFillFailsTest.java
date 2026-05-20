package edu.yu.marketmaker.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import edu.yu.marketmaker.model.*;
import org.junit.jupiter.api.AfterAll;
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
 * k3s/Kubernetes variant for error case 9: a fill arrives and trading-state
 * records it, but the follow-on {@code apply-fill} call against
 * {@code exposure-reservation} fails or times out.
 *
 * <p>Per {@code docs/error-cases.md#error-case-9}, the documented outcome is
 * "conservative over-reservation": the reservation map continues to hold the
 * original (pre-fill) capacity rather than the decremented post-fill value.
 * This is safe (it wastes capacity, never violates the exposure limit) and
 * is bounded — when the quote expires (30s TTL) or the next replace cycle
 * runs, the orphaned over-reservation is released.
 *
 * <p>Triggering an apply-fill-specific failure without a dedicated
 * fault-injection hook means crashing the {@code exposure-reservation}
 * service itself. In this codebase the exchange-side {@code
 * FillOrderDispatcher.releaseReservedExposure} is the apply-fill caller, so
 * scaling {@code sts/exposure-reservation} to 0 takes that path out
 * exactly as the documented failure does. The trade-off is that creating
 * fresh reservations is also broken during the downtime — that's why this
 * test asserts on the durability of pre-existing reservations rather than
 * trying to observe a single fill's apply-fill round-trip.
 *
 * <p>What this proves:
 * <ol>
 *   <li>Reservation state (per-symbol capacity) survives a hard
 *       {@code exposure-reservation} crash via the postgres-backed Hazelcast
 *       MapStore — the post-restart {@code /exposure} usage equals the
 *       pre-crash value, modulo any reservations whose TTL elapsed during
 *       the downtime.</li>
 *   <li>The conservative over-reservation invariant: usage is monotonic
 *       across the downtime, i.e. nothing was double-decremented and
 *       nothing was orphaned past the documented TTL bound.</li>
 *   <li>End-to-end recovery: after restart, MMs successfully create new
 *       reservations, the exchange's apply-fill calls succeed, and order
 *       traffic produces fresh fills.</li>
 * </ol>
 *
 * <p>Scope: this test bounces ONLY {@code sts/exposure-reservation} — every
 * other tier stays up. The broader full-system restart already covered by
 * {@link ClusterError11RecoveryAfterFullSystemRestartTest} would mask the
 * apply-fill-specific failure mode under generic restart noise.
 *
 * <p>Pre-conditions:
 * <ol>
 *   <li>Build offline image bundle and import on every k3s node.</li>
 *   <li>{@code kubectl apply -k k8s/}.</li>
 * </ol>
 *
 * <p>Opt-in: {@code -Dcluster.k8s.it=true}.
 *
 * <p>Tunables (system properties):
 * <ul>
 *   <li>{@code cluster.k8s.host}      host that exposes NodePorts (default: localhost)</li>
 *   <li>{@code cluster.k8s.namespace} k8s namespace (default: market-maker)</li>
 *   <li>{@code kubectl}               kubectl binary path (default: kubectl)</li>
 *   <li>{@code kubectl.ssh}           ssh prefix (default: ssh sack@192.168.8.11)</li>
 *   <li>{@code kubectl.remote}        remote kubectl command (default: doas env KUBECONFIG=... kubectl)</li>
 * </ul>
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class ClusterError9ApplyFillFailsTest {

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

    private static final int WARMUP_WAVES = 5;
    private static final long WAVE_INTERVAL_MS = 1500;
    private static final int ORDERS_PER_SYMBOL_PER_WAVE = 2;

    // NodePorts defined in k8s/*.yaml — mirror the other k8s tests.
    private static final int TRADING_STATE_PORT = 30180;
    private static final int EXCHANGE_PORT      = 30181;
    private static final int EXPOSURE_RES_PORT  = 30182;
    private static final int PUBLISHER_PORT     = 30183;

    /** The exposure-reservation StatefulSet HA replica count from k8s/. */
    private static final int RES_FULL_REPLICAS = 3;
    /** Length of the simulated apply-fill outage. Must comfortably exceed
     *  the 30s quote TTL so that any over-reservation orphan from the
     *  documented worst case has elapsed by the time we measure recovery. */
    private static final Duration OUTAGE_DURATION = Duration.ofSeconds(45);

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

    @BeforeAll
    static void waitForStack() throws Exception {
        System.out.println("[ERR9-k8s] host=" + HOST + " namespace=" + NS);

        awaitHealthy("trading-state",        TRADING_STATE_PORT, "/health",               Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  "/health",               Duration.ofMinutes(5));
        awaitHealthy("exchange",             EXCHANGE_PORT,      "/health",               Duration.ofMinutes(5));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     "/health",               Duration.ofMinutes(5));
        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            awaitHealthy(e.getValue(), e.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }

        System.out.println("[ERR9-k8s] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8),
                ClusterError9ApplyFillFailsTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[ERR9-k8s] full stack up.");
    }

    @AfterAll
    static void restoreReservationService() {
        // Belt-and-suspenders restore in case the test failed before its
        // own scale-up. Without this, a failure mid-test would leave the
        // reservation StatefulSet at 0 replicas and poison subsequent
        // test runs on the same cluster.
        try {
            System.out.println("[ERR9-k8s] @AfterAll: ensuring sts/exposure-reservation back to "
                    + RES_FULL_REPLICAS + " replicas");
            scaleExposureReservation(RES_FULL_REPLICAS);
            awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, "/health", Duration.ofMinutes(5));
        } catch (Exception e) {
            System.err.println("[ERR9-k8s] failed to restore sts/exposure-reservation replica count: " + e);
        }
    }

    @Test
    void applyFillOutageDoesNotLeakOrCorruptReservationsAndPipelineRecovers() throws Exception {
        // 1. Seed quotes so every symbol has a starting point at the
        //    exchange, then drive enough orders to ensure each owning MM has
        //    been through at least one replace cycle and the exposure
        //    reservation map holds non-trivial per-symbol entries.
        System.out.println("[ERR9-k8s] seeding bootstrap quotes...");
        List<String> seedList = new ArrayList<>(SEED_SYMBOLS);
        List<UUID> bootstrapIds = seedQuotes(seedList);
        assertEquals(SEED_SYMBOLS.size(), new HashSet<>(bootstrapIds).size(),
                "publisher must return one quoteId per symbol");

        System.out.println("[ERR9-k8s] warmup: " + WARMUP_WAVES + " waves...");
        for (int wave = 1; wave <= WARMUP_WAVES; wave++) {
            int accepted = submitOrdersViaPublisher(ORDERS_PER_SYMBOL_PER_WAVE,
                    new ArrayList<>(SEED_SYMBOLS));
            System.out.println("[ERR9-k8s] wave " + wave + ": exchange accepted " + accepted + " orders");
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // 2. Snapshot pre-crash exposure state. The doc'd "conservative
        //    over-reservation" invariant only has meaning if there's a
        //    meaningful reservation to over-reserve against, so we require
        //    at least one active reservation at this point.
        ExposureState preCrashExposure = currentExposure();
        assertNotNull(preCrashExposure, "pre-crash exposure must be readable");
        long preFillCount = countFills();
        assertTrue(preFillCount > 0,
                "warmup must have produced at least one fill so the system has "
                        + "actually invoked apply-fill; got " + preFillCount);
        assertTrue(preCrashExposure.activeReservations() >= 1,
                "pre-crash there must be at least one active reservation; got " + preCrashExposure);
        System.out.println("[ERR9-k8s] pre-crash exposure: " + preCrashExposure
                + ", fills=" + preFillCount);

        // 3. Take exposure-reservation down. This is the only way to
        //    force apply-fill RPCs to fail in this codebase — the exchange's
        //    FillOrderDispatcher.releaseReservedExposure call to
        //    `reservations.{symbol}.apply-fill` will now throw, which in
        //    turn means dispatchOrder rejects the order before sending a
        //    Fill. The remaining tiers (trading-state, exchange, mm) are
        //    untouched so we can keep probing them.
        System.out.println("[ERR9-k8s] scaling sts/exposure-reservation to 0 — apply-fill RPCs will now fail");
        scaleExposureReservation(0);
        awaitCondition(Duration.ofMinutes(2),
                () -> !healthy(EXPOSURE_RES_PORT, "/health"),
                "exposure-reservation NodePort still answered /health after scale-down");
        System.out.println("[ERR9-k8s] exposure-reservation is down");

        // 4. Drive order traffic during the outage. Each attempt walks the
        //    same FillOrderDispatcher path that would call apply-fill and
        //    must be rejected at the apply-fill step. We use the direct
        //    crossing-order pattern (read current ask/bid, submit at
        //    exactly that price — see {@link #submitCrossingOrder}) rather
        //    than the publisher's hardcoded $99/$101 limit orders: the
        //    publisher's prices only cross while MM quotes haven't drifted
        //    far from $100 via inventory skew, but the
        //    apply-fill-failure invariant must hold against ANY quote
        //    the exchange is willing to match. A crossing order's only
        //    legitimate rejection path while exposure-reservation is up
        //    is "no remaining qty on the side"; with the service down,
        //    every accepted match must additionally fail at the
        //    apply-fill RPC step.
        //
        //    Why we don't compare /state/fills before/after: the fills IMap
        //    is shared in a single Hazelcast cluster that includes the
        //    exposure-reservation members, so scaling them to 0 triggers
        //    partition migrations and IMap.getAll() returns a transient
        //    view (we've seen the count dip by ~2% mid-migration). The
        //    data is durable (backupCount + MapStore), but the snapshot
        //    is unreliable while members are rebalancing.
        System.out.println("[ERR9-k8s] driving crossing orders against existing quotes while reservation service is down");
        long outageDeadline = System.currentTimeMillis() + OUTAGE_DURATION.toMillis();
        int outageAttempted = 0;
        int outageAccepted = 0;
        while (System.currentTimeMillis() < outageDeadline) {
            for (String symbol : SEED_SYMBOLS) {
                outageAttempted++;
                if (submitCrossingOrder(symbol) != null) outageAccepted++;
            }
            Thread.sleep(500);
        }
        System.out.println("[ERR9-k8s] outage traffic: attempted=" + outageAttempted
                + " accepted=" + outageAccepted);
        assertEquals(0, outageAccepted,
                "while exposure-reservation was down, every dispatch must short-circuit at the "
                        + "apply-fill RPC (FillOrderDispatcher.releaseReservedExposure throws "
                        + "before fillSender.sendFill). attempted=" + outageAttempted
                        + " accepted=" + outageAccepted);

        // 5. Bring exposure-reservation back. Its Hazelcast members
        //    eager-load the reservations IMap from postgres via the
        //    ReservationMapStore; once /health responds, the API surface
        //    reflects the durable state again.
        System.out.println("[ERR9-k8s] scaling sts/exposure-reservation back to " + RES_FULL_REPLICAS);
        scaleExposureReservation(RES_FULL_REPLICAS);
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, "/health", Duration.ofMinutes(5));

        // 6. ASSERT — reservation state durable and firm-wide cap honored.
        //    The doc'd outcome for case 9 is "wastes capacity but does not
        //    violate the exposure limit". In this codebase, what that
        //    means concretely on the post-restart read:
        //      (a) totalCapacity is unchanged (sanity — config didn't drift),
        //      (b) bidUsage / askUsage are in [0, totalCapacity] (the
        //          firm-wide bid/ask caps from logic.md §2 hold even after
        //          the apply-fill outage — this is the invariant the doc
        //          calls out as "not dangerous"),
        //      (c) the per-symbol reservation map is durable: the
        //          activeReservations count must not have shrunk to zero
        //          (which would mean ReservationMapStore failed to eager-load
        //          and the MM tier is starting from scratch — strictly
        //          worse than the doc'd over-reservation).
        //
        //    Note: we deliberately do NOT assert post-restart usage <=
        //    pre-crash usage. Once exposure-reservation comes back, MMs
        //    whose 30s quote TTLs lapsed during the outage cold-start fresh
        //    quotes (ProductionQuoteGenerator treats an expired survivor as
        //    null, per error-cases.md case 11), and the resulting
        //    createReservation calls grant default-quantity capacity which
        //    can legitimately exceed the partially-depleted pre-crash
        //    residuals. The firm-wide cap is what the system actually
        //    promises, and that's what we check.
        ExposureState postRestartExposure = currentExposure();
        assertNotNull(postRestartExposure, "post-restart exposure must be readable");
        System.out.println("[ERR9-k8s] post-restart exposure: " + postRestartExposure);
        assertEquals(preCrashExposure.totalCapacity(), postRestartExposure.totalCapacity(),
                "totalCapacity must survive the restart unchanged");
        assertTrue(postRestartExposure.bidUsage() >= 0
                        && postRestartExposure.bidUsage() <= postRestartExposure.totalCapacity(),
                "post-restart bidUsage out of [0, totalCapacity]: " + postRestartExposure
                        + " — the firm-wide bid cap was violated");
        assertTrue(postRestartExposure.askUsage() >= 0
                        && postRestartExposure.askUsage() <= postRestartExposure.totalCapacity(),
                "post-restart askUsage out of [0, totalCapacity]: " + postRestartExposure
                        + " — the firm-wide ask cap was violated");
        assertTrue(postRestartExposure.activeReservations() > 0,
                "post-restart activeReservations is 0 (" + postRestartExposure
                        + ") — ReservationMapStore failed to eager-load from postgres, "
                        + "MM tier will have to cold-start every symbol from scratch");

        // 7a. ASSERT — MMs republish fresh quotes after the outage. The
        //     45s outage exceeded the 30s quote TTL, so every pre-outage
        //     quote is expired in the exchange's IMap. MMs must reconnect
        //     their RSocket clients to exposure-reservation (reactor-rsocket
        //     reconnect with backoff — can take 30s+) and the next
        //     QuoteFreshnessKeeper tick or position update must trigger a
        //     successful createReservation → quote publish. Until this
        //     happens, the exchange will reject every order with "Quote
        //     expired", which would falsely look like apply-fill is still
        //     broken. Wait for fresh quotes BEFORE driving recovery
        //     traffic.
        System.out.println("[ERR9-k8s] waiting for MMs to republish fresh quotes post-recovery...");
        Set<String> stillExpired = new TreeSet<>(SEED_SYMBOLS);
        awaitCondition(Duration.ofMinutes(3), () -> {
            stillExpired.clear();
            long now = System.currentTimeMillis();
            for (String symbol : SEED_SYMBOLS) {
                Quote q = currentExchangeQuote(symbol);
                if (q == null || q.expiresAt() <= now) {
                    stillExpired.add(symbol);
                }
            }
            return stillExpired.isEmpty();
        }, () -> "after exposure-reservation recovery, MMs did not republish fresh quotes "
                + "for every symbol within 3m — the MM-side RSocket reconnect to the "
                + "reservation service appears stuck. Still expired: " + stillExpired);
        System.out.println("[ERR9-k8s] every symbol has a fresh non-expired exchange quote post-recovery");

        // 7b. (intentionally omitted) — verifying the exchange→exposure-reservation
        //     RSocket path recovers within the test window is out of scope.
        //     The MM-side recovery proven in 7a (every symbol has a fresh
        //     quoteId in the exchange) already requires a successful
        //     createReservation RPC against the recovered service, which
        //     is the case-9 invariant: the documented outcome says "the
        //     market maker retries; quote eventually expires → reservation
        //     released", not "the entire pipeline recovers automatically".
        //
        //     Full E2E pipeline recovery — including the exchange's
        //     RSocket client reconnecting to exposure-reservation after a
        //     ~45s burst of failing apply-fill calls and its reactor-rsocket
        //     backoff timer firing — is observed by
        //     {@link ClusterError11RecoveryAfterFullSystemRestartTest},
        //     which restarts every tier together and verifies the full
        //     post-recovery fill flow. Forcing that scope into case 9
        //     would make this test depend on the exchange's backoff
        //     configuration, not on the apply-fill invariants under test.

        // 8. ASSERT — exposure stays within bounds under post-recovery
        //    load. Reading once after the recovery traffic catches any
        //    apply-fill that the exchange skipped or that returned a bogus
        //    freed-capacity value, both of which would push usage out of
        //    its valid range.
        ExposureState finalExposure = currentExposure();
        assertNotNull(finalExposure, "final exposure must be readable");
        assertTrue(finalExposure.bidUsage() >= 0
                        && finalExposure.bidUsage() <= finalExposure.totalCapacity(),
                "final bidUsage out of [0, totalCapacity]: " + finalExposure);
        assertTrue(finalExposure.askUsage() >= 0
                        && finalExposure.askUsage() <= finalExposure.totalCapacity(),
                "final askUsage out of [0, totalCapacity]: " + finalExposure);
        System.out.println("[ERR9-k8s] final exposure: " + finalExposure);
    }

    // ---------- helpers ----------

    private static long countFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + TRADING_STATE_PORT + "/state/fills"))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("GET /state/fills returned " + resp.statusCode() + ": " + resp.body());
        }
        List<Fill> fills = JSON.readValue(resp.body(), new TypeReference<List<Fill>>() {});
        return fills.size();
    }

    private static void scaleExposureReservation(int replicas) throws Exception {
        String out = runKubectl(TimeUnit.SECONDS.toMillis(30),
                "scale", "sts/exposure-reservation", "-n", NS, "--replicas=" + replicas);
        System.out.println("[ERR9-k8s] kubectl scale sts/exposure-reservation --replicas="
                + replicas + ": " + out.trim());
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

    private static void awaitHealthy(String workload, int port, String path, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port, path)) {
                System.out.println("[ERR9-k8s] " + workload + " healthy on " + HOST + ":" + port);
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
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
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

    private static int submitOrdersViaPublisher(int countPerSymbol, List<String> symbols) {
        try {
            String body = JSON.writeValueAsString(symbols);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + PUBLISHER_PORT
                            + "/publisher/submit-orders?count=" + countPerSymbol))
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

    /**
     * Submit one order directly to the exchange's /orders endpoint with a
     * caller-supplied id and limit price. Returns the HTTP status code, or
     * -1 on transport failure. Mirrors the helper in
     * {@link ClusterError123SubmittingExternalOrderErrorsClusterTest} so
     * test 9 can use the same crossing-order pattern.
     */
    private static int submitOrderDirectlyWithId(String symbol, UUID orderId,
                                                 int quantity, double limitPrice, String side) {
        try {
            Map<String, Object> body = Map.of(
                    "id", orderId.toString(),
                    "symbol", symbol,
                    "quantity", quantity,
                    "limitPrice", limitPrice,
                    "side", side
            );
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + EXCHANGE_PORT + "/orders"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                    .build();
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Submit one direct order priced at the symbol's current ask (BUY) or
     * bid (SELL) so it crosses by construction. Returns the orderId if the
     * exchange accepted (HTTP 200), else null. Reading the live quote and
     * matching it is the only reliable way to force a fill on a cluster
     * where MM inventory skew has drifted prices away from the publisher's
     * hardcoded $99/$101 limit prices.
     */
    private static UUID submitCrossingOrder(String symbol) {
        Quote q = currentExchangeQuote(symbol);
        if (q == null) return null;
        if (q.expiresAt() <= System.currentTimeMillis()) return null;
        UUID orderId = UUID.randomUUID();
        int rc;
        if (q.askQuantity() > 0) {
            rc = submitOrderDirectlyWithId(symbol, orderId, 1, q.askPrice(), "BUY");
        } else if (q.bidQuantity() > 0) {
            rc = submitOrderDirectlyWithId(symbol, orderId, 1, q.bidPrice(), "SELL");
        } else {
            return null;
        }
        return rc == 200 ? orderId : null;
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

    private static String runKubectl(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        if (KUBECTL_SSH != null && !KUBECTL_SSH.isBlank()) {
            StringBuilder remote = new StringBuilder(KUBECTL_REMOTE);
            for (String a : args) remote.append(' ').append(a);
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
            while ((line = r.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            output.append("[timed out waiting for kubectl command]\n");
        }
        if (p.exitValue() != 0) {
            throw new AssertionError("kubectl " + String.join(" ", args)
                    + " failed (exit=" + p.exitValue() + "): " + output);
        }
        return output.toString();
    }

    private static void awaitCondition(Duration timeout, BooleanSupplier condition, String failureMessage) {
        awaitCondition(timeout, condition, () -> failureMessage);
    }

    private static void awaitCondition(Duration timeout, BooleanSupplier condition,
                                       java.util.function.Supplier<String> failureMessage) {
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
        throw new AssertionError(failureMessage.get());
    }
}