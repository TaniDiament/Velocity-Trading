package edu.yu.velocitytrading.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.velocitytrading.model.Fill;
import edu.yu.velocitytrading.model.Quote;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * k3s/Kubernetes variant of
 * {@code edu.yu.velocitytrading.errorcases.local.SubmittingExternalOrderErrorsLocalTest}.
 *
 * <p>Same HA failover scenarios for "Submitting External Orders" (error cases
 * 1-3 in {@code docs/error-cases.md}), but run against a live k3s cluster
 * rather than a docker-compose stack. Crash injection is
 * {@code kubectl delete pod <pod>}: the StatefulSet recreates the pod while
 * the surviving replicas keep the system running.
 *
 * <ul>
 *   <li><b>Case 1</b> (exchange goes down): delete one exchange pod
 *       mid-traffic; the {@code exchange} Service routes to the survivors and
 *       orders keep filling. The deleted pod is recreated and rejoins.</li>
 *   <li><b>Case 2</b> (publisher retries): two waves of fresh-UUID orders
 *       produce independent fills — no dedup.</li>
 *   <li><b>Case 3</b> (trading-state goes down): delete one trading-state pod
 *       mid-traffic; Hazelcast replication + leader failover keep fills being
 *       recorded. The deleted pod is recreated and rejoins.</li>
 * </ul>
 *
 * <p>Pre-conditions (run before invoking this test):
 * <ol>
 *   <li>Build the offline image bundle and import it on every k3s node.</li>
 *   <li>{@code kubectl apply -k k8s/}; wait for all pods Ready.</li>
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
 *   <li>{@code kubectl.remote}        remote kubectl command
 *       (default: doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl)</li>
 * </ul>
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class ClusterError123SubmittingExternalOrderErrorsClusterTest {

    private static final Set<String> SEED_SYMBOLS = new TreeSet<>(List.of(
            "AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META"));

    // NodePorts defined in k8s/*.yaml — mirror the other k8s tests.
    private static final int TRADING_STATE_PORT = 30180;
    private static final int EXCHANGE_PORT      = 30181;
    private static final int PUBLISHER_PORT     = 30183;

    // StatefulSet pods targeted for crash injection (ordinal 0 of each tier).
    private static final String EXCHANGE_POD      = "exchange-0";
    private static final String TRADING_STATE_POD = "trading-state-0";

    private static final Duration FAILOVER_BUDGET = Duration.ofMinutes(2);

    private static final String HOST = System.getProperty("cluster.k8s.host", "localhost");
    private static final String NS   = System.getProperty("cluster.k8s.namespace", "market-maker");
    private static final String KUBECTL = System.getProperty("kubectl", "kubectl");

    // SSH the kubectl invocations to the control-plane node where KUBECONFIG
    // lives. Set -Dkubectl.ssh="" to run kubectl locally instead.
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
        System.out.println("[ERR1-3-k8s] host=" + HOST + " namespace=" + NS);
        awaitHealthy("trading-state",      TRADING_STATE_PORT, Duration.ofMinutes(5));
        awaitHealthy("exchange",           EXCHANGE_PORT,      Duration.ofMinutes(5));
        awaitHealthy("external-publisher", PUBLISHER_PORT,     Duration.ofMinutes(5));
        // Light baseline: confirm at least one new fill lands. Avoid the
        // grind-to-fills-on-every-symbol pattern from the local test —
        // on the cluster, MMs are active and one-sided baseline traffic
        // pushes positions into the exposure cap, leaving every quote with
        // qty=0 on one side and breaking subsequent random-price orders.
        awaitFirstFill(Duration.ofMinutes(2));
        System.out.println("[ERR1-3-k8s] stack up, order flow confirmed.");
    }

    @AfterEach
    void restoreFullStrength() throws Exception {
        // Each test deletes a pod; the StatefulSet recreates it. Wait for both
        // tiers to be fully rolled out so the next test starts at full strength.
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/exchange", "-n", NS, "--timeout=5m");
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/trading-state", "-n", NS, "--timeout=5m");
        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(3));
        awaitHealthy("exchange",      EXCHANGE_PORT,      Duration.ofMinutes(3));
    }

    // --- Error Case 1: an exchange pod goes down ---

    /**
     * md case 1: "Exchange goes down before handling the order — order is
     * lost, publisher retries with new order, no state corruption."
     *
     * <p>HA mapping: delete one exchange pod; surviving replicas keep serving
     * via the {@code exchange} Service. A publisher retry with a fresh UUID
     * (this test submits crossing orders with fresh UUIDs explicitly) must
     * land a fill — that fill's orderId must be one of OUR retried order ids,
     * proving the retry succeeded (not just that unrelated background traffic
     * happened to produce a fill).
     */
    @Test
    void exchangeReplicaFailoverKeepsOrdersFilling() throws Exception {
        Set<UUID> baselineFillOrderIds = collectFillOrderIds();
        assertNotNull(baselineFillOrderIds, "trading-state must be reachable before the test");

        deletePod(EXCHANGE_POD);

        Set<UUID> retriedOrderIds = submitRetriesUntilOneLands(
                baselineFillOrderIds, FAILOVER_BUDGET);

        Set<UUID> newFillOrderIds = collectFillOrderIds();
        newFillOrderIds.removeAll(baselineFillOrderIds);
        Set<UUID> retriesThatFilled = new HashSet<>(retriedOrderIds);
        retriesThatFilled.retainAll(newFillOrderIds);
        assertFalse(retriesThatFilled.isEmpty(),
                "no retried order produced a fill after exchange pod loss — "
                        + "md case 1's 'publisher retries with new order' is not honored. "
                        + "retried=" + retriedOrderIds + " newFillOrderIds=" + newFillOrderIds);
    }

    /**
     * A deleted exchange pod is recreated by the StatefulSet and the exchange
     * tier returns to full strength.
     */
    @Test
    void killedExchangeReplicaRejoinsCleanly() throws Exception {
        deletePod(EXCHANGE_POD);
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/exchange", "-n", NS, "--timeout=5m");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(3));
    }

    // --- Error Case 2: publisher retries with fresh ids (no duplicate fill) ---

    /**
     * md case 2: "Exchange crashes AFTER recording the fill but BEFORE
     * responding to the publisher — fill IS recorded, position is correct,
     * publisher retries with a new UUID, the new order produces a separate
     * fill (no dedup at the exchange)."
     *
     * <p>This test reproduces the spec invariants:
     * <ol>
     *   <li>Wave 1 (the "original" orders) is submitted with fresh UUIDs;
     *       we wait for those fills to actually land in trading-state before
     *       moving on, proving "fill IS recorded" (the precondition the md
     *       describes happening before the simulated crash).</li>
     *   <li>An exchange pod is deleted between waves — models the crash that
     *       the md says happens AFTER fills are recorded.</li>
     *   <li>Wave 2 (the "retry" wave) is submitted with brand-new UUIDs.
     *       Surviving exchange replicas accept them; trading-state must record
     *       independent fills for THOSE orderIds (no dedup against wave 1).</li>
     *   <li>The wave-1 fills survive the crash (durably recorded before the
     *       pod went down), and the wave-2 fills are recorded separately.</li>
     * </ol>
     */
    @Test
    void retriedOrdersWithFreshIdsProduceIndependentFills() throws Exception {
        Set<UUID> baselineFillOrderIds = collectFillOrderIds();

        // Wave 1: original publisher submission. Track each accepted orderId
        // so we can assert THESE specific fills land (and survive the crash).
        Set<UUID> wave1OrderIds = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            UUID id = submitCrossingOrder("AAPL");
            if (id != null) wave1OrderIds.add(id);
        }
        assertFalse(wave1OrderIds.isEmpty(),
                "wave 1 must accept at least one order to model the md scenario");

        // Wave 1 fills must be durably recorded BEFORE the simulated crash.
        // This is the md's "fill IS recorded" precondition.
        awaitCondition(Duration.ofSeconds(30), () -> {
            Set<UUID> seen = collectFillOrderIdsOrEmpty();
            seen.retainAll(wave1OrderIds);
            return !seen.isEmpty();
        }, "wave 1 produced no recorded fills before the simulated crash; "
                + "md case 2 requires the fill to be in trading-state first. "
                + "wave1=" + wave1OrderIds);

        // Models "exchange crashes after responding to trading-state but before
        // confirming to the publisher": kill an exchange pod between waves.
        deletePod(EXCHANGE_POD);

        // Wave 2: publisher retry with brand-new UUIDs. By construction these
        // share no orderId with wave 1 (UUID.randomUUID per call), so the
        // "no dedup" property is testable by orderId set comparison.
        Set<UUID> wave2OrderIds = new HashSet<>();
        Instant deadline = Instant.now().plus(FAILOVER_BUDGET);
        while (Instant.now().isBefore(deadline) && wave2OrderIds.size() < 3) {
            UUID id = submitCrossingOrder("AAPL");
            if (id != null) wave2OrderIds.add(id);
            else sleepQuietly(500);
        }
        assertFalse(wave2OrderIds.isEmpty(),
                "wave 2 (retry post-crash) must accept at least one order — "
                        + "surviving exchange replicas should keep serving");

        Set<UUID> intersect = new HashSet<>(wave1OrderIds);
        intersect.retainAll(wave2OrderIds);
        assertTrue(intersect.isEmpty(),
                "wave 1 and wave 2 must use disjoint UUIDs (fresh-UUID retry semantic): "
                        + intersect);

        // Wave 2 fills must land independently — exchange does not dedup.
        awaitCondition(Duration.ofSeconds(60), () -> {
            Set<UUID> seen = collectFillOrderIdsOrEmpty();
            seen.retainAll(wave2OrderIds);
            return !seen.isEmpty();
        }, "wave 2 (retry) produced no recorded fills — md case 2's "
                + "'no dedup at exchange, independent fill' property is violated. "
                + "wave2=" + wave2OrderIds);

        Set<UUID> newFillOrderIds = collectFillOrderIds();
        newFillOrderIds.removeAll(baselineFillOrderIds);

        Set<UUID> wave1Filled = new HashSet<>(newFillOrderIds);
        wave1Filled.retainAll(wave1OrderIds);
        assertFalse(wave1Filled.isEmpty(),
                "pre-crash wave 1 fills did not survive the exchange pod crash — "
                        + "md case 2 says the fill IS recorded before the crash. "
                        + "wave1=" + wave1OrderIds + " newFills=" + newFillOrderIds);

        Set<UUID> wave2Filled = new HashSet<>(newFillOrderIds);
        wave2Filled.retainAll(wave2OrderIds);
        assertFalse(wave2Filled.isEmpty(),
                "post-crash wave 2 fills not recorded — retry must produce a separate fill. "
                        + "wave2=" + wave2OrderIds + " newFills=" + newFillOrderIds);

        for (Fill f : getAllFills()) {
            if (wave1OrderIds.contains(f.orderId()) || wave2OrderIds.contains(f.orderId())) {
                assertEquals("AAPL", f.symbol(),
                        "test only submitted AAPL orders, but a tracked fill is for: " + f);
            }
        }
    }

    // --- Error Case 3: a trading-state pod goes down ---

    /**
     * md case 3: "Trading state crashes BEFORE recording the fill — exchange
     * thinks it succeeded (fire-and-forget RSocket), the fill is lost, the
     * quote was already decremented, position and quote are now inconsistent.
     * KNOWN GAP in the single-instance design."
     *
     * <p>HA mapping (what we actually test here): with 3 trading-state
     * replicas, the {@code fills} IMap is replicated across Hazelcast members
     * and the leader is resolved via Zookeeper on every exchange-side RSocket
     * send. Deleting one trading-state pod must not lose fills — the
     * surviving members keep accepting fills and the orderIds of orders
     * submitted during the failover window must appear in {@code /state/fills}.
     */
    @Test
    void tradingStateReplicaFailoverKeepsFillsRecorded() throws Exception {
        Set<UUID> baselineFillOrderIds = collectFillOrderIds();
        assertNotNull(baselineFillOrderIds, "trading-state must be reachable before the test");

        deletePod(TRADING_STATE_POD);

        Set<UUID> retriedOrderIds = submitRetriesUntilOneLands(
                baselineFillOrderIds, FAILOVER_BUDGET);

        Set<UUID> newFillOrderIds = collectFillOrderIds();
        newFillOrderIds.removeAll(baselineFillOrderIds);
        Set<UUID> retriesThatFilled = new HashSet<>(retriedOrderIds);
        retriesThatFilled.retainAll(newFillOrderIds);
        assertFalse(retriesThatFilled.isEmpty(),
                "orders submitted during trading-state pod loss produced no recorded fills — "
                        + "Hazelcast replication failed to mitigate md case 3. "
                        + "retried=" + retriedOrderIds + " newFillOrderIds=" + newFillOrderIds);
    }

    /**
     * A deleted trading-state pod is recreated by the StatefulSet and the
     * trading-state tier returns to full strength.
     */
    @Test
    void killedTradingStateReplicaRejoinsCleanly() throws Exception {
        deletePod(TRADING_STATE_POD);
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/trading-state", "-n", NS, "--timeout=5m");
        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(3));
    }

    // --- Sanity: a single pod loss does not take the tier down ---

    /**
     * Deleting one exchange pod must not make the exchange tier unreachable:
     * {@code GET /quotes/{symbol}} keeps returning a quote, served by a
     * surviving replica.
     */
    @Test
    void killingExchangeReplicaKeepsTheTierServingQuotes() throws Exception {
        reseedQuietly();
        awaitCondition(Duration.ofSeconds(30),
                () -> currentExchangeQuoteId("AAPL") != null,
                "AAPL quote did not appear in exchange after seed");

        deletePod(EXCHANGE_POD);

        awaitCondition(Duration.ofSeconds(60),
                () -> currentExchangeQuoteId("AAPL") != null,
                "exchange tier stopped serving /quotes/AAPL after one pod was deleted");
    }

    // ---------- crash injection ----------

    /** Delete {@code pod}; the StatefulSet recreates it under the same name. */
    private static void deletePod(String pod) throws Exception {
        System.out.println("[ERR1-3-k8s] kubectl delete pod " + pod);
        String out = runKubectl(TimeUnit.MINUTES.toMillis(2),
                "delete", "pod", pod, "-n", NS, "--wait=true");
        System.out.println("[ERR1-3-k8s]   " + out.trim());
    }

    // ---------- traffic ----------

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

    /**
     * Best-effort quote seed — swallows transient failures during a failover
     * window. Catches {@code Throwable} deliberately: {@link #seedQuotes}
     * reports a non-200 via JUnit {@code fail()}, which throws an
     * {@code AssertionError}; mid-failover that is expected, not a failure.
     */
    private static void reseedQuietly() {
        try {
            seedQuotes(new ArrayList<>(SEED_SYMBOLS));
        } catch (Throwable ignored) {
            // exchange/publisher may be momentarily unreachable mid-failover
        }
    }

    private static List<Fill> getAllFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + TRADING_STATE_PORT + "/state/fills"))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("GET /state/fills returned " + resp.statusCode() + ": " + resp.body());
        }
        return JSON.readValue(resp.body(), new TypeReference<List<Fill>>() {});
    }

    /**
     * Total recorded fill count, or -1 if trading-state is currently
     * unreachable. Catches {@code Throwable}: {@link #getAllFills} reports a
     * non-200 via JUnit {@code fail()} (an {@code AssertionError}), a tolerable
     * outcome mid-failover.
     */
    private static int fillCountOrMinusOne() {
        try {
            return getAllFills().size();
        } catch (Throwable t) {
            return -1;
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

    private static UUID currentExchangeQuoteId(String symbol) {
        Quote q = currentExchangeQuote(symbol);
        return q == null ? null : q.quoteId();
    }

    /**
     * Submit one order directly to the exchange with a caller-supplied id.
     * Returns the HTTP status code, or -1 on transport failure.
     */
    private static int submitOrderDirectlyWithId(String symbol, UUID orderId,
                                                 int quantity, double limitPrice, String side) {
        try {
            java.util.Map<String, Object> body = java.util.Map.of(
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
     * Submit one direct order against the symbol's current quote so it crosses
     * by construction: BUY at askPrice if askQty>0, SELL at bidPrice if
     * bidQty>0. The MM publishes position-aware, skewed quotes (and zeros one
     * side at the exposure cap), so reading the live quote and matching it is
     * the only reliable way to force a fill regardless of position state.
     *
     * @return the order id if the exchange accepted (HTTP 200), else null
     */
    private static UUID submitCrossingOrder(String symbol) {
        Quote q = currentExchangeQuote(symbol);
        if (q == null) return null;
        UUID orderId = UUID.randomUUID();
        int rc;
        if (q.askQuantity() > 0) {
            rc = submitOrderDirectlyWithId(symbol, orderId, 1, q.askPrice(), "BUY");
        } else if (q.bidQuantity() > 0) {
            rc = submitOrderDirectlyWithId(symbol, orderId, 1, q.bidPrice(), "SELL");
        } else {
            return null; // both sides at qty=0 — rare; nothing we can fill
        }
        return rc == 200 ? orderId : null;
    }

    /** One crossing order per seed symbol; returns count accepted. */
    private static int submitCrossingRound() {
        int n = 0;
        for (String s : SEED_SYMBOLS) {
            if (submitCrossingOrder(s) != null) n++;
        }
        return n;
    }

    /**
     * Baseline "system is healthy" check — submit crossing rounds until at
     * least one new fill lands. Lighter than grinding fills on every symbol;
     * just proves the order path is alive end-to-end before the test injects
     * a fault.
     */
    private static void awaitFirstFill(Duration timeout) {
        int before = fillCountOrMinusOne();
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            submitCrossingRound();
            int now = fillCountOrMinusOne();
            if (now > before) return;
            sleepQuietly(1500);
        }
        throw new AssertionError("baseline traffic produced no new fills within " + timeout);
    }

    /**
     * Set of orderIds for every fill currently recorded in trading-state.
     * Bails to {@code AssertionError} via {@link #getAllFills} on non-200.
     */
    private static Set<UUID> collectFillOrderIds() throws Exception {
        Set<UUID> out = new HashSet<>();
        for (Fill f : getAllFills()) out.add(f.orderId());
        return out;
    }

    /**
     * Like {@link #collectFillOrderIds} but never throws — used inside an
     * {@code awaitCondition} poll where trading-state may be transiently
     * unreachable mid-failover.
     */
    private static Set<UUID> collectFillOrderIdsOrEmpty() {
        try {
            return collectFillOrderIds();
        } catch (Throwable t) {
            return new HashSet<>();
        }
    }

    /**
     * Models a publisher retrying with a fresh UUID per the md case 1 and
     * case 3 scripts. Keep submitting crossing orders (each with a brand-new
     * UUID) until trading-state records one of OUR submitted orderIds as a
     * fill, then return the full set of orderIds the exchange accepted during
     * the retry loop. Rides out the failover window: the {@code exchange}
     * Service routes around the deleted pod within seconds, but transient
     * 5xx/connection errors during pod teardown are expected and tolerated.
     *
     * @throws AssertionError if no submitted orderId becomes a recorded fill
     *         before the timeout — i.e. failover did not let any retry through.
     */
    private static Set<UUID> submitRetriesUntilOneLands(
            Set<UUID> baselineFillOrderIds, Duration timeout) {
        Set<UUID> submitted = new HashSet<>();
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (String s : SEED_SYMBOLS) {
                UUID id = submitCrossingOrder(s);
                if (id != null) submitted.add(id);
            }
            Set<UUID> recorded = collectFillOrderIdsOrEmpty();
            recorded.removeAll(baselineFillOrderIds);
            recorded.retainAll(submitted);
            if (!recorded.isEmpty()) return submitted;
            sleepQuietly(2000);
        }
        throw new AssertionError("no submitted retry orderId was recorded as a fill within "
                + timeout + "; failover did not keep the order path alive. submitted=" + submitted);
    }

    // ---------- health ----------

    private static void awaitHealthy(String workload, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port)) {
                System.out.println("[ERR1-3-k8s] " + workload + " healthy on " + HOST + ":" + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(workload + " not healthy within " + timeout);
    }

    private static boolean healthy(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + port + "/health"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    // ---------- generic helpers ----------

    /**
     * Run kubectl either locally or via ssh to a control-plane node, mirroring
     * the convention in the {@code ClusterError*} tests. Returns combined
     * stdout+stderr; throws if kubectl exits non-zero.
     */
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
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) return;
            sleepQuietly(1000);
        }
        throw new AssertionError(failureMessage);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
