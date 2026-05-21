package edu.yu.velocitytrading.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.velocitytrading.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
 * k3s/Kubernetes variant of
 * {@code edu.yu.velocitytrading.errorcases.local.LocalError11RecoveryAfterFullSystemRestartTest}.
 *
 * <p>Drives the cluster into a non-trivial state (fills, positions, quotes,
 * reservations), then {@code kubectl rollout restart}s every app-tier
 * StatefulSet/Deployment, and verifies the system recovers from durable
 * storage without manual intervention.
 *
 * <p>What this proves (matching {@code docs/error-cases.md#error-case-11}):
 * <ol>
 *   <li>Positions survive restart — {@code PositionMapStore} reloads from
 *       the postgres StatefulSet's PVC.</li>
 *   <li>Fills survive restart — {@code FillMapStore} reloads similarly.</li>
 *   <li>Market-makers reconnect to {@code state.stream}, receive the
 *       restored position snapshot, and republish fresh quotes via
 *       {@code AssignmentListener.bootstrapQuoteForNewlyAssigned}.</li>
 *   <li>The 7-pod MM StatefulSet re-converges on a single ZK leader.</li>
 * </ol>
 *
 * <p>This test deliberately does NOT restart {@code sts/zk} or
 * {@code sts/postgres}: they are the durable storage layer (per the error-case
 * 11 sequence diagram), and bouncing them adds large variance to the test
 * runtime without exercising different code paths than the local variant
 * already covers.
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
class ClusterError11RecoveryAfterFullSystemRestartTest {

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

    /**
     * App-tier workloads to bounce. Format: {@code "<kind>/<name>"}. ZK and
     * postgres are intentionally excluded — they're the durable layer and
     * survive in the documented scenario. headlamp is operational tooling,
     * not part of the system under test.
     */
    private static final List<String> RESTART_TARGETS = List.of(
            "sts/trading-state",
            "sts/exposure-reservation",
            "sts/exchange",
            "sts/mm",
            "deploy/external-publisher");

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
        System.out.println("[ERR11-k8s] host=" + HOST + " namespace=" + NS);

        awaitHealthy("trading-state",        TRADING_STATE_PORT, "/health",               Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  "/health",               Duration.ofMinutes(5));
        awaitHealthy("exchange",             EXCHANGE_PORT,      "/health",               Duration.ofMinutes(5));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     "/health",               Duration.ofMinutes(5));
        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            awaitHealthy(e.getValue(), e.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }

        System.out.println("[ERR11-k8s] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8),
                ClusterError11RecoveryAfterFullSystemRestartTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[ERR11-k8s] full stack up.");
    }

    @Test
    void fullSystemRestartRecoversPositionsFillsAndResumesQuoting() throws Exception {
        // 1. Seed quotes so MMs have something to replace, and so the
        //    exchange's quote rows are non-empty in postgres.
        System.out.println("[ERR11-k8s] seeding bootstrap quotes...");
        List<String> seedList = new ArrayList<>(SEED_SYMBOLS);
        List<UUID> bootstrapIds = seedQuotes(seedList);
        assertEquals(SEED_SYMBOLS.size(), new HashSet<>(bootstrapIds).size(),
                "publisher must return one quoteId per symbol");

        // 2. Warmup: drive enough orders to produce fills on every symbol so
        //    the position and fill IMaps are populated and persisted to
        //    postgres via MapStore.
        System.out.println("[ERR11-k8s] warmup: " + WARMUP_WAVES + " waves...");
        for (int wave = 1; wave <= WARMUP_WAVES; wave++) {
            int accepted = submitOrdersViaPublisher(ORDERS_PER_SYMBOL_PER_WAVE,
                    new ArrayList<>(SEED_SYMBOLS));
            System.out.println("[ERR11-k8s] wave " + wave + ": exchange accepted " + accepted + " orders");
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // 3. Capture pre-restart durable state — used as the baseline for
        //    the post-restart equality / monotonicity checks.
        Map<String, Position> prePositions = capturePositions();
        long preFillCount = countFills();
        ExposureState preExposure = currentExposure();
        assertNotNull(preExposure, "pre-restart exposure must be readable");
        assertTrue(preFillCount > 0,
                "warmup must have produced at least one fill; got " + preFillCount);
        long nonZeroPositionCount = prePositions.values().stream()
                .filter(p -> p.netQuantity() != 0).count();
        assertTrue(nonZeroPositionCount > 0,
                "warmup must have produced at least one non-zero position; got "
                        + prePositions);
        System.out.println("[ERR11-k8s] pre-restart: " + prePositions.size() + " positions ("
                + nonZeroPositionCount + " non-zero), " + preFillCount + " fills, "
                + preExposure.activeReservations() + " active reservations");

        // 4-5. Rolling-restart every app-tier workload, **one StatefulSet at
        //      a time**. `kubectl rollout restart` is non-blocking — firing
        //      all 5 targets in a row before any `rollout status` wait would
        //      put 4 different Hazelcast members (one per stateful tier) into
        //      simultaneous restart. With backupCount=1 a partition whose
        //      primary and backup happen to live on two of those bouncing
        //      pods loses both copies before migration completes, and any
        //      in-flight IMap.put whose MapStore.store hadn't committed dies
        //      with the JVM (fill sends are fire-and-forget over RSocket, so
        //      the exchange never learns the trading-state write was lost).
        //      Serializing — restart + wait, then move on — keeps at most one
        //      Hazelcast member bouncing at a time, which is what the
        //      backupCount=1 (now 2) replication can survive.
        //      zk + postgres are deliberately not in the list.
        for (String target : RESTART_TARGETS) {
            System.out.println("[ERR11-k8s] kubectl rollout restart " + target);
            String out = runKubectl(TimeUnit.SECONDS.toMillis(30),
                    "rollout", "restart", target, "-n", NS);
            System.out.println("[ERR11-k8s]   " + out.trim());

            System.out.println("[ERR11-k8s] kubectl rollout status " + target + " (waiting)...");
            String status = runKubectl(TimeUnit.MINUTES.toMillis(10),
                    "rollout", "status", target, "-n", NS, "--timeout=10m");
            System.out.println("[ERR11-k8s]   " + status.trim());
        }

        // 6. Same health gate as @BeforeAll — Spring context refresh must
        //    finish before our HTTP probes can rely on the endpoints.
        System.out.println("[ERR11-k8s] waiting for app services healthy after restart...");
        awaitHealthy("trading-state",        TRADING_STATE_PORT, "/health",               Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  "/health",               Duration.ofMinutes(5));
        awaitHealthy("exchange",             EXCHANGE_PORT,      "/health",               Duration.ofMinutes(5));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     "/health",               Duration.ofMinutes(5));
        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            awaitHealthy(e.getValue(), e.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }

        System.out.println("[ERR11-k8s] waiting for 7-node cluster reconvergence...");
        awaitCondition(Duration.ofMinutes(8),
                ClusterError11RecoveryAfterFullSystemRestartTest::allNodesConverged,
                "cluster did not reconverge within 8 minutes after restart");
        System.out.println("[ERR11-k8s] cluster reconverged.");

        // 7. ASSERT — positions recovered. Equality on netQuantity is the
        //    right granularity; version may increment if any in-flight
        //    snapshot replay touched the row, but the durable net must match.
        Map<String, Position> postPositions = capturePositions();
        assertEquals(prePositions.keySet(), postPositions.keySet(),
                "post-restart positions map must hold the same symbols as pre-restart");
        for (String symbol : prePositions.keySet()) {
            Position pre = prePositions.get(symbol);
            Position post = postPositions.get(symbol);
            assertEquals(pre.netQuantity(), post.netQuantity(),
                    "post-restart netQuantity for " + symbol + " must match pre-restart: "
                            + "pre=" + pre + " post=" + post
                            + ". A mismatch means PositionMapStore lost data.");
        }
        System.out.println("[ERR11-k8s] all " + postPositions.size()
                + " positions recovered (netQuantity preserved)");

        // 8. ASSERT — fills durable (count must not shrink).
        long postFillCount = countFills();
        assertTrue(postFillCount >= preFillCount,
                "post-restart fill count must not be less than pre-restart: pre="
                        + preFillCount + " post=" + postFillCount
                        + ". A shrink means FillMapStore lost data.");
        System.out.println("[ERR11-k8s] fills durable: pre=" + preFillCount
                + " post=" + postFillCount);

        // 9. ASSERT — MMs republish fresh quotes for every symbol.
        Set<String> stillMissing = new TreeSet<>(SEED_SYMBOLS);
        awaitCondition(Duration.ofMinutes(2), () -> {
            stillMissing.clear();
            long now = System.currentTimeMillis();
            for (String symbol : SEED_SYMBOLS) {
                Quote q = currentExchangeQuote(symbol);
                if (q == null || q.expiresAt() <= now) {
                    stillMissing.add(symbol);
                }
            }
            return stillMissing.isEmpty();
        }, () -> "after restart, MMs did not republish a non-expired quote for every symbol "
                + "within 2m. Still missing: " + stillMissing);
        System.out.println("[ERR11-k8s] every symbol has a fresh, non-expired exchange quote");

        // 10. ASSERT — end-to-end pipeline functional: post-recovery orders
        //     produce additional fills.
        System.out.println("[ERR11-k8s] post-recovery: 2 more order waves...");
        for (int wave = 1; wave <= 2; wave++) {
            int accepted = submitOrdersViaPublisher(ORDERS_PER_SYMBOL_PER_WAVE,
                    new ArrayList<>(SEED_SYMBOLS));
            System.out.println("[ERR11-k8s] post-recovery wave " + wave + ": exchange accepted "
                    + accepted + " orders");
            Thread.sleep(WAVE_INTERVAL_MS);
        }
        long finalFillCount = countFills();
        // On failure, ask the publisher for the first non-200 it saw — turns
        // an opaque "accepted=0" into a concrete exchange-side reason
        // (validation message, leader-guard 503, transport exception, ...).
        String publisherFailureSample = finalFillCount > postFillCount ? "" : lastPublisherFailureOrEmpty();
        assertTrue(finalFillCount > postFillCount,
                "post-recovery orders must produce new fills (proving end-to-end pipeline "
                        + "is back online): pre=" + postFillCount + " final=" + finalFillCount
                        + (publisherFailureSample.isEmpty()
                                ? ""
                                : " — publisher firstFailure: " + publisherFailureSample));
        System.out.println("[ERR11-k8s] end-to-end pipeline functional after restart: "
                + (finalFillCount - postFillCount) + " new fills");
    }

    // ---------- pre/post snapshot helpers ----------

    private static Map<String, Position> capturePositions() throws Exception {
        Map<String, Position> out = new HashMap<>();
        for (String symbol : SEED_SYMBOLS) {
            Position p = currentPosition(symbol);
            if (p != null) {
                out.put(symbol, p);
            } else {
                out.put(symbol, new Position(symbol, 0, 0L, null));
            }
        }
        return out;
    }

    private static Position currentPosition(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + TRADING_STATE_PORT + "/positions/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            JsonNode node = JSON.readTree(resp.body());
            JsonNode pos = node.has("value") ? node.path("value") : node;
            if (pos.isMissingNode() || pos.isNull()) return null;
            return JSON.treeToValue(pos, Position.class);
        } catch (Exception e) {
            return null;
        }
    }

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

    // ---------- generic helpers (mirror ClusterError10) ----------

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
                System.out.println("[ERR11-k8s] " + workload + " healthy on " + HOST + ":" + port);
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

    private static String lastPublisherFailureOrEmpty() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + PUBLISHER_PORT + "/publisher/last-failure"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return "<last-failure endpoint HTTP " + resp.statusCode() + ">";
            return resp.body();
        } catch (Exception e) {
            return "<last-failure endpoint threw: " + e + ">";
        }
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

    /**
     * Run kubectl either locally or via ssh to a control-plane node, mirroring
     * the convention in {@link ClusterError10MMCrashDuringQuoteReplaceTest}.
     * Returns combined stdout+stderr.
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
