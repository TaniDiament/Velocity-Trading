package edu.yu.marketmaker.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Side;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * k3s/Kubernetes variant of {@link ClusterIntegrationWithSystemTest}.
 *
 * Same end-to-end assertions, but talks to a running cluster via NodePorts
 * defined in {@code k8s/} instead of bringing up a docker-compose stack.
 *
 * Pre-conditions (run before invoking this test):
 *   1. Build the offline image bundle: {@code ./scripts/build-offline-bundle.sh}
 *   2. Import on every k3s node:        {@code sudo k3s ctr images import images.tar}
 *   3. Apply manifests:                 {@code kubectl apply -k k8s/}
 *   4. Ensure the MM StatefulSet uses the production-quote-generator profile
 *      (edit {@code k8s/market-maker.yaml} SPRING_PROFILES_ACTIVE before applying).
 *
 * Opt-in: {@code -Dcluster.k8s.it=true}.
 *
 * Tunables (system properties):
 *   - {@code cluster.k8s.host}      host that exposes NodePorts (default: localhost)
 *   - {@code cluster.k8s.namespace} k8s namespace (default: market-maker)
 *   - {@code kubectl}               kubectl binary path (default: kubectl)
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class ClusterIntegrationWithSystemK8sTest {

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

    // NodePorts defined in k8s/*.yaml.
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
    private static final ObjectMapper JSON = new ObjectMapper();

    @BeforeAll
    static void waitForStack() throws Exception {
        System.out.println("[E2E-k8s] host=" + HOST + " namespace=" + NS);

        awaitHealthy("trading-state",        TRADING_STATE_PORT, Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  Duration.ofMinutes(5));
        awaitHealthy("exchange",             EXCHANGE_PORT,      Duration.ofMinutes(5));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     Duration.ofMinutes(5));

        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            // mm pods don't expose /health; /marketmaker/status returns 200
            // once the Spring context is up and the pod has joined the cluster.
            awaitHealthy(e.getValue(), e.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }

        System.out.println("[E2E-k8s] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(4), ClusterIntegrationWithSystemK8sTest::allNodesConverged,
                "cluster did not converge within 4 minutes");
        System.out.println("[E2E-k8s] full stack up.");
    }

    /**
     * Seed quotes, drive orders, verify the loop closes: trading-state has
     * fills for every symbol, and the exchange eventually serves a quote
     * whose id was never issued by the publisher — i.e. a market-maker
     * wrote it back into the shared Hazelcast map.
     */
    @Test
    void ordersFlowThroughEntireSystemAndMarketMakersProduceQuotes() throws Exception {
        System.out.println("[E2E-k8s] seeding bootstrap quotes via external-publisher...");
        Set<UUID> bootstrapQuoteIds = new HashSet<>(seedQuotes(new ArrayList<>(SEED_SYMBOLS)));
        assertEquals(SEED_SYMBOLS.size(), bootstrapQuoteIds.size(),
                "publisher must return one quoteId per symbol");

        Set<String> symbolsWithFills = new TreeSet<>();
        Set<String> symbolsWithMmQuote = new TreeSet<>();

        Instant deadline = Instant.now().plus(Duration.ofMinutes(3));
        int wave = 0;
        while (Instant.now().isBefore(deadline)) {
            wave++;
            int accepted = submitOrders(new ArrayList<>(SEED_SYMBOLS), 25);
            System.out.println("[E2E-k8s] wave " + wave + ": exchange accepted " + accepted + " orders");

            for (String symbol : SEED_SYMBOLS) {
                if (!symbolsWithFills.contains(symbol) && hasNonZeroPosition(symbol)) {
                    symbolsWithFills.add(symbol);
                }
                if (!symbolsWithMmQuote.contains(symbol)) {
                    UUID currentId = currentExchangeQuoteId(symbol);
                    if (currentId != null && !bootstrapQuoteIds.contains(currentId)) {
                        symbolsWithMmQuote.add(symbol);
                    }
                }
            }

            if (symbolsWithFills.equals(SEED_SYMBOLS) && !symbolsWithMmQuote.isEmpty()) {
                break;
            }
            Thread.sleep(1500);
        }

        System.out.println("[E2E-k8s] symbols with fills: " + symbolsWithFills);
        System.out.println("[E2E-k8s] symbols with MM-generated quote in exchange: " + symbolsWithMmQuote);

        assertEquals(SEED_SYMBOLS, symbolsWithFills,
                "every seed symbol must have at least one fill in trading-state; "
                        + "proves external-publisher → exchange → trading-state wiring");
        assertFalse(symbolsWithMmQuote.isEmpty(),
                "at least one symbol must have a current exchange quote whose quoteId "
                        + "is not in the bootstrap set; proves a market-maker wrote a quote "
                        + "back via the shared Hazelcast quotes map. bootstrap ids="
                        + bootstrapQuoteIds);

        List<Fill> allFills = getAllFills();
        assertFalse(allFills.isEmpty(), "trading-state /state/fills returned no fills");

        Set<String> symbolsSeenInFills = new TreeSet<>();
        Map<String, Long> signedNetBySymbolFromFills = new TreeMap<>();
        Set<UUID> quoteIdsSeenInFills = new HashSet<>();
        for (Fill fill : allFills) {
            assertNotNull(fill.orderId(), "fill orderId must be present: " + fill);
            assertTrue(SEED_SYMBOLS.contains(fill.symbol()),
                    "fill symbol must be one of seed symbols: " + fill.symbol());
            assertNotNull(fill.side(), "fill side must be present: " + fill);
            assertTrue(fill.quantity() > 0, "fill quantity must be > 0: " + fill);
            assertTrue(fill.price() > 0.0, "fill price must be > 0: " + fill);
            assertNotNull(fill.quoteId(), "fill quoteId must be present: " + fill);
            assertTrue(fill.createdAt() > 0, "fill createdAt must be positive: " + fill);

            if (fill.side() == Side.BUY) {
                assertTrue(fill.price() >= 99.0,
                        "BUY fills must execute at/beyond the SELL limit (>=99.0): " + fill);
            } else {
                assertTrue(fill.price() <= 101.0,
                        "SELL fills must execute at/below the BUY limit (<=101.0): " + fill);
            }

            symbolsSeenInFills.add(fill.symbol());
            quoteIdsSeenInFills.add(fill.quoteId());
            long signed = fill.side() == Side.BUY ? fill.quantity() : -fill.quantity();
            signedNetBySymbolFromFills.merge(fill.symbol(), signed, Long::sum);
        }

        assertEquals(SEED_SYMBOLS, symbolsSeenInFills,
                "every seed symbol must appear in /state/fills at least once");
        assertEquals(SEED_SYMBOLS, signedNetBySymbolFromFills.keySet(),
                "net position must be derivable from fills for every seed symbol");
        assertTrue(quoteIdsSeenInFills.stream().anyMatch(id -> !bootstrapQuoteIds.contains(id)),
                "fills should include at least one market-maker quoteId not in bootstrap set");
    }

    // ---------- helpers ----------

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

    /**
     * Poll {@code GET /health} on {@code port} until 200; on timeout dump
     * {@code kubectl describe} and {@code kubectl logs} for the named workload
     * to make CI failures debuggable.
     */
    private static void awaitHealthy(String workload, int port, Duration timeout) throws Exception {
        awaitHealthy(workload, port, "/health", timeout);
    }

    private static void awaitHealthy(String workload, int port, String path, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port, path)) {
                System.out.println("[E2E-k8s] " + workload + " healthy on " + HOST + ":" + port);
                return;
            }
            Thread.sleep(2000);
        }
        System.err.println("[E2E-k8s] " + workload + " did not respond on /health within " + timeout);
        // For pods (mm-N) describe/logs the pod directly; otherwise treat as a Deployment.
        boolean isPod = workload.startsWith("mm-");
        String selectorArg = isPod ? workload : "deploy/" + workload;
        System.err.println("---- kubectl describe " + selectorArg + " ----");
        System.err.println(runKubectlCapturing(TimeUnit.SECONDS.toMillis(30),
                "describe", isPod ? "pod" : "deploy", workload, "-n", NS));
        System.err.println("---- kubectl logs --tail=300 " + selectorArg + " ----");
        System.err.println(runKubectlCapturing(TimeUnit.MINUTES.toMillis(1),
                "logs", "--tail=300", "-n", NS, isPod ? workload : "deploy/" + workload));
        System.err.println("---- end logs ----");
        throw new AssertionError(workload + " not healthy within " + timeout);
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

    private static boolean healthy(int port) {
        return healthy(port, "/health");
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

    private static int submitOrders(List<String> symbols, int count) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + PUBLISHER_PORT
                        + "/publisher/submit-orders?count=" + count))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("submit-orders returned " + resp.statusCode() + ": " + resp.body());
        }
        return Integer.parseInt(resp.body().trim());
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
        return JSON.copy()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(resp.body(), new TypeReference<List<Fill>>() {});
    }

    private static boolean hasNonZeroPosition(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + TRADING_STATE_PORT + "/positions/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return false;
            JsonNode node = JSON.readTree(resp.body());
            JsonNode pos = node.has("value") ? node.path("value") : node;
            if (pos.isMissingNode() || pos.isNull()) return false;
            JsonNode netQty = pos.path("netQuantity");
            return !netQty.isMissingNode() && netQty.asLong(0) != 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    private static UUID currentExchangeQuoteId(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + EXCHANGE_PORT + "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            JsonNode node = JSON.readTree(resp.body());
            String id = node.path("quoteId").asText(null);
            return id == null ? null : UUID.fromString(id);
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