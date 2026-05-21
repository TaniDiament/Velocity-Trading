package edu.yu.velocitytrading.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.velocitytrading.model.ExposureState;
import edu.yu.velocitytrading.model.Fill;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error case 4 k3s variant: one MM pod misses a position update, survivors
 * keep serving, and the restored pod rejoins from the latest snapshot.
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class ClusterError4MMCrashBeforePositionUpdateTest {

    private static final String TAG = "ERR4-k8s";

    @BeforeAll
    static void waitForStack() throws Exception {
        waitForStack(TAG);
        driveTrafficUntilEverySymbolHasFill(Duration.ofMinutes(3));
    }

    @AfterAll
    static void restoreAfterClass() throws Exception {
        restoreServices(TAG);
    }

    @AfterEach
    void restoreServices() throws Exception {
        restoreServices(TAG);
    }

    @Test
    void killedMarketMakerIsEvictedAndSurvivorsReconverge() throws Exception {
        String observedSymbol = prepareCrashVictimSymbol();
        int victimPort = crashVictimPort();
        String victimPod = crashVictimPod();
        UUID preCrashQuoteId = awaitQuoteId(
                observedSymbol, Duration.ofSeconds(30));
        assertNotNull(preCrashQuoteId, "expected an active quote before crash");

        kill(victimPod);
        assertTrue(submitSyntheticFill(observedSymbol),
                "trading-state did not accept the position update while " + victimPod + " was down"
                        + (lastSyntheticFillFailure.isEmpty() ? "" : " — last response: " + lastSyntheticFillFailure));
        awaitCondition(Duration.ofMinutes(2),
                () -> survivorsConverged(
                        victimPort, MM_PORT_TO_POD.size() - 1),
                "survivors did not reconverge after scaling down " + victimPod);
        awaitCondition(Duration.ofSeconds(30),
                () -> currentExchangeQuoteId(observedSymbol) != null,
                "observed symbol became unquotable after node crash: " + observedSymbol);
    }

    @Test
    void restartedMarketMakerRejoinsAndPublishesFreshQuote() throws Exception {
        String observedSymbol = prepareCrashVictimSymbol();
        int victimPort = crashVictimPort();
        String victimPod = crashVictimPod();
        UUID preCrashQuoteId = awaitQuoteId(
                observedSymbol, Duration.ofSeconds(30));

        kill(victimPod);
        assertTrue(submitSyntheticFill(observedSymbol),
                "trading-state did not accept the position update while " + victimPod + " was down"
                        + (lastSyntheticFillFailure.isEmpty() ? "" : " — last response: " + lastSyntheticFillFailure));
        awaitCondition(Duration.ofMinutes(2),
                () -> survivorsConverged(
                        victimPort, MM_PORT_TO_POD.size() - 1),
                "survivors did not reconverge after scaling down " + victimPod);

        start(victimPod);
        awaitCondition(Duration.ofMinutes(5),
                ClusterError4MMCrashBeforePositionUpdateTest::allNodesConverged,
                "cluster did not reconverge after scaling " + victimPod + " back up");
        JsonNode status = mmStatusOrNull(victimPort);
        assertNotNull(status, "restored MM pod did not answer /marketmaker/status");

        awaitCondition(Duration.ofMinutes(2), () -> {
            try {
                submitOrders(List.of(observedSymbol), 5);
            } catch (Exception ignored) {
                // tolerate transient recovery churn
            }
            UUID after = currentExchangeQuoteId(observedSymbol);
            return after != null && !after.equals(preCrashQuoteId);
        }, "observed symbol quote did not refresh after MM pod restore: " + observedSymbol);
    }


    static final SortedMap<Integer, String> MM_PORT_TO_POD;
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

    static final Set<String> SEED_SYMBOLS = Collections.unmodifiableSet(
            new TreeSet<>(List.of("AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META")));

    static final int TRADING_STATE_PORT = 30180;
    static final int EXCHANGE_PORT = 30181;
    static final int EXPOSURE_RES_PORT = 30182;
    static final int PUBLISHER_PORT = 30183;

    private static final String TARGET_MM_POD = "mm-6";
    private static final int TARGET_MM_PORT = 30087;
    private static final int TARGET_MM_REPLICAS = 6;
    private static final int FULL_MM_REPLICAS = 7;
    private static final int HA_REPLICAS = 3;

    private static final String HOST = System.getProperty("cluster.k8s.host", "localhost");
    private static final String NS = System.getProperty("cluster.k8s.namespace", "market-maker");
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
    static void waitForStack(String tag) throws Exception {
        System.out.println("[" + tag + "] host=" + HOST + " namespace=" + NS);
        awaitHealthy(tag, "trading-state", TRADING_STATE_PORT, "/health", Duration.ofMinutes(5));
        awaitHealthy(tag, "exposure-reservation", EXPOSURE_RES_PORT, "/health", Duration.ofMinutes(5));
        awaitHealthy(tag, "exchange", EXCHANGE_PORT, "/health", Duration.ofMinutes(5));
        awaitHealthy(tag, "external-publisher", PUBLISHER_PORT, "/health", Duration.ofMinutes(5));
        for (Map.Entry<Integer, String> mm : MM_PORT_TO_POD.entrySet()) {
            awaitHealthy(tag, mm.getValue(), mm.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }
        awaitCondition(Duration.ofMinutes(8), ClusterError4MMCrashBeforePositionUpdateTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[" + tag + "] full stack up.");
    }

    static void restoreServices(String tag) throws Exception {
        scaleWorkload("sts/exposure-reservation", HA_REPLICAS);
        awaitHealthy(tag, "exposure-reservation", EXPOSURE_RES_PORT, "/health", Duration.ofMinutes(5));
        scaleWorkload("sts/exchange", HA_REPLICAS);
        awaitHealthy(tag, "exchange", EXCHANGE_PORT, "/health", Duration.ofMinutes(5));
        scaleMmStatefulSet(FULL_MM_REPLICAS);
        awaitHealthy(tag, TARGET_MM_POD, TARGET_MM_PORT, "/marketmaker/status", Duration.ofMinutes(5));
        awaitCondition(Duration.ofMinutes(5), ClusterError4MMCrashBeforePositionUpdateTest::allNodesConverged,
                "cluster did not reconverge after restoring services");
    }

    static void driveTrafficUntilEverySymbolHasFill(Duration timeout) throws Exception {
        Set<UUID> bootstrapIds = new HashSet<>(seedQuotes(new ArrayList<>(SEED_SYMBOLS)));
        if (bootstrapIds.isEmpty()) fail("seed-quotes returned no bootstrap ids");

        Set<String> symbolsWithFills = new TreeSet<>();
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            submitOrders(new ArrayList<>(SEED_SYMBOLS), 25);
            for (Fill fill : getAllFills()) {
                if (SEED_SYMBOLS.contains(fill.symbol())) {
                    symbolsWithFills.add(fill.symbol());
                }
            }
            if (symbolsWithFills.equals(SEED_SYMBOLS)) return;
            Thread.sleep(1500);
        }
        throw new AssertionError("baseline traffic did not produce fills for every symbol within "
                + timeout + "; got=" + symbolsWithFills);
    }

    static int crashVictimPort() {
        return TARGET_MM_PORT;
    }

    static String crashVictimPod() {
        return TARGET_MM_POD;
    }

    static String prepareCrashVictimSymbol() throws Exception {
        ensureTargetIsWorker();
        String symbol = awaitTargetSymbol();
        if (symbol == null) {
            throw new AssertionError(TARGET_MM_POD + " owns no symbols after convergence");
        }
        return symbol;
    }

    static void kill(String service) throws Exception {
        switch (service) {
            case TARGET_MM_POD -> scaleMmStatefulSet(TARGET_MM_REPLICAS);
            case "exposure-reservation" -> scaleWorkload("sts/exposure-reservation", 0);
            case "exchange" -> scaleWorkload("sts/exchange", 0);
            default -> throw new IllegalArgumentException("unsupported k8s kill target: " + service);
        }
    }

    static void stop(String service) throws Exception {
        kill(service);
    }

    static void start(String service) throws Exception {
        switch (service) {
            case TARGET_MM_POD -> scaleMmStatefulSet(FULL_MM_REPLICAS);
            case "exposure-reservation" -> scaleWorkload("sts/exposure-reservation", HA_REPLICAS);
            case "exchange" -> scaleWorkload("sts/exchange", HA_REPLICAS);
            default -> throw new IllegalArgumentException("unsupported k8s start target: " + service);
        }
    }

    static boolean allNodesConverged() {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_POD.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String leaderId = status.path("leaderId").asText(null);
            if (leaderId == null) return false;
            leaders.add(leaderId);
            if (status.path("members").size() != MM_PORT_TO_POD.size()) return false;
        }
        return responding == MM_PORT_TO_POD.size() && leaders.size() == 1;
    }

    static boolean survivorsConverged(int excludedPort, int expectedMembers) {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_POD.keySet()) {
            if (port == excludedPort) continue;
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String leaderId = status.path("leaderId").asText(null);
            if (leaderId == null) return false;
            leaders.add(leaderId);
            if (status.path("members").size() != expectedMembers) return false;
        }
        return responding == expectedMembers && leaders.size() == 1;
    }

    static JsonNode mmStatusOrNull(int port) {
        return jsonOrNull(port, "/marketmaker/status");
    }

    static UUID awaitQuoteId(String symbol, Duration timeout) {
        UUID[] out = new UUID[1];
        awaitCondition(timeout, () -> {
            out[0] = currentExchangeQuoteId(symbol);
            return out[0] != null;
        }, "quote did not appear for symbol " + symbol + " within " + timeout);
        return out[0];
    }

    static ExposureState awaitExposure(Duration timeout) {
        ExposureState[] out = new ExposureState[1];
        awaitCondition(timeout, () -> {
            out[0] = currentExposure();
            return out[0] != null;
        }, "exposure did not become reachable within " + timeout);
        return out[0];
    }

    static boolean healthy(int port) {
        return healthy(port, "/health");
    }

    static int submitOrders(List<String> symbols, int count) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri(PUBLISHER_PORT, "/publisher/submit-orders?count=" + count))
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

    /**
     * Inject a Fill via the {@code trading-state} NodePort to simulate the
     * position update that error case 4 says arrives while the target MM is
     * down. The NodePort fans out across the 3 trading-state replicas and
     * {@link edu.yu.velocitytrading.ha.LeaderGuardFilter} 503-rejects mutating
     * writes on the two standbys, so a single-shot POST has only ~1/3 chance
     * of landing on the leader. Retry past the standbys (each retry re-rolls
     * the iptables hash) until one attempt is accepted or the budget runs
     * out; surface the last observed status / body on the assertion side so
     * a real failure isn't masked as "trading-state was busy."
     */
    static boolean submitSyntheticFill(String symbol) {
        lastSyntheticFillFailure = "";
        try {
            Map<String, Object> body = Map.of(
                    "orderId", UUID.randomUUID().toString(),
                    "symbol", symbol,
                    "side", "BUY",
                    "quantity", 1,
                    "price", 100.0,
                    "quoteId", UUID.randomUUID().toString(),
                    "createdAt", System.currentTimeMillis()
            );
            String payload = JSON.writeValueAsString(body);
            Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
            int attempts = 0;
            while (Instant.now().isBefore(deadline)) {
                attempts++;
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(uri(TRADING_STATE_PORT, "/state/fills"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();
                HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) return true;
                // 503 is the expected leader-guard rejection while we keep
                // re-rolling the NodePort hash; record any other status for
                // diagnostics in case the failure is real (e.g. 400 from a
                // malformed payload).
                lastSyntheticFillFailure = "HTTP " + resp.statusCode() + " " + resp.body()
                        + " (attempt " + attempts + ")";
                Thread.sleep(150);
            }
            return false;
        } catch (Exception e) {
            lastSyntheticFillFailure = "EXCEPTION " + e;
            return false;
        }
    }

    private static volatile String lastSyntheticFillFailure = "";

    static UUID currentExchangeQuoteId(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(EXCHANGE_PORT, "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            String raw = JSON.readTree(resp.body()).path("quoteId").asText(null);
            return raw == null ? null : UUID.fromString(raw);
        } catch (Exception e) {
            return null;
        }
    }

    static ExposureState currentExposure() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(EXPOSURE_RES_PORT, "/exposure"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readValue(resp.body(), ExposureState.class);
        } catch (Exception e) {
            return null;
        }
    }

    static void awaitCondition(Duration timeout, BooleanSupplier condition, String failureMessage) {
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

    private static void ensureTargetIsWorker() throws Exception {
        String leaderId = currentLeaderId();
        if (leaderId == null || !TARGET_MM_POD.equals(leaderId)) return;

        runKubectl(TimeUnit.SECONDS.toMillis(30),
                "delete", "pod", TARGET_MM_POD, "-n", NS, "--wait=true");
        awaitHealthy("cluster-support", TARGET_MM_POD, TARGET_MM_PORT,
                "/marketmaker/status", Duration.ofMinutes(5));
        awaitCondition(Duration.ofMinutes(2), () -> {
            String current = currentLeaderId();
            return current != null && !TARGET_MM_POD.equals(current);
        }, "after deleting " + TARGET_MM_POD + ", the cluster did not elect a different leader");
    }

    private static String awaitTargetSymbol() {
        Instant deadline = Instant.now().plus(Duration.ofMinutes(2));
        while (Instant.now().isBefore(deadline)) {
            JsonNode status = mmStatusOrNull(TARGET_MM_PORT);
            JsonNode assigned = status == null ? null : status.path("assigned");
            if (assigned != null && assigned.isArray() && !assigned.isEmpty()) {
                return assigned.get(0).asText();
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
            String leaderId = status.path("leaderId").asText(null);
            if (leaderId != null && !leaderId.isBlank()) return leaderId;
        }
        return null;
    }

    private static JsonNode clusterStatusOrNull(int port) {
        return jsonOrNull(port, "/cluster/status");
    }

    private static JsonNode jsonOrNull(int port, String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(port, path))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readTree(resp.body());
        } catch (Exception e) {
            return null;
        }
    }

    private static void awaitHealthy(String tag, String workload, int port,
                                     String path, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port, path)) {
                System.out.println("[" + tag + "] " + workload + " healthy on " + HOST + ":" + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(workload + " not healthy within " + timeout);
    }

    private static boolean healthy(int port, String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(port, path))
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
                .uri(uri(PUBLISHER_PORT, "/publisher/seed-quotes"))
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

    private static List<Fill> getAllFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri(TRADING_STATE_PORT, "/state/fills"))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("GET /state/fills returned " + resp.statusCode() + ": " + resp.body());
        }
        return JSON.readValue(resp.body(), new TypeReference<List<Fill>>() {});
    }

    private static URI uri(int port, String path) {
        return URI.create("http://" + HOST + ":" + port + path);
    }

    private static void scaleMmStatefulSet(int replicas) throws Exception {
        scaleWorkload("sts/mm", replicas);
    }

    private static void scaleWorkload(String workload, int replicas) throws Exception {
        runKubectl(TimeUnit.MINUTES.toMillis(2),
                "scale", workload, "-n", NS, "--replicas=" + replicas);
    }

    private static String runKubectl(long timeoutMs, String... args) throws Exception {
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
