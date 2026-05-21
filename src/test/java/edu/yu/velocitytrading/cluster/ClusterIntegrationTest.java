package edu.yu.velocitytrading.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
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
 * End-to-end cluster test: builds the compose stack (3-node ZK ensemble + 7
 * market-maker nodes + postgres), waits for convergence, kills the elected
 * leader, and verifies that a new leader is elected and the dead node is
 * evicted from the members set.
 *
 * Opt-in: requires {@code -Dcluster.it=true} on the mvn command line and
 * docker running locally.
 */
@EnabledIfSystemProperty(named = "cluster.it", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterIntegrationTest {

    /** Host port -> compose service name, per compose.yml. */
    private static final SortedMap<Integer, String> PORT_TO_SERVICE;
    static {
        SortedMap<Integer, String> m = new TreeMap<>();
        m.put(8081, "market-maker-node-1");
        m.put(8082, "market-maker-node-2");
        m.put(8083, "market-maker-node-3");
        m.put(8084, "market-maker-node-4");
        m.put(8085, "market-maker-node-5");
        m.put(8086, "market-maker-node-6");
        m.put(8087, "market-maker-node-7");
        PORT_TO_SERVICE = Collections.unmodifiableSortedMap(m);
    }

    private static final Set<String> SEED_SYMBOLS = new TreeSet<>(List.of(
            "AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META"));

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path PROJECT_ROOT = Path.of(".").toAbsolutePath().normalize();

    @BeforeAll
    static void bootStack() throws Exception {
        System.out.println("[IT] cleaning any prior stack...");
        runDocker(TimeUnit.MINUTES.toMillis(2), "compose", "down", "-v", "--remove-orphans");

        System.out.println("[IT] docker compose build market-maker-node-1 (first run may take several minutes)...");
        int buildRc = runDocker(TimeUnit.MINUTES.toMillis(15),
                "compose", "build", "market-maker-node-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[IT] docker compose up -d (zk + postgres + trading-state)...");
        int rcCore = runDocker(TimeUnit.MINUTES.toMillis(5),
                "compose", "up", "-d",
                "zookeeper1", "zookeeper2", "zookeeper3", "postgres", "trading-state");
        assertEquals(0, rcCore, "docker compose up (core) failed");

        System.out.println("[IT] waiting for trading-state health...");
        awaitCondition(Duration.ofMinutes(2), ClusterIntegrationTest::tradingStateHealthy,
                "trading-state not healthy within 2 minutes");

        System.out.println("[IT] docker compose up -d (market-maker nodes)...");
        List<String> upCmd = new ArrayList<>(List.of("compose", "up", "-d"));
        upCmd.addAll(PORT_TO_SERVICE.values());
        int rc = runDocker(TimeUnit.MINUTES.toMillis(5), upCmd.toArray(String[]::new));
        assertEquals(0, rc, "docker compose up (market-maker nodes) failed");

        System.out.println("[IT] waiting for all 7 nodes to converge (leader elected, members=7)...");
        awaitCondition(Duration.ofMinutes(4), ClusterIntegrationTest::allNodesConverged,
                "cluster did not converge within 4 minutes");
        System.out.println("[IT] cluster up.");
    }

    @AfterAll
    static void teardownStack() throws Exception {
        System.out.println("[IT] docker compose down -v");
        runDocker(TimeUnit.MINUTES.toMillis(2), "compose", "down", "-v");
    }

    @Test
    @Order(1)
    void clusterConvergesOnSingleLeaderAndFullSymbolList() throws Exception {
        Map<Integer, JsonNode> statuses = statusFromEachNode(-1);
        assertEquals(PORT_TO_SERVICE.size(), statuses.size(), "not all nodes responded");

        Set<String> leadersSeen = new HashSet<>();
        Set<String> membersUnion = new HashSet<>();
        for (JsonNode s : statuses.values()) {
            leadersSeen.add(s.path("leaderId").asText(null));
            JsonNode m = s.path("members");
            for (int i = 0; i < m.size(); i++) membersUnion.add(m.get(i).asText());
        }
        assertEquals(1, leadersSeen.size(), "nodes disagree on leader: " + leadersSeen);
        assertNotNull(leadersSeen.iterator().next(), "leader is null");
        assertEquals(PORT_TO_SERVICE.size(), membersUnion.size(),
                "expected " + PORT_TO_SERVICE.size() + " members, got " + membersUnion);

        int leaderCount = 0;
        for (JsonNode s : statuses.values()) {
            if (s.path("leader").asBoolean(false)) leaderCount++;
        }
        assertEquals(1, leaderCount, "expected exactly one node to report leader=true");

        JsonNode symbolsNode = statuses.values().iterator().next().path("symbols");
        Set<String> symbols = new TreeSet<>();
        for (int i = 0; i < symbolsNode.size(); i++) symbols.add(symbolsNode.get(i).asText());
        assertEquals(SEED_SYMBOLS, symbols, "seed symbol list mismatch");
    }

    @Test
    @Order(2)
    void leaderFailoverElectsNewLeaderAndEvictsOld() throws Exception {
        Map<Integer, JsonNode> before = statusFromEachNode(-1);
        String oldLeader = before.values().iterator().next().path("leaderId").asText();

        int leaderPort = before.entrySet().stream()
                .filter(e -> oldLeader.equals(e.getValue().path("nodeId").asText()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "couldn't locate host port for leader nodeId=" + oldLeader));
        String leaderService = PORT_TO_SERVICE.get(leaderPort);

        System.out.println("[IT] killing leader service=" + leaderService + " (nodeId=" + oldLeader + ")");
        assertEquals(0, runDocker(TimeUnit.MINUTES.toMillis(1),
                        "compose", "kill", "-s", "SIGKILL", leaderService),
                "docker compose kill failed");

        awaitCondition(Duration.ofSeconds(90), () -> {
            Map<Integer, JsonNode> now = statusFromEachNode(leaderPort);
            if (now.size() != PORT_TO_SERVICE.size() - 1) return false;
            Set<String> leadersSeen = new HashSet<>();
            Set<String> membersUnion = new HashSet<>();
            for (JsonNode s : now.values()) {
                String lid = s.path("leaderId").asText(null);
                if (lid == null || lid.equals(oldLeader)) return false;
                leadersSeen.add(lid);
                JsonNode m = s.path("members");
                for (int i = 0; i < m.size(); i++) membersUnion.add(m.get(i).asText());
            }
            return leadersSeen.size() == 1
                    && !membersUnion.contains(oldLeader)
                    && membersUnion.size() == PORT_TO_SERVICE.size() - 1;
        }, "new leader not elected and old leader not evicted within 90s");

        Map<Integer, JsonNode> after = statusFromEachNode(leaderPort);
        String newLeader = after.values().iterator().next().path("leaderId").asText();
        System.out.println("[IT] new leader=" + newLeader + " (old=" + oldLeader + ")");
        assertTrue(!newLeader.equals(oldLeader), "new leader must differ from old");
    }

    /**
     * After failover, the new leader must subscribe to the trading-state
     * {@code state.stream} and forward each {@link edu.yu.velocitytrading.model.StateSnapshot}
     * over TCP to the single worker that owns the symbol — fire-and-forget,
     * with no ACK.
     *
     * We exercise this by posting one {@code Fill} per seed symbol to the
     * trading-state HTTP endpoint (which broadcasts a snapshot on
     * {@code state.stream}), then polling every surviving node's
     * {@code /marketmaker/status} endpoint. The {@code forwardsBySymbol}
     * counter there only increments inside {@code WorkerForwardReceiver},
     * so a nonzero delta is direct proof that the leader forwarded to that
     * node.
     *
     * Assertions:
     * <ul>
     *   <li>Every seed symbol lands on exactly one surviving node.</li>
     *   <li>That node is the one the leader's assignment maps the symbol to
     *       (per {@code /marketmaker/status}'s {@code assigned} set).</li>
     *   <li>The new leader receives no forwards (dedicated-leader mode).</li>
     * </ul>
     */
    @Test
    @Order(3)
    void leaderForwardsEachSymbolOverTcpToTheWorkerAssignedToIt() throws Exception {
        Map<Integer, JsonNode> statuses = statusFromEachNode(-1);
        assertEquals(PORT_TO_SERVICE.size() - 1, statuses.size(), "expected 6 survivors after failover");
        String leaderId = statuses.values().iterator().next().path("leaderId").asText();
        int leaderPort = statuses.entrySet().stream()
                .filter(e -> leaderId.equals(e.getValue().path("nodeId").asText()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no host port for new leader " + leaderId));
        Set<Integer> survivorPorts = new TreeSet<>(statuses.keySet());

        // Who owns what, according to each worker's own view. After failover
        // the coordinator rebalances znode-by-znode, so a symbol may be
        // momentarily claimed by its old and new owner simultaneously —
        // poll until the per-node assigned sets are mutually exclusive,
        // cover every seed symbol, and exclude the new leader.
        Map<String, Integer> ownerPortBySymbol = awaitStableAssignmentOwners(
                survivorPorts, leaderPort, Duration.ofSeconds(60));

        Map<Integer, Map<String, Long>> baseline = snapshotForwardCounts(survivorPorts);

        // Re-submit fills in a loop: the new leader's LeaderForwarder may not
        // have finished its state.stream subscription by the time the first
        // fill lands on trading-state. Each resubmit bumps the position
        // version, producing a fresh emission for any subscriber that has
        // since attached.
        System.out.println("[IT] submitting fills for seed symbols via trading-state...");
        Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
        boolean ok = false;
        while (Instant.now().isBefore(deadline)) {
            for (String symbol : SEED_SYMBOLS) {
                submitFill(symbol);
            }
            Map<String, Set<Integer>> receivers = receiversBySymbol(baseline, survivorPorts);
            if (receivers.size() == SEED_SYMBOLS.size()
                    && receivers.values().stream().allMatch(s -> s.size() == 1 && !s.contains(leaderPort))) {
                ok = true;
                break;
            }
            Thread.sleep(1000);
        }
        assertTrue(ok, "leader did not forward each symbol to exactly one non-leader worker");

        Map<String, Set<Integer>> receivers = receiversBySymbol(baseline, survivorPorts);
        System.out.println("[IT] per-symbol forward receiver ports: " + receivers
                + " (leaderPort=" + leaderPort + ", owners=" + ownerPortBySymbol + ")");

        assertEquals(SEED_SYMBOLS, receivers.keySet(),
                "every seed symbol must have been forwarded");
        for (String symbol : SEED_SYMBOLS) {
            Set<Integer> got = receivers.get(symbol);
            assertEquals(1, got.size(),
                    "exactly one worker should receive forwards for " + symbol + ": " + got);
            int gotPort = got.iterator().next();
            assertTrue(gotPort != leaderPort,
                    "leader must receive no forwards but received " + symbol);
            assertEquals(ownerPortBySymbol.get(symbol), (Integer) gotPort,
                    "forward for " + symbol + " went to port " + gotPort
                            + " but assignment owner is " + ownerPortBySymbol.get(symbol));
        }
    }

    // ---------- helpers ----------

    private static boolean allNodesConverged() {
        Map<Integer, JsonNode> statuses = statusFromEachNode(-1);
        if (statuses.size() != PORT_TO_SERVICE.size()) return false;
        Set<String> leaders = new HashSet<>();
        for (JsonNode s : statuses.values()) {
            String lid = s.path("leaderId").asText(null);
            if (lid == null) return false;
            if (s.path("members").size() != PORT_TO_SERVICE.size()) return false;
            leaders.add(lid);
        }
        return leaders.size() == 1;
    }

    /** Fetch {@code /cluster/status} from every node except {@code excludedPort}. Unreachable nodes are skipped. */
    private static Map<Integer, JsonNode> statusFromEachNode(int excludedPort) {
        Map<Integer, JsonNode> out = new LinkedHashMap<>();
        for (int port : PORT_TO_SERVICE.keySet()) {
            if (port == excludedPort) continue;
            try {
                out.put(port, status(port));
            } catch (Exception ignored) {
                // node not yet up, or just killed
            }
        }
        return out;
    }

    private static boolean tradingStateHealthy() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:18080/health"))
                    .timeout(Duration.ofSeconds(2))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Ask every surviving node for its own assigned symbol set, then invert
     * into symbol -> owning port. Returns null if the view is inconsistent
     * (a symbol claimed by two workers) — used by
     * {@link #awaitStableAssignmentOwners} to retry until the rebalance
     * settles.
     */
    private static Map<String, Integer> assignmentOwnersOrNull(Set<Integer> ports) {
        Map<String, Integer> out = new TreeMap<>();
        for (int port : ports) {
            JsonNode status = marketMakerStatusOrNull(port);
            if (status == null) return null;
            JsonNode assigned = status.path("assigned");
            if (!assigned.isArray()) return null;
            for (int i = 0; i < assigned.size(); i++) {
                String symbol = assigned.get(i).asText();
                Integer prior = out.put(symbol, port);
                if (prior != null && prior != port) {
                    return null;
                }
            }
        }
        return out;
    }

    /**
     * Poll {@code /marketmaker/status} across all survivors until their
     * assigned sets form a valid partition of {@link #SEED_SYMBOLS} and
     * exclude {@code leaderPort}.
     */
    private static Map<String, Integer> awaitStableAssignmentOwners(
            Set<Integer> survivorPorts, int leaderPort, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        Map<String, Integer> last = null;
        while (Instant.now().isBefore(deadline)) {
            Map<String, Integer> owners = assignmentOwnersOrNull(survivorPorts);
            last = owners;
            if (owners != null
                    && owners.keySet().equals(SEED_SYMBOLS)
                    && !owners.containsValue(leaderPort)) {
                return owners;
            }
            try { Thread.sleep(500); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("assignment did not stabilize within "
                + timeout + "; last view=" + last);
    }

    /** Snapshot per-symbol forward counts from /marketmaker/status on every port. */
    private static Map<Integer, Map<String, Long>> snapshotForwardCounts(Set<Integer> ports) {
        Map<Integer, Map<String, Long>> out = new LinkedHashMap<>();
        for (int port : ports) {
            out.put(port, forwardCountsOrEmpty(port));
        }
        return out;
    }

    /** For each seed symbol, the ports whose forwarded-frame count rose above {@code baseline}. */
    private static Map<String, Set<Integer>> receiversBySymbol(
            Map<Integer, Map<String, Long>> baseline, Set<Integer> ports) {
        Map<String, Set<Integer>> out = new TreeMap<>();
        for (int port : ports) {
            Map<String, Long> now = forwardCountsOrEmpty(port);
            Map<String, Long> before = baseline.getOrDefault(port, Map.of());
            for (String symbol : SEED_SYMBOLS) {
                long d = now.getOrDefault(symbol, 0L) - before.getOrDefault(symbol, 0L);
                if (d > 0) {
                    out.computeIfAbsent(symbol, k -> new TreeSet<>()).add(port);
                }
            }
        }
        return out;
    }

    private static Map<String, Long> forwardCountsOrEmpty(int port) {
        JsonNode status = marketMakerStatusOrNull(port);
        if (status == null) return Map.of();
        JsonNode map = status.path("forwardsBySymbol");
        if (!map.isObject()) return Map.of();
        Map<String, Long> out = new LinkedHashMap<>();
        map.properties().forEach(e -> out.put(e.getKey(), e.getValue().asLong()));
        return out;
    }

    private static JsonNode marketMakerStatusOrNull(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/marketmaker/status"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readTree(resp.body());
        } catch (Exception e) {
            return null;
        }
    }

    private static void submitFill(String symbol) throws IOException, InterruptedException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("orderId", UUID.randomUUID().toString());
        body.put("symbol", symbol);
        body.put("side", "BUY");
        body.put("quantity", 1);
        body.put("price", 100.0);
        body.put("quoteId", UUID.randomUUID().toString());
        body.put("createdAt", System.currentTimeMillis());
        String json = JSON.writeValueAsString(body);
        IOException last = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:18080/state/fills"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(30))
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();
                HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) return;
                last = new IOException("POST /state/fills for " + symbol + " returned " + resp.statusCode());
            } catch (java.net.http.HttpTimeoutException e) {
                last = new IOException("POST /state/fills for " + symbol + " timed out (attempt " + attempt + ")", e);
            }
            Thread.sleep(500);
        }
        throw last;
    }

    private static JsonNode status(int port) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/cluster/status"))
                .timeout(Duration.ofSeconds(3))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("non-200 from port " + port + ": " + resp.statusCode());
        }
        return JSON.readTree(resp.body());
    }

    private static int runDocker(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true)
                .inheritIO();
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
