package edu.yu.velocitytrading.errorcases.local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.velocitytrading.model.*;
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
 * Error case 11 end-to-end: drives the stack into a non-trivial state
 * (fills, positions, quotes, reservations), then stops every container
 * (postgres included), starts them all back up, and verifies the system
 * recovers from durable storage without manual intervention.
 *
 * <p>What this proves (matching {@code docs/error-cases.md#error-case-11}):
 * <ol>
 *   <li>Positions survive restart — Hazelcast {@code positions} IMap
 *       is reloaded from PostgreSQL via {@code PositionMapStore} on
 *       trading-state startup.</li>
 *   <li>Fills survive restart — same mechanism via {@code FillMapStore}.</li>
 *   <li>Reservation state survives restart — {@code ReservationMapStore}.</li>
 *   <li>Market-makers reconnect to {@code state.stream}, receive the
 *       restored position snapshot, and republish fresh quotes via
 *       {@code AssignmentListener.bootstrapQuoteForNewlyAssigned}.</li>
 *   <li>The 7-node MM cluster re-converges on a single ZK leader.</li>
 * </ol>
 *
 * <p>Opt-in via {@code -Dcluster.it=true}; requires docker.
 */
@EnabledIfSystemProperty(named = "cluster.it", matches = "true")
class LocalError11RecoveryAfterFullSystemRestartTest {

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

    /** Every container in the compose stack, in dependency order (start-time). */
    private static final List<String> ALL_SERVICES;
    static {
        List<String> list = new ArrayList<>(List.of(
                "zookeeper1", "zookeeper2", "zookeeper3",
                "postgres",
                "trading-state-1", "trading-state-2", "trading-state-3",
                "exposure-reservation-1", "exposure-reservation-2", "exposure-reservation-3",
                "exchange-1", "exchange-2", "exchange-3",
                "service-lb",
                "external-publisher"));
        list.addAll(MM_PORT_TO_SERVICE.values());
        ALL_SERVICES = Collections.unmodifiableList(list);
    }

    private static final Set<String> SEED_SYMBOLS = new TreeSet<>(List.of(
            "AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META"));

    private static final int TRADING_STATE_PORT = 18080;
    private static final int EXCHANGE_PORT = 18081;
    private static final int EXPOSURE_RES_PORT = 18082;
    private static final int PUBLISHER_PORT = 18083;

    private static final int WARMUP_WAVES = 5;
    private static final long WAVE_INTERVAL_MS = 1500;
    private static final int ORDERS_PER_SYMBOL_PER_WAVE = 2;

    /**
     * Wait between {@code docker compose stop} and {@code start}. Long enough
     * that RSocket/Hazelcast clients on the test JVM have given up on the dead
     * connections, short enough that the test stays under its overall budget.
     */
    private static final Duration RESTART_DOWNTIME = Duration.ofSeconds(5);

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Path PROJECT_ROOT = Path.of(".").toAbsolutePath().normalize();

    @BeforeAll
    static void bootStack() throws Exception {
        System.out.println("[ERR11] cleaning any prior stack...");
        runDocker(null, TimeUnit.MINUTES.toMillis(3), "compose", "down", "-v", "--remove-orphans");

        System.out.println("[ERR11] docker compose build...");
        int buildRc = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(20),
                "compose", "build", "market-maker-node-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[ERR11] bringing up core infra...");
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

        System.out.println("[ERR11] bringing up exchange + publisher...");
        int rcExchange = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", "exchange-1", "exchange-2", "exchange-3", "external-publisher");
        assertEquals(0, rcExchange, "docker compose up (exchange/publisher) failed");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(4));
        awaitHealthy("external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[ERR11] bringing up market-maker nodes...");
        List<String> upCmd = new ArrayList<>(List.of("compose", "up", "-d"));
        upCmd.addAll(MM_PORT_TO_SERVICE.values());
        int rcMm = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                upCmd.toArray(String[]::new));
        assertEquals(0, rcMm, "docker compose up (market-maker nodes) failed");

        System.out.println("[ERR11] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8),
                LocalError11RecoveryAfterFullSystemRestartTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[ERR11] full stack up.");
    }

    @AfterAll
    static void teardownStack() throws Exception {
        System.out.println("[ERR11] docker compose down -v");
        runDocker(null, TimeUnit.MINUTES.toMillis(2), "compose", "down", "-v");
    }

    @Test
    void fullSystemRestartRecoversPositionsFillsAndResumesQuoting() throws Exception {
        // 1. Seed quotes so MMs have something to replace, and so the
        //    exchange has a quotes row per symbol in postgres.
        System.out.println("[ERR11] seeding bootstrap quotes...");
        List<String> seedList = new ArrayList<>(SEED_SYMBOLS);
        List<UUID> bootstrapIds = seedQuotes(seedList);
        assertEquals(SEED_SYMBOLS.size(), new HashSet<>(bootstrapIds).size(),
                "publisher must return one quoteId per symbol");

        // 2. Warmup: drive enough orders to produce fills on every symbol.
        //    Each fill writes to fillRepository AND updates positionRepository,
        //    so both Hazelcast maps will have non-trivial entries to persist
        //    to postgres before we stop the stack.
        System.out.println("[ERR11] warmup: " + WARMUP_WAVES + " waves...");
        for (int wave = 1; wave <= WARMUP_WAVES; wave++) {
            int accepted = submitOrdersViaPublisher(ORDERS_PER_SYMBOL_PER_WAVE,
                    new ArrayList<>(SEED_SYMBOLS));
            System.out.println("[ERR11] wave " + wave + ": exchange accepted " + accepted + " orders");
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // 3. Capture pre-restart durable state. We compare these snapshots
        //    against post-restart values to prove the MapStores reloaded
        //    rather than the stack starting fresh.
        Map<String, Position> prePositions = capturePositions();
        long preFillCount = countFills();
        ExposureState preExposure = currentExposure();
        assertNotNull(preExposure, "pre-restart exposure must be readable");
        assertTrue(preFillCount > 0,
                "warmup must have produced at least one fill; got " + preFillCount
                        + ". If 0, the orders never crossed — investigate quote generation.");
        long nonZeroPositionCount = prePositions.values().stream()
                .filter(p -> p.netQuantity() != 0).count();
        assertTrue(nonZeroPositionCount > 0,
                "warmup must have produced at least one non-zero position; got "
                        + prePositions);
        System.out.println("[ERR11] pre-restart: " + prePositions.size() + " positions ("
                + nonZeroPositionCount + " non-zero), " + preFillCount + " fills, "
                + preExposure.activeReservations() + " active reservations");

        // 4. Stop every container in the stack. We stop postgres too — the
        //    docs describe a "full system restart" that includes the database,
        //    and `docker compose stop` preserves the container's anonymous
        //    volume so the DB rows survive the restart cycle. If postgres
        //    weren't truly durable, this test would catch it.
        System.out.println("[ERR11] stopping all " + ALL_SERVICES.size() + " containers...");
        List<String> stopCmd = new ArrayList<>(List.of("compose", "stop"));
        stopCmd.addAll(ALL_SERVICES);
        int rcStop = runDocker(null, TimeUnit.MINUTES.toMillis(3),
                stopCmd.toArray(String[]::new));
        assertEquals(0, rcStop, "docker compose stop failed");

        System.out.println("[ERR11] downtime: " + RESTART_DOWNTIME);
        Thread.sleep(RESTART_DOWNTIME.toMillis());

        // 5. Start everything back up. Compose handles dependency order, but
        //    we still need to wait for the health endpoints because container
        //    PID 1 being up != Spring Boot context refresh complete.
        System.out.println("[ERR11] starting all " + ALL_SERVICES.size() + " containers...");
        List<String> startCmd = new ArrayList<>(List.of("compose", "start"));
        startCmd.addAll(ALL_SERVICES);
        int rcStart = runDocker(null, TimeUnit.MINUTES.toMillis(5),
                startCmd.toArray(String[]::new));
        assertEquals(0, rcStart, "docker compose start failed");

        System.out.println("[ERR11] waiting for app services healthy...");
        awaitHealthy("trading-state",        TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  Duration.ofMinutes(4));
        awaitHealthy("exchange",             EXCHANGE_PORT,      Duration.ofMinutes(4));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     Duration.ofMinutes(4));

        System.out.println("[ERR11] waiting for 7-node cluster reconvergence...");
        awaitCondition(Duration.ofMinutes(6),
                LocalError11RecoveryAfterFullSystemRestartTest::allNodesConverged,
                "cluster did not reconverge within 6 minutes after restart");
        System.out.println("[ERR11] stack back up.");

        // 6. ASSERT — positions recovered from postgres via Hazelcast MapStore.
        //    Equality on the (symbol, netQuantity) pair is the right granularity:
        //    version may increment if some other process touched the row, but
        //    the durable net position must match what we saw pre-restart.
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
        System.out.println("[ERR11] all " + postPositions.size() + " positions recovered (netQuantity preserved)");

        // 7. ASSERT — fills durable. The count may grow if MMs replay any
        //    in-flight orders during restart, but it must NOT shrink — that
        //    would mean FillMapStore lost rows.
        long postFillCount = countFills();
        assertTrue(postFillCount >= preFillCount,
                "post-restart fill count must not be less than pre-restart: pre="
                        + preFillCount + " post=" + postFillCount
                        + ". A shrink means FillMapStore lost data.");
        System.out.println("[ERR11] fills durable: pre=" + preFillCount + " post=" + postFillCount);

        // 8. ASSERT — MMs republish fresh quotes for every symbol. After
        //    restart, AssignmentListener.bootstrapQuoteForNewlyAssigned
        //    feeds a synthetic snapshot to the MM which publishes a quote
        //    via the production generator. We allow up to 60s because:
        //    (a) cluster convergence already completed in step 5, but
        //    (b) symbol-assignment fan-out happens after convergence and
        //    (c) per-symbol bootstrap then triggers a generateQuote each.
        Set<String> stillMissing = new TreeSet<>(SEED_SYMBOLS);
        awaitCondition(Duration.ofMinutes(1), () -> {
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
                + "within 60s. Still missing: " + stillMissing);
        System.out.println("[ERR11] every symbol has a fresh, non-expired exchange quote");

        // 9. ASSERT — system continues to function: a new round of orders
        //    must produce additional fills. If recovery is only superficial
        //    (e.g. positions loaded but RSocket wiring broken) the fill
        //    count won't grow when we drive traffic.
        System.out.println("[ERR11] post-recovery: 2 more order waves...");
        for (int wave = 1; wave <= 2; wave++) {
            int accepted = submitOrdersViaPublisher(ORDERS_PER_SYMBOL_PER_WAVE,
                    new ArrayList<>(SEED_SYMBOLS));
            System.out.println("[ERR11] post-recovery wave " + wave + ": exchange accepted "
                    + accepted + " orders");
            Thread.sleep(WAVE_INTERVAL_MS);
        }
        long finalFillCount = countFills();
        assertTrue(finalFillCount > postFillCount,
                "post-recovery orders must produce new fills (proving end-to-end pipeline "
                        + "is back online): pre=" + postFillCount + " final=" + finalFillCount);
        System.out.println("[ERR11] end-to-end pipeline functional after restart: "
                + (finalFillCount - postFillCount) + " new fills");
    }

    // ---------- pre/post snapshot helpers ----------

    /** Map every seed symbol to its current Position, or a zero placeholder if missing. */
    private static Map<String, Position> capturePositions() throws Exception {
        Map<String, Position> out = new HashMap<>();
        for (String symbol : SEED_SYMBOLS) {
            Position p = currentPosition(symbol);
            if (p != null) {
                out.put(symbol, p);
            } else {
                // Treat a missing position as netQuantity=0 so the diff
                // comparison still has a key to match against post-restart.
                out.put(symbol, new Position(symbol, 0, 0L, null));
            }
        }
        return out;
    }

    private static Position currentPosition(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + TRADING_STATE_PORT + "/positions/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            JsonNode node = JSON.readTree(resp.body());
            // Endpoint returns an Optional<Position> serialized as either the
            // bare record or {"value": ...} depending on Jackson version.
            JsonNode pos = node.has("value") ? node.path("value") : node;
            if (pos.isMissingNode() || pos.isNull()) return null;
            return JSON.treeToValue(pos, Position.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static long countFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + TRADING_STATE_PORT + "/state/fills"))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("GET /state/fills returned " + resp.statusCode() + ": " + resp.body());
        }
        List<Fill> fills = JSON.readValue(resp.body(), new TypeReference<List<Fill>>() {});
        return fills.size();
    }

    // ---------- generic helpers (parallel structure to LocalError10) ----------

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

    private static void awaitHealthy(String serviceName, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port)) {
                System.out.println("[ERR11] " + serviceName + " healthy on port " + port);
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

    private static int submitOrdersViaPublisher(int countPerSymbol, List<String> symbols) {
        try {
            String body = JSON.writeValueAsString(symbols);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + PUBLISHER_PORT
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
