package edu.yu.marketmaker.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.ExternalOrder;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.Side;
import org.junit.jupiter.api.AfterAll;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
 * End-to-end docker-compose system test. Mirrors {@link ClusterIntegrationWithSystemK8sTest}
 * but boots the stack via {@code docker compose} rather than reaching a running k3s
 * cluster.
 *
 * <p>Stack: postgres, zookeeper (3-node ensemble), trading-state, exposure-reservation,
 * exchange, external-publisher, and 7 market-maker nodes running the
 * {@code production-quote-generator} profile.
 *
 * <p>Order-submission strategy (matches the k8s variant): orders are POSTed directly
 * to the exchange — never through the publisher's bulk {@code /publisher/submit-orders}
 * endpoint — and are split per wave into:
 * <ul>
 *   <li>{@code SELF_CROSS_PAIRS_PER_WAVE} BUY/SELL pairs at {@link #SELF_CROSS_PRICE},
 *       priced inside the MM's spread so the two external orders match each other and
 *       leave MM exposure untouched.</li>
 *   <li>One "wide" order per wave whose side alternates, priced to cross the MM's
 *       quote — this keeps the MM engaged while balancing flow so net position
 *       oscillates around zero rather than running into the ±100 exposure cap and
 *       saturating the production quote generator.</li>
 * </ul>
 *
 * <p>Assertions:
 * <ul>
 *   <li>Every seed symbol records at least one fill in trading-state.</li>
 *   <li>Each symbol eventually has a quote in the exchange whose quoteId is not in
 *       the bootstrap set — proves the assigned MM wrote a fresh quote back via
 *       the shared Hazelcast {@code quotes} map.</li>
 *   <li>Each MM-generated quote is structurally valid (correct symbol, non-crossed,
 *       expected spread, sane mid, capped quantities).</li>
 * </ul>
 *
 * <p>Opt-in: {@code -Dcluster.it=true}; docker must be running locally.
 */
@EnabledIfSystemProperty(named = "cluster.it", matches = "true")
class ClusterIntegrationWithSystemTest {

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

    private static final int TRADING_STATE_PORT = 18080;
    private static final int EXCHANGE_PORT = 18081;
    private static final int EXPOSURE_RES_PORT = 18082;
    private static final int PUBLISHER_PORT = 18083;

    private static final int TOTAL_WAVES = 50;
    private static final long WAVE_INTERVAL_MS = 1500;
    private static final int SELF_CROSS_PAIRS_PER_WAVE = 5;
    private static final double SELF_CROSS_PRICE = 100.00;
    private static final double WIDE_BUY_LIMIT = 101.00;
    private static final double WIDE_SELL_LIMIT = 99.00;

    // Mirrors marketmaker.target-spread in application-market-maker-node.properties.
    private static final double EXPECTED_SPREAD = 0.10;
    private static final double SPREAD_TOLERANCE = 1e-3;
    private static final double MIN_REASONABLE_MID = 95.0;
    private static final double MAX_REASONABLE_MID = 105.0;
    private static final int MAX_QUANTITY_PER_SIDE = 200;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path PROJECT_ROOT = Path.of(".").toAbsolutePath().normalize();

    @BeforeAll
    static void bootStack() throws Exception {
        System.out.println("[E2E] cleaning any prior stack...");
        runDocker(null, TimeUnit.MINUTES.toMillis(3), "compose", "down", "-v", "--remove-orphans");

        System.out.println("[E2E] docker compose build (first run may take several minutes)...");
        int buildRc = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(20),
                "compose", "build", "market-maker-node-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[E2E] bringing up core infra (zk + postgres + trading-state + exposure-reservation)...");
        int rcCore = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                "compose", "up", "-d",
                "zookeeper1", "zookeeper2", "zookeeper3",
                "postgres", "trading-state", "exposure-reservation");
        assertEquals(0, rcCore, "docker compose up (core) failed");

        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(4));

        System.out.println("[E2E] bringing up exchange...");
        int rcExchange = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", "exchange");
        assertEquals(0, rcExchange, "docker compose up (exchange) failed");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(4));

        System.out.println("[E2E] bringing up external-publisher...");
        int rcPub = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", "external-publisher");
        assertEquals(0, rcPub, "docker compose up (external-publisher) failed");
        awaitHealthy("external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[E2E] bringing up market-maker nodes (production-quote-generator profile)...");
        List<String> upCmd = new ArrayList<>(List.of("compose", "up", "-d"));
        upCmd.addAll(MM_PORT_TO_SERVICE.values());
        int rcMm = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                upCmd.toArray(String[]::new));
        assertEquals(0, rcMm, "docker compose up (market-maker nodes) failed");

        System.out.println("[E2E] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(4), ClusterIntegrationWithSystemTest::allNodesConverged,
                "cluster did not converge within 4 minutes");
        System.out.println("[E2E] full stack up.");
    }

    @AfterAll
    static void teardownStack() throws Exception {
        System.out.println("[E2E] docker compose down -v");
        runDocker(null, TimeUnit.MINUTES.toMillis(2), "compose", "down", "-v");
    }

    @Test
    void ordersFlowThroughEntireSystemAndMarketMakersProduceQuotes() throws Exception {
        Map<String, List<TimedLine>> eventLog = new TreeMap<>();
        for (String s : SEED_SYMBOLS) {
            eventLog.put(s, new ArrayList<>());
        }
        Map<String, UUID> lastSeenQuoteIdBySymbol = new HashMap<>();
        Map<String, Set<UUID>> loggedQuoteIdsBySymbol = new HashMap<>();
        for (String s : SEED_SYMBOLS) {
            loggedQuoteIdsBySymbol.put(s, new HashSet<>());
        }

        // /state/fills returns the entire historical fill table; capture this
        // run's start so prior-run fills aren't folded into the per-symbol logs.
        long testStartMillis = System.currentTimeMillis() - 5_000;

        System.out.println("[E2E] seeding bootstrap quotes via external-publisher...");
        List<String> seedSymbolList = new ArrayList<>(SEED_SYMBOLS);
        List<UUID> bootstrapList = seedQuotes(seedSymbolList);
        Set<UUID> bootstrapQuoteIds = new HashSet<>(bootstrapList);
        assertEquals(SEED_SYMBOLS.size(), bootstrapQuoteIds.size(),
                "publisher must return one quoteId per symbol");

        long seedTime = System.currentTimeMillis();
        for (int i = 0; i < seedSymbolList.size(); i++) {
            String sym = seedSymbolList.get(i);
            UUID id = bootstrapList.get(i);
            Quote bq = currentExchangeQuote(sym);
            String detail = (bq != null && id.equals(bq.quoteId()))
                    ? formatQuoteFields(bq)
                    : "(details unavailable)";
            eventLog.get(sym).add(new TimedLine(seedTime,
                    "QUOTE [bootstrap] id=" + id + " " + detail));
            lastSeenQuoteIdBySymbol.put(sym, id);
            loggedQuoteIdsBySymbol.get(sym).add(id);
        }

        Set<String> symbolsWithFills = new TreeSet<>();
        Map<String, Quote> firstMmQuoteBySymbol = new TreeMap<>();
        Random rnd = new Random(0xC0FFEE);
        for (int wave = 1; wave <= TOTAL_WAVES; wave++) {
            long waveStart = System.currentTimeMillis();
            int accepted = 0;
            for (String symbol : SEED_SYMBOLS) {
                int pairsAccepted = 0;
                for (int i = 0; i < SELF_CROSS_PAIRS_PER_WAVE; i++) {
                    int qty = 1 + rnd.nextInt(3);
                    if (postOrderToExchange(new ExternalOrder(
                            UUID.randomUUID(), symbol, qty, SELF_CROSS_PRICE, Side.BUY))) {
                        accepted++;
                    }
                    if (postOrderToExchange(new ExternalOrder(
                            UUID.randomUUID(), symbol, qty, SELF_CROSS_PRICE, Side.SELL))) {
                        accepted++;
                        pairsAccepted++;
                    }
                }
                Side wideSide = (wave % 2 == 1) ? Side.BUY : Side.SELL;
                double wideLimit = wideSide == Side.BUY ? WIDE_BUY_LIMIT : WIDE_SELL_LIMIT;
                int wideQty = 1 + rnd.nextInt(3);
                boolean wideAccepted = postOrderToExchange(new ExternalOrder(
                        UUID.randomUUID(), symbol, wideQty, wideLimit, wideSide));
                if (wideAccepted) accepted++;

                eventLog.get(symbol).add(new TimedLine(waveStart,
                        "ORDERS wave=" + wave
                                + " self-cross=" + pairsAccepted + "/" + SELF_CROSS_PAIRS_PER_WAVE
                                + "pairs@" + String.format("%.2f", SELF_CROSS_PRICE)
                                + " wide=" + wideSide + "@" + String.format("%.2f", wideLimit)
                                + " qty=" + wideQty
                                + (wideAccepted ? "" : " (rejected)")));
            }
            System.out.println("[E2E] wave " + wave + ": exchange accepted " + accepted + " orders");

            for (String symbol : SEED_SYMBOLS) {
                if (!symbolsWithFills.contains(symbol) && hasNonZeroPosition(symbol)) {
                    symbolsWithFills.add(symbol);
                }
                Quote quote = currentExchangeQuote(symbol);
                if (quote != null && !quote.quoteId().equals(lastSeenQuoteIdBySymbol.get(symbol))) {
                    long now = System.currentTimeMillis();
                    String origin = bootstrapQuoteIds.contains(quote.quoteId()) ? "bootstrap" : "mm";
                    eventLog.get(symbol).add(new TimedLine(now,
                            "QUOTE [" + origin + "] id=" + quote.quoteId() + " " + formatQuoteFields(quote)));
                    lastSeenQuoteIdBySymbol.put(symbol, quote.quoteId());
                    loggedQuoteIdsBySymbol.get(symbol).add(quote.quoteId());
                    if (!firstMmQuoteBySymbol.containsKey(symbol)
                            && !bootstrapQuoteIds.contains(quote.quoteId())) {
                        firstMmQuoteBySymbol.put(symbol, quote);
                    }
                }
            }
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        Map<String, Map<UUID, List<Fill>>> fillsByQuoteIdBySymbol = new TreeMap<>();
        for (String s : SEED_SYMBOLS) {
            fillsByQuoteIdBySymbol.put(s, new LinkedHashMap<>());
        }
        for (Fill fill : getAllFills()) {
            if (fill.createdAt() < testStartMillis) continue;
            List<TimedLine> log = eventLog.get(fill.symbol());
            if (log == null) continue;
            log.add(new TimedLine(fill.createdAt(),
                    "FILL orderId=" + fill.orderId() + " side=" + fill.side()
                            + " qty=" + fill.quantity() + " price=" + fill.price()
                            + " quoteId=" + fill.quoteId()));
            fillsByQuoteIdBySymbol.get(fill.symbol())
                    .computeIfAbsent(fill.quoteId(), k -> new ArrayList<>())
                    .add(fill);
        }

        for (String symbol : SEED_SYMBOLS) {
            Set<UUID> loggedIds = loggedQuoteIdsBySymbol.get(symbol);
            for (Map.Entry<UUID, List<Fill>> entry
                    : fillsByQuoteIdBySymbol.get(symbol).entrySet()) {
                UUID qid = entry.getKey();
                if (loggedIds.contains(qid)) continue;
                List<Fill> fills = entry.getValue();
                long earliest = Long.MAX_VALUE;
                Double bid = null;
                Double ask = null;
                for (Fill f : fills) {
                    if (f.createdAt() < earliest) earliest = f.createdAt();
                    if (f.side() == Side.SELL && ask == null) ask = f.price();
                    else if (f.side() == Side.BUY && bid == null) bid = f.price();
                }
                String bidStr = bid == null ? "?" : String.format("%.4f", bid);
                String askStr = ask == null ? "?" : String.format("%.4f", ask);
                eventLog.get(symbol).add(new TimedLine(earliest,
                        "QUOTE [from-fill] id=" + qid + " bid=" + bidStr + " ask=" + askStr
                                + " (inferred from " + fills.size() + " fill"
                                + (fills.size() == 1 ? "" : "s") + ")"));
                loggedIds.add(qid);
            }
        }

        writePerSymbolLogs(eventLog);

        System.out.println("[E2E] symbols with fills: " + symbolsWithFills);
        System.out.println("[E2E] symbols with MM-generated quote in exchange: " + firstMmQuoteBySymbol.keySet());

        assertEquals(SEED_SYMBOLS, symbolsWithFills,
                "every seed symbol must have at least one fill in trading-state; "
                        + "proves external-publisher → exchange → trading-state wiring");
        assertEquals(SEED_SYMBOLS, firstMmQuoteBySymbol.keySet(),
                "after " + TOTAL_WAVES + " waves every seed symbol must have produced "
                        + "an exchange quote whose quoteId is not in the bootstrap set; "
                        + "proves each symbol's assigned market-maker wrote a quote back "
                        + "via the shared Hazelcast quotes map. bootstrap ids="
                        + bootstrapQuoteIds);

        for (String symbol : SEED_SYMBOLS) {
            Quote quote = firstMmQuoteBySymbol.get(symbol);
            assertNotNull(quote, "no MM-generated quote captured for " + symbol);
            assertEquals(symbol, quote.symbol(),
                    "quote.symbol must match requested symbol: " + quote);
            assertFalse(bootstrapQuoteIds.contains(quote.quoteId()),
                    "quote.quoteId must not be a bootstrap id (i.e. an MM wrote it): " + quote);
            assertTrue(quote.bidPrice() > 0.0, "quote.bidPrice must be > 0: " + quote);
            assertTrue(quote.askPrice() > 0.0, "quote.askPrice must be > 0: " + quote);
            assertTrue(quote.bidQuantity() >= 0, "quote.bidQuantity must be >= 0: " + quote);
            assertTrue(quote.askQuantity() >= 0, "quote.askQuantity must be >= 0: " + quote);
            assertTrue(quote.bidPrice() <= quote.askPrice(),
                    "bid must not cross ask: " + quote);
            assertTrue(quote.expiresAt() > 0, "quote.expiresAt must be > 0: " + quote);

            double spread = quote.askPrice() - quote.bidPrice();
            assertEquals(EXPECTED_SPREAD, spread, SPREAD_TOLERANCE,
                    "spread must equal marketmaker.target-spread (" + EXPECTED_SPREAD + "): " + quote);

            double mid = (quote.bidPrice() + quote.askPrice()) / 2.0;
            assertTrue(mid >= MIN_REASONABLE_MID && mid <= MAX_REASONABLE_MID,
                    "mid (" + mid + ") must be in [" + MIN_REASONABLE_MID + ", "
                            + MAX_REASONABLE_MID + "]: " + quote);

            assertTrue(quote.bidQuantity() <= MAX_QUANTITY_PER_SIDE,
                    "bidQuantity must be <= " + MAX_QUANTITY_PER_SIDE + ": " + quote);
            assertTrue(quote.askQuantity() <= MAX_QUANTITY_PER_SIDE,
                    "askQuantity must be <= " + MAX_QUANTITY_PER_SIDE + ": " + quote);
        }

        List<Fill> allFills = new ArrayList<>();
        for (Fill fill : getAllFills()) {
            if (fill.createdAt() >= testStartMillis) {
                allFills.add(fill);
            }
        }
        assertFalse(allFills.isEmpty(),
                "trading-state /state/fills returned no fills from this run (after " + testStartMillis + ")");

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
                System.out.println("[E2E] " + serviceName + " healthy on port " + port);
                return;
            }
            Thread.sleep(2000);
        }
        System.err.println("[E2E] " + serviceName + " did not respond on /health within " + timeout);
        System.err.println("---- docker compose ps " + serviceName + " ----");
        System.err.println(runDockerCapturing(TimeUnit.SECONDS.toMillis(30),
                "compose", "ps", serviceName));
        System.err.println("---- docker compose logs --tail 300 " + serviceName + " ----");
        System.err.println(runDockerCapturing(TimeUnit.MINUTES.toMillis(1),
                "compose", "logs", "--tail", "300", serviceName));
        System.err.println("---- end logs ----");
        throw new AssertionError(serviceName + " not healthy within " + timeout);
    }

    private static String runDockerCapturing(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true);
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
            output.append("[timed out waiting for docker command]\n");
        }
        return output.toString();
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

    /**
     * Submit a single order directly to the exchange. Returns true on HTTP 200
     * (the exchange accepted the order — it may have filled, partially filled,
     * or been booked). Non-200 / transport error → false.
     */
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

    private static List<Fill> getAllFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + TRADING_STATE_PORT + "/state/fills"))
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
                    .uri(URI.create("http://localhost:" + TRADING_STATE_PORT + "/positions/" + symbol))
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

    private static Quote currentExchangeQuote(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXCHANGE_PORT + "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(resp.body(), Quote.class);
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
        if (env != null) {
            pb.environment().putAll(env);
        }
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

    private record TimedLine(long epochMillis, String text) {}

    private static final DateTimeFormatter LOG_TS = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(ZoneOffset.UTC);

    private static String formatQuoteFields(Quote q) {
        return String.format("bid=%.4f@%d ask=%.4f@%d expires=%d",
                q.bidPrice(), q.bidQuantity(), q.askPrice(), q.askQuantity(), q.expiresAt());
    }

    private static void writePerSymbolLogs(Map<String, List<TimedLine>> eventLog) throws IOException {
        Path dir = Paths.get("target", "integration-logs");
        Files.createDirectories(dir);
        for (Map.Entry<String, List<TimedLine>> entry : eventLog.entrySet()) {
            String symbol = entry.getKey();
            List<TimedLine> lines = new ArrayList<>(entry.getValue());
            lines.sort(Comparator.comparingLong(TimedLine::epochMillis));
            List<String> rendered = new ArrayList<>(lines.size());
            for (TimedLine line : lines) {
                rendered.add(LOG_TS.format(Instant.ofEpochMilli(line.epochMillis())) + " " + line.text());
            }
            Path file = dir.resolve(symbol + ".log");
            Files.write(file, rendered);
            System.out.println("[E2E] wrote " + lines.size() + " events to " + file.toAbsolutePath());
        }
    }
}