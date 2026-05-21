package edu.yu.velocitytrading.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import edu.yu.velocitytrading.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

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

    private static final int TOTAL_WAVES = 50;
    private static final long WAVE_INTERVAL_MS = 1500;
    // Each wave per symbol: SELF_CROSS_PAIRS_PER_WAVE pairs of (BUY@P, SELL@P)
    // at SELF_CROSS_PRICE — both orders rest inside the MM's spread and match
    // each other, so MM exposure isn't consumed. Plus one "wide" order per
    // wave that DOES cross the MM (BUY@101 on odd waves, SELL@99 on even),
    // which keeps the MM engaged while balancing flow so net position oscillates
    // around zero rather than running to the ±100 cap.
    private static final int SELF_CROSS_PAIRS_PER_WAVE = 5;
    private static final double SELF_CROSS_PRICE = 100.00;
    private static final double WIDE_BUY_LIMIT = 101.00;
    private static final double WIDE_SELL_LIMIT = 99.00;

    // Mirrors marketmaker.target-spread in application-market-maker-node.properties.
    // The production quote generator publishes ref±0.05 deterministically, so
    // the spread should be exactly 0.10 modulo floating-point noise.
    private static final double EXPECTED_SPREAD = 0.10;
    private static final double SPREAD_TOLERANCE = 1e-3;
    // Bootstrap reference is 100.00. Inventory-aware skew nudges by 0.01 per
    // share filled, so mid drifts a bit, but the first captured MM quote
    // should still be near the bootstrap mid.
    private static final double MIN_REASONABLE_MID = 95.0;
    private static final double MAX_REASONABLE_MID = 105.0;
    // ProductionQuoteGenerator caps each side at 100 ± netQuantity, so the
    // sum of granted bid+ask quantities cannot exceed 200 by construction.
    private static final int MAX_QUANTITY_PER_SIDE = 200;

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
        // Per-symbol activity log: orders submitted (per wave aggregate, since
        // individual orders are created server-side by the publisher), quote
        // observations, and fills. Each entry is timestamped and the file is
        // sorted by timestamp before flushing.
        Map<String, List<TimedLine>> eventLog = new TreeMap<>();
        for (String s : SEED_SYMBOLS) {
            eventLog.put(s, new ArrayList<>());
        }
        Map<String, UUID> lastSeenQuoteIdBySymbol = new HashMap<>();
        // Tracks every quoteId we've already emitted a QUOTE entry for, so we
        // don't double-log when reconstructing missed quotes from fill records.
        Map<String, Set<UUID>> loggedQuoteIdsBySymbol = new HashMap<>();
        for (String s : SEED_SYMBOLS) {
            loggedQuoteIdsBySymbol.put(s, new HashSet<>());
        }

        // /state/fills returns the entire historical fill table, so fills from
        // prior test runs would dominate the per-symbol logs. Capture the
        // wall-clock start of this run and use it as a lower bound when
        // distributing fills into the per-symbol event lists. Subtract a small
        // safety margin to forgive minor clock skew between this machine and
        // trading-state.
        long testStartMillis = System.currentTimeMillis() - 5_000;

        System.out.println("[E2E-k8s] seeding bootstrap quotes via external-publisher...");
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

        // Fixed wave count keeps the test deterministic regardless of how
        // quickly fills propagate. 50 waves of 25 orders × 7 symbols is far
        // more than enough for every symbol's assigned MM to react.
        //
        // We capture the FIRST MM-generated quote we observe per symbol (rather
        // than reading the live quote after all 50 waves). After heavy drain
        // an MM may legitimately publish a paused quote with 0 quantity; we
        // want to validate the structurally-meaningful first publish, which
        // is what proves the production-quote-generator is working.
        Set<String> symbolsWithFills = new TreeSet<>();
        Map<String, Quote> firstMmQuoteBySymbol = new TreeMap<>();
        Random rnd = new Random(0xC0FFEE);
        for (int wave = 1; wave <= TOTAL_WAVES; wave++) {
            long waveStart = System.currentTimeMillis();
            int accepted = 0;
            for (String symbol : SEED_SYMBOLS) {
                // SELF_CROSS_PAIRS_PER_WAVE pairs at SELF_CROSS_PRICE — both
                // orders rest inside the MM's spread; SELL matches the resting
                // BUY, MM is not the counterparty.
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
                // One wide order whose side alternates per wave keeps MM's
                // net position oscillating around zero.
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
            System.out.println("[E2E-k8s] wave " + wave + ": exchange accepted " + accepted + " orders");

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

        // Fold this run's fills into per-symbol logs. Each fill carries its
        // real createdAt timestamp from trading-state; we drop anything older
        // than the test's start time to avoid including fills accumulated by
        // prior runs in the persistent postgres table.
        // We also bucket fills by (symbol, quoteId) so we can synthesize a
        // QUOTE entry for any quoteId that only appears in fills (the MM
        // republished faster than the test polled).
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

        // Reconstruct QUOTE entries for any quoteId we only saw in fill data.
        // ProductionQuoteGenerator publishes faster than this test polls, so
        // many MM-published quoteIds are referenced by fills but never appear
        // in our poll-based QUOTE entries.
        //
        // Side semantics in fills (from inspection of the data + the price
        // bands the publisher uses): fill.side reflects the MM's side of the
        // trade. fill.side=SELL → MM's ask was hit; fill.side=BUY → MM's bid
        // was hit. So a SELL fill's price is the quote's ask, a BUY fill's
        // price is the quote's bid. With both sides represented, we recover
        // both legs of the quote; otherwise we mark the missing side '?'.
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

        System.out.println("[E2E-k8s] symbols with fills: " + symbolsWithFills);
        System.out.println("[E2E-k8s] symbols with MM-generated quote in exchange: " + firstMmQuoteBySymbol.keySet());

        assertEquals(SEED_SYMBOLS, symbolsWithFills,
                "every seed symbol must have at least one fill in trading-state; "
                        + "proves external-publisher → exchange → trading-state wiring");
        assertEquals(SEED_SYMBOLS, firstMmQuoteBySymbol.keySet(),
                "after " + TOTAL_WAVES + " waves every seed symbol must have produced "
                        + "an exchange quote whose quoteId is not in the bootstrap set; "
                        + "proves each symbol's assigned market-maker wrote a quote back "
                        + "via the shared Hazelcast quotes map. bootstrap ids="
                        + bootstrapQuoteIds);

        // Per-symbol quote-content assertions on the first MM-generated quote
        // observed for each symbol: prove the MM is publishing structurally
        // valid quotes (correct symbol, positive prices/qty, non-crossed bid/ask).
        for (String symbol : SEED_SYMBOLS) {
            Quote quote = firstMmQuoteBySymbol.get(symbol);
            assertNotNull(quote, "no MM-generated quote captured for " + symbol);
            assertEquals(symbol, quote.symbol(),
                    "quote.symbol must match requested symbol: " + quote);
            assertFalse(bootstrapQuoteIds.contains(quote.quoteId()),
                    "quote.quoteId must not be a bootstrap id (i.e. an MM wrote it): " + quote);
            assertTrue(quote.bidPrice() > 0.0, "quote.bidPrice must be > 0: " + quote);
            assertTrue(quote.askPrice() > 0.0, "quote.askPrice must be > 0: " + quote);
            // Quantities can be 0 — a 0-qty side means the MM has hit its
            // exposure cap on that side and is signalling "do not match here".
            // A fully 0×0 quote is also valid: the MM is paused on both sides.
            // What we don't allow is a negative quantity.
            assertTrue(quote.bidQuantity() >= 0, "quote.bidQuantity must be >= 0: " + quote);
            assertTrue(quote.askQuantity() >= 0, "quote.askQuantity must be >= 0: " + quote);
            assertTrue(quote.bidPrice() <= quote.askPrice(),
                    "bid must not cross ask: " + quote);
            assertTrue(quote.expiresAt() > 0, "quote.expiresAt must be > 0: " + quote);

            // Spread must match the configured target. ProductionQuoteGenerator
            // computes bid/ask as ref±halfSpread, so this is exact.
            double spread = quote.askPrice() - quote.bidPrice();
            assertEquals(EXPECTED_SPREAD, spread, SPREAD_TOLERANCE,
                    "spread must equal marketmaker.target-spread (" + EXPECTED_SPREAD + "): " + quote);

            // Mid-price must stay near the bootstrap reference (100). Drifts
            // a fraction per fill via inventory skew but should never wander
            // far on the first observed MM quote.
            double mid = (quote.bidPrice() + quote.askPrice()) / 2.0;
            assertTrue(mid >= MIN_REASONABLE_MID && mid <= MAX_REASONABLE_MID,
                    "mid (" + mid + ") must be in [" + MIN_REASONABLE_MID + ", "
                            + MAX_REASONABLE_MID + "]: " + quote);

            // Each side is capped by 100 ± netQuantity; reservation can only
            // shrink that. So neither side may exceed 200.
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

    /**
     * Submit a single order directly to the exchange's /orders endpoint.
     * Returns true on HTTP 200 (the exchange accepted the order — it may have
     * filled, partially filled, or been booked). Returns false on any non-200
     * or transport error; we tally those as rejected for the wave summary.
     */
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

    /** A single chronologically-ordered event line in a per-symbol log. */
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
            System.out.println("[E2E-k8s] wrote " + lines.size() + " events to " + file.toAbsolutePath());
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
            return JSON.copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(resp.body(), Quote.class);
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