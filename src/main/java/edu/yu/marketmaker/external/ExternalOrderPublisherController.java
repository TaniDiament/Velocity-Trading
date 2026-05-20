package edu.yu.marketmaker.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.ha.ServiceRegistry;
import edu.yu.marketmaker.model.ExternalOrder;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.Side;
import edu.yu.marketmaker.service.ServiceHealth;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/**
 * Test-driven external-order publisher. Under the {@code external-publisher}
 * profile this replaces the standalone {@link OrderPublisherRunner}: the
 * container boots idle and exposes two endpoints the end-to-end test calls
 * to drive traffic deterministically:
 * <ul>
 *   <li>{@code POST /publisher/seed-quotes} — PUTs a fixed, well-formed quote
 *       per symbol into the exchange so orders can match, and returns the
 *       generated quoteIds so the test can distinguish bootstrap quotes from
 *       ones later written by market-maker nodes.</li>
 *   <li>{@code POST /publisher/submit-orders?count=N} — submits {@code N}
 *       orders per symbol, priced to cross both the bootstrap spread and the
 *       tighter spread a {@code ProductionQuoteGenerator} will produce.</li>
 * </ul>
 *
 * <p>All writes target the current {@code exchange} leader — resolved from the
 * {@link ServiceRegistry} ZK watch — rather than the round-robin
 * {@code exchange} DNS alias, which would land on a non-leader replica 2/3 of
 * the time and get rejected with HTTP 503 by {@code LeaderGuardFilter}. The
 * helper retries briefly on 503 / connect failure so an in-flight failover
 * doesn't fail user-visible requests.
 */
@RestController
@Profile("external-publisher")
public class ExternalOrderPublisherController {

    private static final Logger logger = LoggerFactory.getLogger(ExternalOrderPublisherController.class);
    private static final String EXCHANGE_SERVICE = "exchange";
    private static final long QUOTE_TTL_MS = 5 * 60_000L;
    /** Total time we'll spend trying to resolve a leader / dodge a 503 before giving up on one request. */
    private static final long LEADER_RESOLVE_BUDGET_MS = 10_000L;
    private static final long LEADER_RESOLVE_BACKOFF_MS = 100L;

    private final ServiceRegistry serviceRegistry;
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpClient http;

    /**
     * Last diagnostic message from a non-200 / exception path in
     * {@link #submitOrders}. Surfaced via {@code GET /publisher/last-failure}
     * so an e2e assertion that sees {@code accepted=0} can include the actual
     * exchange-side response in its failure message without scraping pod logs.
     * Volatile because submit waves and the GET endpoint run on different
     * Tomcat threads.
     */
    private volatile String lastFailureSample = "";

    public ExternalOrderPublisherController(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    @PostConstruct
    void init() {
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * Seed one quote per symbol in the exchange. Deterministic prices
     * (bid=99.50, ask=100.50, qty=1000 each side) so (a) orders at 99/101
     * always cross and (b) the reservation service has plenty of capacity.
     */
    @PostMapping("/publisher/seed-quotes")
    public ResponseEntity<List<UUID>> seedQuotes(@RequestBody List<String> symbols) throws Exception {
        List<UUID> ids = new ArrayList<>();
        for (String symbol : symbols) {
            UUID quoteId = UUID.randomUUID();
            Quote quote = new Quote(
                    symbol,
                    99.50, 1000,
                    100.50, 1000,
                    quoteId,
                    System.currentTimeMillis() + QUOTE_TTL_MS);
            String body = mapper.writeValueAsString(quote);
            HttpResponse<String> resp = sendToExchangeLeader("/quotes/" + symbol, "PUT", body);
            if (resp.statusCode() >= 400) {
                throw new IllegalStateException(
                        "PUT /quotes/" + symbol + " failed: " + resp.statusCode() + " " + resp.body());
            }
            ids.add(quoteId);
            logger.info("Seeded quote for {} with id {}", symbol, quoteId);
        }
        return ResponseEntity.ok(ids);
    }

    /**
     * Submit {@code count} orders per symbol. Alternating sides with limit
     * prices that cross both the bootstrap ({@code 99.50/100.50}) and the
     * tighter ({@code 99.95/100.05}) production quote.
     * @return number of orders the exchange accepted (HTTP 200); non-200
     *         responses are tallied separately in the logs but not counted,
     *         since stale quotes / empty residual depth legitimately reject.
     */
    @PostMapping("/publisher/submit-orders")
    public ResponseEntity<Integer> submitOrders(
            @RequestParam int count,
            @RequestBody List<String> symbols) {
        int accepted = 0;
        int rejected = 0;
        // Capture the first non-200 body we see so a 100%-rejected wave
        // (the error-case-11 failure mode) leaves a concrete server-side
        // diagnostic in the publisher log — otherwise we'd only see the
        // "accepted=0" count with no way to tell whether it's 400 / 500 /
        // 503 / transport error.
        String firstFailureSample = null;
        Random rnd = new Random();
        for (int i = 0; i < count; i++) {
            for (String symbol : symbols) {
                Side side = (i + symbol.hashCode()) % 2 == 0 ? Side.BUY : Side.SELL;
                double limitPrice = side == Side.BUY ? 101.0 : 99.0;
                int quantity = 1 + rnd.nextInt(3);
                ExternalOrder order = new ExternalOrder(UUID.randomUUID(), symbol, quantity, limitPrice, side);
                try {
                    String body = mapper.writeValueAsString(order);
                    HttpResponse<String> resp = sendToExchangeLeader("/orders", "POST", body);
                    if (resp.statusCode() == 200) {
                        accepted++;
                    } else {
                        rejected++;
                        if (firstFailureSample == null) {
                            firstFailureSample = "HTTP " + resp.statusCode() + " " + resp.body();
                        }
                        logger.warn("order rejected for {} ({}@{}): HTTP {} {}",
                                symbol, side, limitPrice, resp.statusCode(), resp.body());
                    }
                } catch (Exception e) {
                    rejected++;
                    if (firstFailureSample == null) {
                        firstFailureSample = "EXCEPTION " + e.getClass().getSimpleName() + ": " + e.getMessage();
                    }
                    logger.warn("order submit threw for {} ({}@{}): {}",
                            symbol, side, limitPrice, e.toString());
                }
            }
        }
        logger.info("Submitted orders: accepted={}, rejected={}, firstFailure={}",
                accepted, rejected, firstFailureSample);
        if (firstFailureSample != null) {
            this.lastFailureSample = firstFailureSample;
        }
        return ResponseEntity.ok(accepted);
    }

    /**
     * @return the first non-200 response body (or exception summary) seen by
     *         the most recent {@link #submitOrders} wave that had any
     *         rejections. Empty string if the publisher has only seen 200s
     *         since startup. Used by e2e tests to surface the exchange-side
     *         reason for an unexpected 0-accepted wave.
     */
    @GetMapping("/publisher/last-failure")
    public ResponseEntity<String> lastFailure() {
        return ResponseEntity.ok(lastFailureSample);
    }

    /**
     * Send a JSON body to the current exchange leader at {@code pathAndQuery}.
     *
     * <p>The exchange runs three replicas behind a Hazelcast/ZK leader latch;
     * only the leader accepts mutating requests, the rest reply 503. The
     * leader's hostname is published in {@link ServiceRegistry}, so we look
     * it up per-request and retry briefly on 503 (stale registry cache during
     * failover) or on a {@link ConnectException} (leader pod restarting).
     *
     * @throws IllegalStateException if no leader can be reached within {@link #LEADER_RESOLVE_BUDGET_MS}
     */
    private HttpResponse<String> sendToExchangeLeader(String pathAndQuery,
                                                      String method,
                                                      String body) throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + LEADER_RESOLVE_BUDGET_MS;
        Throwable lastTransport = null;
        int attempts = 0;
        while (System.currentTimeMillis() < deadline) {
            attempts++;
            Optional<ServiceRegistry.Endpoint> leader = serviceRegistry.getLeaderAddress(EXCHANGE_SERVICE);
            if (leader.isEmpty()) {
                Thread.sleep(LEADER_RESOLVE_BACKOFF_MS);
                continue;
            }
            URI uri = URI.create("http://" + leader.get().host + ":" + leader.get().httpPort + pathAndQuery);
            HttpRequest.BodyPublisher publisher = body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body);
            HttpRequest.Builder b = HttpRequest.newBuilder(uri)
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(10));
            switch (method) {
                case "PUT" -> b.PUT(publisher);
                case "POST" -> b.POST(publisher);
                default -> throw new IllegalArgumentException("unsupported method: " + method);
            }
            try {
                HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 503) return resp;
                // 503 from LeaderGuardFilter: registry cache caught a stale endpoint.
                // Sleep briefly and let the watcher catch up before retrying.
            } catch (ConnectException e) {
                lastTransport = e;
            }
            Thread.sleep(LEADER_RESOLVE_BACKOFF_MS);
        }
        String msg = "exchange leader unreachable for " + method + " " + pathAndQuery
                + " after " + attempts + " attempts";
        if (lastTransport != null) throw new IllegalStateException(msg, lastTransport);
        throw new IllegalStateException(msg);
    }

    @GetMapping("/health")
    public ServiceHealth getHealth() {
        return new ServiceHealth(true, 0, "External Order Publisher");
    }
}