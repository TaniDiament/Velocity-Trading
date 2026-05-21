package edu.yu.velocitytrading.marketmaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.velocitytrading.model.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Integration test for the MarketMaker <-> ExposureReservation flow.
 *
 * <p>Prerequisites: {@code docker compose up --build -d} must be running.</p>
 */
public class MarketMakerTest {

    private static final String MARKET_MAKER_BASE = "http://localhost:9080";
    private static final String EXPOSURE_RESERVATION_BASE = "http://localhost:9081";
    private static final int DASHBOARD_PORT = 9099;
    private static final int EXPECTED_FIRST_BID_USAGE = 12;
    private static final int EXPECTED_FIRST_ASK_USAGE = 9;
    private static final int EXPECTED_SECOND_BID_USAGE = 11;
    private static final int EXPECTED_SECOND_ASK_USAGE = 11;
    private static final int DEFAULT_QUOTE_QUANTITY = 10;
    private static final double TARGET_SPREAD = 0.10;
    private static final int DASHBOARD_FINAL_VIEW_MILLIS = 20_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        System.out.println("=== MarketMaker <-> ExposureReservation Integration Test ===\n");

        TestDashboardServer dashboard = TestDashboardServer.start(DASHBOARD_PORT);
        boolean success = false;
        System.out.println("Dashboard: http://localhost:" + DASHBOARD_PORT);
        dashboard.mark("Startup", "RUNNING", "Test process started", null);

        try {
            // Step 0: Wait for services to be healthy
            dashboard.mark("Step 0", "RUNNING", "Waiting for services to become healthy", null);
            System.out.println("Waiting for services to be healthy...");
            waitForHealth(EXPOSURE_RESERVATION_BASE + "/health", "exposure-reservation");
            // market-maker has no /health endpoint; use TestPositionTracker liveness
            waitForHealth(MARKET_MAKER_BASE + "/test/position-tracker/received/NONE", "market-maker");
            dashboard.mark("Step 0", "PASS", "Services are healthy", safeGetExposureState());
            System.out.println("All services healthy.\n");

            // Step 1: Verify initial exposure is zero
            dashboard.mark("Step 1", "RUNNING", "Checking initial exposure state", null);
            System.out.println("--- Step 1: Checking initial exposure state ---");
            ExposureState initialState = getExposureState();
            System.out.println("Initial exposure: " + initialState);
            assertEqual(0, initialState.bidUsage(), "initial bidUsage");
            assertEqual(0, initialState.askUsage(), "initial askUsage");
            assertEqual(0, initialState.activeReservations(), "initial activeReservations");
            dashboard.mark("Step 1", "PASS", "Initial exposure is zero", initialState);
            System.out.println("PASS: Initial exposure is zero.\n");

            // Step 2: Register symbol AAPL with the TestPositionTracker
            dashboard.mark("Step 2", "RUNNING", "Registering symbol AAPL", initialState);
            System.out.println("--- Step 2: Registering symbol AAPL ---");
            int code = httpPut(MARKET_MAKER_BASE + "/test/position-tracker/symbols/AAPL", null);
            System.out.println("PUT /test/position-tracker/symbols/AAPL -> " + code);
            assertHttpSuccess(code, "Register symbol AAPL");
            dashboard.mark("Step 2", "PASS", "Symbol AAPL registered", safeGetExposureState());
            System.out.println("PASS: Symbol AAPL registered.\n");

            // Step 3: Submit a StateSnapshot
            dashboard.mark("Step 3", "RUNNING", "Submitting first StateSnapshot", safeGetExposureState());
            System.out.println("--- Step 3: Submitting StateSnapshot for AAPL ---");
            Fill fill = new Fill(
                    UUID.randomUUID(),
                    "AAPL",
                    Side.BUY,
                    5,
                    150.00,
                    UUID.randomUUID(),
                    System.currentTimeMillis());
            Position position = new Position("AAPL", 5, 1, fill.orderId());
            StateSnapshot snapshot = new StateSnapshot(position, fill);
            QuoteProjection firstProjection = projectQuote(null, fill);

            String snapshotJson = MAPPER.writeValueAsString(snapshot);
            System.out.println("Snapshot JSON: " + snapshotJson);
            code = httpPost(MARKET_MAKER_BASE + "/test/position-tracker/snapshots", snapshotJson);
            System.out.println("POST /test/position-tracker/snapshots -> " + code);
            assertHttpSuccess(code, "Submit snapshot");
            dashboard.mark(
                    "Step 3",
                    "PASS",
                    "First snapshot submitted: " + positionSummary(snapshot) + ", " + quoteSummary(firstProjection),
                    safeGetExposureState());
            System.out.println("PASS: Snapshot submitted.\n");

            // Step 4: Wait for the async pipeline to process the snapshot
            dashboard.mark("Step 4", "RUNNING", "Waiting for async processing", safeGetExposureState());
            System.out.println("--- Step 4: Waiting for async processing ---");
            Thread.sleep(5000);
            dashboard.mark("Step 4", "PASS", "Async processing wait complete", safeGetExposureState());

            // Step 5: Verify exposure reservation was created
            dashboard.mark("Step 5", "RUNNING", "Verifying exposure reservation", safeGetExposureState());
            System.out.println("--- Step 5: Verifying exposure reservation ---");
            ExposureState finalState = getExposureState();
            System.out.println("Final exposure: " + finalState);

            if (finalState.bidUsage() > 0 || finalState.askUsage() > 0) {
                System.out.println("PASS: Exposure reservation created! BidUsage=" + finalState.bidUsage()
                        + ", AskUsage=" + finalState.askUsage()
                        + ", ActiveReservations=" + finalState.activeReservations());
                assertEqual(EXPECTED_FIRST_BID_USAGE, finalState.bidUsage(), "first reservation bidUsage");
                assertEqual(EXPECTED_FIRST_ASK_USAGE, finalState.askUsage(), "first reservation askUsage");
                assertEqual(1, finalState.activeReservations(), "first reservation activeReservations");
                dashboard.mark(
                        "Step 5",
                        "PASS",
                        "Reservation matched expected first quote: bidUsage=12 askUsage=9 active=1",
                        finalState);
            } else {
                throw new AssertionError("FAIL: Expected non-zero exposure usage but got bidUsage="
                        + finalState.bidUsage() + ", askUsage=" + finalState.askUsage());
            }

            // Step 6: Submit a second snapshot with a higher version to test quote update
            dashboard.mark("Step 6", "RUNNING", "Submitting second snapshot", finalState);
            System.out.println("\n--- Step 6: Submitting second snapshot (version 2) ---");
            Fill fill2 = new Fill(
                    UUID.randomUUID(),
                    "AAPL",
                    Side.SELL,
                    3,
                    150.50,
                    UUID.randomUUID(),
                    System.currentTimeMillis());
            Position position2 = new Position("AAPL", 2, 2, fill2.orderId());
            StateSnapshot snapshot2 = new StateSnapshot(position2, fill2);
            QuoteProjection secondProjection = projectQuote(firstProjection, fill2);

            code = httpPost(MARKET_MAKER_BASE + "/test/position-tracker/snapshots",
                    MAPPER.writeValueAsString(snapshot2));
            System.out.println("POST /test/position-tracker/snapshots -> " + code);
            assertHttpSuccess(code, "Submit second snapshot");
            dashboard.mark(
                    "Step 6",
                    "RUNNING",
                    "Second snapshot submitted: " + positionSummary(snapshot2) + ", " + quoteSummary(secondProjection),
                    safeGetExposureState());

            Thread.sleep(5000);

            ExposureState afterSecond = getExposureState();
            System.out.println("Exposure after second snapshot: " + afterSecond);
            if (afterSecond.activeReservations() > 0) {
                assertEqual(EXPECTED_SECOND_BID_USAGE, afterSecond.bidUsage(), "second reservation bidUsage");
                assertEqual(EXPECTED_SECOND_ASK_USAGE, afterSecond.askUsage(), "second reservation askUsage");
                assertEqual(1, afterSecond.activeReservations(), "second reservation activeReservations");
                System.out.println("PASS: Reservation updated after second snapshot.");
                dashboard.mark(
                        "Step 6",
                        "PASS",
                        "Reservation matched expected second quote: bidUsage=11 askUsage=11 active=1",
                        afterSecond);
            } else {
                throw new AssertionError("FAIL: Expected active reservations after second snapshot.");
            }

            success = true;
            dashboard.mark("Test Run", "PASS", "All tests passed", afterSecond);
            System.out.println("\n=== ALL TESTS PASSED ===");
        } catch (Throwable t) {
            dashboard.mark("Test Run", "FAIL", t.getClass().getSimpleName() + ": " + t.getMessage(), safeGetExposureState());
            throw t;
        } finally {
            if (!success) {
                System.out.println("Dashboard marked failure. See http://localhost:" + DASHBOARD_PORT);
            }
            System.out.println("Keeping dashboard alive for " + (DASHBOARD_FINAL_VIEW_MILLIS / 1000)
                    + " seconds before shutdown...");
            Thread.sleep(DASHBOARD_FINAL_VIEW_MILLIS);
            dashboard.stop();
        }
    }

    // --- Helper methods ---

    private static ExposureState getExposureState() throws Exception {
        URL url = URI.create(EXPOSURE_RESERVATION_BASE + "/exposure").toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");
        int code = conn.getResponseCode();
        if (code != 200) {
            throw new RuntimeException("GET /exposure returned " + code);
        }
        String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        conn.disconnect();
        return MAPPER.readValue(body, ExposureState.class);
    }

    private static ExposureState safeGetExposureState() {
        try {
            return getExposureState();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void waitForHealth(String url, String serviceName) throws Exception {
        int maxRetries = 60;
        for (int i = 1; i <= maxRetries; i++) {
            try {
                HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(2000);
                conn.setReadTimeout(2000);
                int code = conn.getResponseCode();
                conn.disconnect();
                if (code == 200) {
                    System.out.println("  " + serviceName + " healthy (attempt " + i + ")");
                    return;
                }
            } catch (IOException ignored) {
                // Service not ready yet
            }
            System.out.println("  " + serviceName + " not ready, retrying (" + i + "/" + maxRetries + ")...");
            Thread.sleep(3000);
        }
        throw new RuntimeException(serviceName + " failed to become healthy after " + maxRetries + " retries");
    }

    private static int httpPut(String url, String jsonBody) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        if (jsonBody != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
            }
        }
        int code = conn.getResponseCode();
        conn.disconnect();
        return code;
    }

    private static int httpPost(String url, String jsonBody) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        if (jsonBody != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
            }
        }
        int code = conn.getResponseCode();
        conn.disconnect();
        return code;
    }

    private static void assertEqual(int expected, int actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertHttpSuccess(int code, String label) {
        if (code < 200 || code >= 300) {
            throw new AssertionError(label + ": expected 2xx but got " + code);
        }
    }

    private static String positionSummary(StateSnapshot snapshot) {
        Position p = snapshot.position();
        Fill f = snapshot.fill();
        return String.format(
                "position[symbol=%s qty=%d ver=%d side=%s fillQty=%d px=%.2f]",
                p.symbol(),
                p.netQuantity(),
                p.version(),
                f.side(),
                f.quantity(),
                f.price());
    }

    private static String quoteSummary(QuoteProjection quote) {
        return String.format(
                "quote[symbol=%s bid=%.2fx%d ask=%.2fx%d]",
                quote.symbol(),
                quote.bidPrice(),
                quote.bidQuantity(),
                quote.askPrice(),
                quote.askQuantity());
    }

    private static QuoteProjection projectQuote(QuoteProjection current, Fill lastFill) {
        double referencePrice = current != null ? (current.bidPrice() + current.askPrice()) / 2.0 : lastFill.price();
        double halfSpread = TARGET_SPREAD / 2.0;
        int bidQuantity = current != null ? Math.max(0, current.bidQuantity()) : Math.max(1, DEFAULT_QUOTE_QUANTITY);
        int askQuantity = current != null ? Math.max(0, current.askQuantity()) : Math.max(1, DEFAULT_QUOTE_QUANTITY);

        if (lastFill.side() == Side.SELL) {
            referencePrice += 0.01 * lastFill.quantity();
            askQuantity += 2;
            bidQuantity = Math.max(0, bidQuantity - 1);
        } else if (lastFill.side() == Side.BUY) {
            referencePrice -= 0.01 * lastFill.quantity();
            askQuantity = Math.max(0, askQuantity - 1);
            bidQuantity += 2;
        }

        double bidPrice = Math.max(0.01, referencePrice - halfSpread);
        double askPrice = Math.max(bidPrice, referencePrice + halfSpread);
        return new QuoteProjection(lastFill.symbol(), bidPrice, bidQuantity, askPrice, askQuantity);
    }

    private record QuoteProjection(
            String symbol,
            double bidPrice,
            int bidQuantity,
            double askPrice,
            int askQuantity) {
    }
}
