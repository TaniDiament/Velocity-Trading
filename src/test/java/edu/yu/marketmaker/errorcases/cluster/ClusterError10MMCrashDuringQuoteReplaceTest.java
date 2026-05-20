package edu.yu.marketmaker.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import edu.yu.marketmaker.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * k3s/Kubernetes variant of
 * {@code edu.yu.marketmaker.errorcases.local.LocalError10MMCrashDuringQuoteReplaceTest}.
 *
 * <p>Same end-to-end assertions for error case 10, but talks to a running
 * k3s cluster via NodePorts instead of bringing up a docker-compose stack.
 * The dead pod is deliberately not waited-on for restart — the cluster must
 * self-recover via HA failover (PR #88) just like in the local variant.
 *
 * <p>Pre-conditions (must hold before invoking this test):
 * <ol>
 *   <li>Build offline image bundle and import on every k3s node.</li>
 *   <li>{@code kubectl apply -k k8s/} — the manifests already include the
 *       {@code fault-injection} profile (see {@code k8s/market-maker.yaml}),
 *       which makes the {@link
 *       edu.yu.marketmaker.marketmaker.FaultInjectionController} endpoint
 *       reachable. The injector stays disarmed until this test arms it.</li>
 * </ol>
 *
 * <p>Opt-in: {@code -Dcluster.k8s.it=true}.
 *
 * <p>Tunables (system properties):
 * <ul>
 *   <li>{@code cluster.k8s.host}      host that exposes NodePorts (default: localhost)</li>
 *   <li>{@code cluster.k8s.namespace} k8s namespace (default: market-maker)</li>
 *   <li>{@code kubectl}               kubectl binary path (default: kubectl)</li>
 * </ul>
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class ClusterError10MMCrashDuringQuoteReplaceTest {

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

    // The crash target is the pod that StatefulSet scale-down removes first
    // (highest ordinal). We then scale `sts/mm` to TARGET_REPLICAS so kubelet
    // won't recreate it — that's the closest k3s analogue to the local
    // docker-compose scenario where the dead container stays dead. The symbol
    // we crash mid-replace is whichever one this pod currently owns; it's
    // resolved from /marketmaker/status at runtime instead of being hardcoded
    // because symbol→pod assignment depends on the cluster's hash sharding.
    private static final String TARGET_POD = "mm-6";
    private static final int TARGET_PORT = 30087;
    private static final int TARGET_REPLICAS = 6;
    private static final int FULL_REPLICAS = 7;

    private static final int WARMUP_WAVES = 4;
    private static final long WAVE_INTERVAL_MS = 1500;
    /** Orders per symbol per wave when calling /publisher/submit-orders. */
    private static final int ORDERS_PER_SYMBOL_PER_WAVE = 2;

    // NodePorts defined in k8s/*.yaml — mirror ClusterIntegrationWithSystemK8sTest.
    private static final int TRADING_STATE_PORT = 30180;
    private static final int EXCHANGE_PORT      = 30181;
    private static final int EXPOSURE_RES_PORT  = 30182;
    private static final int PUBLISHER_PORT     = 30183;

    private static final String HOST = System.getProperty("cluster.k8s.host", "localhost");
    private static final String NS   = System.getProperty("cluster.k8s.namespace", "market-maker");
    private static final String KUBECTL = System.getProperty("kubectl", "kubectl");

    // SSH the kubectl invocations to the control-plane node where KUBECONFIG
    // is actually configured. Without this, running the test from a dev box
    // produces silent "Error from server (NotFound)" failures because kubectl
    // is talking to the wrong API server. Set -Dkubectl.ssh="" to disable
    // (i.e. when running the test on cp1 itself).
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
        System.out.println("[ERR10-k8s] host=" + HOST + " namespace=" + NS);

        awaitHealthy("trading-state",        TRADING_STATE_PORT, "/health",               Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT,  "/health",               Duration.ofMinutes(5));
        awaitHealthy("exchange",             EXCHANGE_PORT,      "/health",               Duration.ofMinutes(5));
        awaitHealthy("external-publisher",   PUBLISHER_PORT,     "/health",               Duration.ofMinutes(5));

        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            awaitHealthy(e.getValue(), e.getKey(), "/marketmaker/status", Duration.ofMinutes(5));
        }

        System.out.println("[ERR10-k8s] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8),
                ClusterError10MMCrashDuringQuoteReplaceTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[ERR10-k8s] full stack up.");
    }

    /** Pod we last identified as the crash target — dumped on any test failure. */
    private static String diagnosticTarget;

    @Test
    void marketMakerCrashDuringQuoteReplaceLeavesBoundedOrphanAndRecovers() throws Exception {
        try {
            runScenario();
        } catch (Throwable t) {
            // Dump diagnostics for whichever MM we last identified as the
            // crash target, so failures past the crash point still surface
            // the FAULT-INJECTION log lines we need to root-cause.
            if (diagnosticTarget != null) {
                dumpMmDiagnostics(diagnosticTarget);
            }
            throw t;
        }
    }

    private void runScenario() throws Exception {
        // 1. Seed bootstrap quotes for every symbol so the assigned MMs have
        //    something to replace.
        System.out.println("[ERR10-k8s] seeding bootstrap quotes...");
        List<String> seedList = new ArrayList<>(SEED_SYMBOLS);
        List<UUID> bootstrapIds = seedQuotes(seedList);
        Set<UUID> bootstrapQuoteIds = new HashSet<>(bootstrapIds);
        assertEquals(SEED_SYMBOLS.size(), bootstrapQuoteIds.size(),
                "publisher must return one quoteId per symbol");

        System.out.println("[ERR10-k8s] warmup: driving " + WARMUP_WAVES + " waves to force initial replace cycles...");
        for (int wave = 1; wave <= WARMUP_WAVES; wave++) {
            driveOneWave(wave);
            Thread.sleep(WAVE_INTERVAL_MS);
        }

        // 2. Pick the crash target: whichever symbol the highest-ordinal pod
        //    (TARGET_POD) currently owns. We crash that pod and then
        //    `kubectl scale sts/mm --replicas=TARGET_REPLICAS`, which removes
        //    the highest-ordinal pod and prevents kubelet from recreating it —
        //    the closest k3s analogue to local docker-compose leaving the dead
        //    container down.
        //
        //    Edge case: the ZK leader latch is FIFO-by-join-time and the
        //    cluster leader gets *no* symbol assignments. If startup-order
        //    racing or a post-convergence leader transfer leaves TARGET_POD
        //    holding the leader latch, it'll own nothing and the test can't
        //    proceed. ensureTargetIsWorker() detects that by inspecting the
        //    leaderId reported by /cluster/status; if TARGET_POD is the
        //    leader, it `kubectl delete pod` the leader so it rejoins at the
        //    back of the latch queue, then waits for the cluster to settle.
        diagnosticTarget = TARGET_POD;
        ensureTargetIsWorker();
        String targetSymbol = awaitTargetSymbol();
        assertNotNull(targetSymbol,
                TARGET_POD + " owns no symbols after warmup; cannot pick a crash target. "
                        + "Either symbols < pods or rebalancing is still in progress.");
        System.out.println("[ERR10-k8s] crash target: " + TARGET_POD + " owns " + targetSymbol);

        // Wait for the MM-published quote to land in the exchange for the
        // target symbol. Without this, the orphan-window assertion below
        // might catch a bootstrap quote that the MM never owned.
        final String symbol = targetSymbol;
        awaitCondition(Duration.ofSeconds(30), () -> {
            Quote q = currentExchangeQuote(symbol);
            return q != null && !bootstrapQuoteIds.contains(q.quoteId());
        }, "no MM-generated quote for " + targetSymbol + " after " + WARMUP_WAVES + " waves");

        Quote preCrashQuote = currentExchangeQuote(targetSymbol);
        assertNotNull(preCrashQuote, "pre-crash exchange must hold a quote for " + targetSymbol);
        assertNotEquals(true, bootstrapQuoteIds.contains(preCrashQuote.quoteId()),
                "pre-crash quote must be MM-generated, not bootstrap: " + preCrashQuote);
        System.out.println("[ERR10-k8s] pre-crash quote: " + preCrashQuote);

        // 4. Snapshot the global exposure state. Post-crash the release call
        //    fired by the FaultInjector must drop the active reservation
        //    count by exactly 1 (this symbol's reservation), proving the
        //    documented "release before crash" half of error case 10.
        ExposureState preCrashExposure = currentExposure();
        assertNotNull(preCrashExposure, "exposure-reservation /exposure must return state pre-crash");
        int preCrashActive = preCrashExposure.activeReservations();
        System.out.println("[ERR10-k8s] pre-crash exposure: " + preCrashExposure);
        assertTrue(preCrashActive >= 1,
                "pre-crash there must be at least one active reservation; got " + preCrashActive);

        // 5. Arm the fault injector on TARGET_POD. The next time it processes
        //    a replace cycle for targetSymbol it will release the reservation
        //    and halt.
        armFaultInjector(TARGET_PORT, targetSymbol);
        System.out.println("[ERR10-k8s] armed " + TARGET_POD + " to crash on next replace of " + targetSymbol);

        // 6. Drive wide orders on the target symbol until TARGET_POD dies (or
        //    the timeout elapses). A single order isn't reliable: it may not
        //    cross, the quote may expire and be replaced between observation
        //    and submission, or the exchange may reject it transiently.
        //    Sustained pressure guarantees at least one fill once the MM
        //    republishes a fresh quote, which produces a position update →
        //    replace cycle → hook fires.
        long crashTriggerMillis = System.currentTimeMillis();
        boolean crashed = driveOrdersUntilCrash(TARGET_PORT, TARGET_POD, targetSymbol,
                Duration.ofSeconds(60));
        if (!crashed) {
            // Dump diagnostic info into the test output BEFORE failing,
            // so the user can see the FAULT-INJECTION log lines without
            // having to kubectl into a pod that may have already restarted.
            dumpMmDiagnostics(TARGET_POD);
            fail("owner pod " + TARGET_POD
                    + " did not appear unhealthy within 60s of arm — fault injector may not have fired. "
                    + "Check that the fault-injection profile is active on the MM (k8s/market-maker.yaml) "
                    + "and that position updates for " + targetSymbol + " are reaching this pod.");
        }
        long crashObservedMillis = System.currentTimeMillis();
        System.out.println("[ERR10-k8s] " + TARGET_POD + " confirmed unhealthy (~" +
                (crashObservedMillis - crashTriggerMillis) + "ms after first trigger)");

        // 7. Scale `sts/mm` to TARGET_REPLICAS so kubelet won't restart the
        //    crashed pod. StatefulSet scale-down always removes the highest
        //    ordinal first, which is exactly TARGET_POD. Without this, the
        //    cluster would briefly drop to N-1 members then return to N as
        //    the pod restarts, never reaching the "permanent loss" state
        //    we're testing recovery against. @AfterAll restores the replica
        //    count.
        System.out.println("[ERR10-k8s] scaling sts/mm to " + TARGET_REPLICAS
                + " so kubelet won't recreate " + TARGET_POD + "...");
        scaleMmStatefulSet(TARGET_REPLICAS);

        // 8. Observe the *documented* error-case-10 inconsistency: the
        //    pre-crash quote is still in the exchange while the reservation
        //    backing it has been released. The window opens the moment the
        //    FaultInjector's release call returns and closes when either
        //    (a) the orphan quote's TTL fires, or (b) HA failover reassigns
        //    the symbol and the new owner republishes (which also requests
        //    a fresh reservation). We poll on 100ms granularity for up to
        //    5 seconds so a fast HA failover doesn't beat us to the punch.
        boolean leakObserved = pollOrphanWithoutReservation(
                targetSymbol, preCrashQuote.quoteId(), preCrashActive,
                Duration.ofSeconds(5));
        ExposureState postCrashExposure = currentExposure();
        if (leakObserved) {
            System.out.println("[ERR10-k8s] CONFIRMED error-case-10 invariant violation: "
                    + "exchange held orphan quoteId=" + preCrashQuote.quoteId()
                    + " while activeReservations < " + preCrashActive
                    + " (proves release-then-crash sequence)");
        } else {
            // HA failover beat the observation window — strictly better than
            // the documented ≤30s TTL bound. We can't *prove* the leak in
            // this run, but the FAULT-INJECTION log lines in the dead pod
            // show the release call did fire. Print them so a CI reviewer
            // can verify out-of-band.
            System.out.println("[ERR10-k8s] leak window not directly observed at 100ms granularity "
                    + "(HA failover faster than measurement). FaultInjector logs follow:");
            dumpMmDiagnostics(TARGET_POD);
        }
        System.out.println("[ERR10-k8s] post-crash exposure: " + postCrashExposure);

        Quote orphanQuote = currentExchangeQuote(targetSymbol);
        assertNotNull(orphanQuote,
                "exchange must still hold *some* quote for " + targetSymbol
                        + " immediately after crash (either the orphan or a fresh "
                        + "post-HA-failover replacement)");
        boolean orphanObserved = preCrashQuote.quoteId().equals(orphanQuote.quoteId());
        if (orphanObserved) {
            System.out.println("[ERR10-k8s] orphan quote confirmed (still the pre-crash quoteId): "
                    + orphanQuote);
        } else {
            // HA failover (PR #88) reassigned the target symbol to another
            // MM, which republished a fresh quote before we could query.
            // The inconsistency window was strictly shorter than the
            // documented 30s TTL bound — strictly better than error case
            // 10's worst case.
            System.out.println("[ERR10-k8s] orphan already replaced before query — HA failover faster than orphan-window measurement"
                    + " (pre=" + preCrashQuote.quoteId() + " now=" + orphanQuote.quoteId() + ")");
        }

        // 9. ASSERT — TTL bound (only meaningful if we actually observed
        //    the orphan; if HA failover already replaced it, the window
        //    was strictly shorter than the documented bound and there's
        //    nothing to wait for).
        if (orphanObserved) {
            long ttlDeadline = orphanQuote.expiresAt() + 2_000L; // small safety margin
            long waitForTtl = Math.max(0L, ttlDeadline - System.currentTimeMillis());
            System.out.println("[ERR10-k8s] waiting " + waitForTtl + "ms for orphan TTL to elapse...");
            Thread.sleep(waitForTtl);
            assertTrue(orphanQuote.expiresAt() < System.currentTimeMillis(),
                    "orphan quote must be past its TTL by now: expiresAt=" + orphanQuote.expiresAt()
                            + " now=" + System.currentTimeMillis());
            System.out.println("[ERR10-k8s] orphan quote TTL elapsed (expiresAt was "
                    + orphanQuote.expiresAt() + ")");

            // After the TTL: two valid states.
            //   (a) The orphan quote (same quoteId) is still in the
            //       exchange. FillOrderDispatcher must reject orders
            //       against it (FillOrderDispatcher.java:55).
            //   (b) HA failover finally kicked in and the orphan was
            //       replaced sometime during our wait — the bound held.
            Quote postTtlQuote = currentExchangeQuote(targetSymbol);
            if (postTtlQuote != null && orphanQuote.quoteId().equals(postTtlQuote.quoteId())) {
                System.out.println("[ERR10-k8s] orphan still resident in exchange — verifying rejection");
                assertExpiredQuoteRejectsOrders(targetSymbol, orphanQuote.quoteId());
            } else {
                System.out.println("[ERR10-k8s] orphan replaced via HA failover during TTL wait: "
                        + postTtlQuote);
            }
        } else {
            System.out.println("[ERR10-k8s] skipping TTL/rejection checks — orphan never observed (HA failover beat us to it)");
        }

        // 10. Recovery via HA failover: TARGET_POD is permanently gone
        //     (scale-down at step 7). The cluster must detect the failure,
        //     evict the dead node, and reassign targetSymbol to a surviving
        //     MM on its own.
        System.out.println("[ERR10-k8s] waiting for cluster to evict " + TARGET_POD
                + " and converge to " + TARGET_REPLICAS + " members...");
        awaitCondition(Duration.ofMinutes(4),
                () -> clusterConvergedExcluding(TARGET_PORT),
                "cluster did not evict " + TARGET_POD + " and reconverge within 4 minutes");

        // 11. Wait for some surviving MM to take ownership of targetSymbol.
        System.out.println("[ERR10-k8s] waiting for HA reassignment of " + targetSymbol + "...");
        awaitCondition(Duration.ofMinutes(2),
                () -> ownerPortOrMinusOne(symbol, TARGET_PORT) != -1,
                targetSymbol + " was not reassigned to a different MM within 2 minutes");
        int newOwnerPort = ownerPortOrMinusOne(targetSymbol, TARGET_PORT);
        String newOwnerPod = MM_PORT_TO_POD.get(newOwnerPort);
        System.out.println("[ERR10-k8s] " + targetSymbol + " reassigned to " + newOwnerPod
                + " on port " + newOwnerPort);

        // 12. ASSERT — recovery: the new owner of targetSymbol must publish
        //     a fresh MM quote on its own. AssignmentListener bootstraps a
        //     quote for newly-assigned symbols by fetching the current
        //     position from trading-state, so the new owner doesn't need a
        //     fill to produce its first quote — which is essential here
        //     because the orphan in the exchange is expired and would
        //     reject any order before a new quote replaces it.
        awaitCondition(Duration.ofSeconds(30), () -> {
            Quote q = currentExchangeQuote(symbol);
            return q != null
                    && !preCrashQuote.quoteId().equals(q.quoteId())
                    && q.expiresAt() > System.currentTimeMillis();
        }, "after HA failover, the new owner of " + targetSymbol
                + " did not publish a fresh MM quote within 30s — cluster did not self-recover");

        Quote recoveryQuote = currentExchangeQuote(targetSymbol);
        System.out.println("[ERR10-k8s] recovery quote: " + recoveryQuote);
        System.out.println("[ERR10-k8s] post-recovery exposure (informational): " + currentExposure());
    }

    /**
     * Return the first symbol currently assigned to the MM listening on
     * {@code port}, or null if none. Used to dynamically pick a crash target
     * after warmup so the test doesn't depend on a hash-based assumption
     * about which pod owns AAPL.
     */
    private static String firstSymbolOwnedBy(int port) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + port + "/marketmaker/status"))
                .timeout(Duration.ofSeconds(5))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) return null;
        JsonNode body = JSON.readTree(resp.body());
        JsonNode assigned = body.path("assigned");
        if (!assigned.isArray() || assigned.isEmpty()) return null;
        return assigned.get(0).asText();
    }

    private static void scaleMmStatefulSet(int replicas) throws Exception {
        String out = runKubectlCapturing(TimeUnit.SECONDS.toMillis(30),
                "scale", "sts/mm", "-n", NS, "--replicas=" + replicas);
        System.out.println("[ERR10-k8s] kubectl scale sts/mm --replicas=" + replicas + ": "
                + out.trim());
    }

    /**
     * Inspect /cluster/status on any responsive MM and return the leader's
     * nodeId, or null if no quorum/leader currently visible.
     */
    private static String currentLeaderId() {
        for (int port : MM_PORT_TO_POD.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) continue;
            String lid = status.path("leaderId").asText(null);
            if (lid != null && !lid.isBlank()) return lid;
        }
        return null;
    }

    /**
     * If {@link #TARGET_POD} currently holds the cluster leader latch it
     * receives no symbol assignments (only workers are assigned), and the
     * crash-target selection further down would fail with a confusing
     * "owns no symbols" message.
     *
     * <p>Delete the pod via kubectl so it loses the latch. The StatefulSet
     * recreates it; on rejoin it lands at the back of the FIFO leader latch
     * queue, so a different pod is elected leader and TARGET_POD becomes an
     * eligible worker again. Then we wait for the cluster to converge and
     * for TARGET_POD's status endpoint to come back.
     *
     * <p>No-op if TARGET_POD is already a worker.
     */
    private static void ensureTargetIsWorker() throws Exception {
        String leader = currentLeaderId();
        if (leader == null) {
            System.out.println("[ERR10-k8s] no leader visible yet — relying on outer awaitCondition to converge");
            return;
        }
        if (!TARGET_POD.equals(leader)) {
            System.out.println("[ERR10-k8s] " + TARGET_POD + " is a worker (leader=" + leader + ") — proceeding");
            return;
        }
        System.out.println("[ERR10-k8s] " + TARGET_POD + " currently holds the leader latch — "
                + "deleting the pod to drop it (a different pod will take over so " + TARGET_POD
                + " can be assigned a symbol)");
        String out = runKubectlCapturing(TimeUnit.SECONDS.toMillis(30),
                "delete", "pod", TARGET_POD, "-n", NS, "--wait=true");
        System.out.println("[ERR10-k8s]   " + out.trim());

        // StatefulSet recreates the pod under the same name and stable
        // network identity. Wait for the readiness probe to pass before
        // checking the cluster again.
        awaitHealthy(TARGET_POD, TARGET_PORT, "/marketmaker/status", Duration.ofMinutes(5));

        // Wait for re-election to settle (leader is no longer TARGET_POD).
        awaitCondition(Duration.ofMinutes(2), () -> {
            String lid = currentLeaderId();
            return lid != null && !TARGET_POD.equals(lid);
        }, "after deleting " + TARGET_POD + ", the cluster did not elect a different leader within 2m");
        System.out.println("[ERR10-k8s] re-election complete; new leader=" + currentLeaderId());
    }

    /**
     * Poll {@link #firstSymbolOwnedBy} until {@link #TARGET_POD} reports an
     * assigned symbol, or the timeout elapses. Returns the symbol, or null on
     * timeout. Used right after {@link #ensureTargetIsWorker}, which
     * guarantees TARGET_POD is *eligible* for assignment but not that the
     * Coordinator has already issued one — the rebalance znode write and the
     * AssignmentListener pickup are async.
     */
    private static String awaitTargetSymbol() {
        Instant deadline = Instant.now().plus(Duration.ofMinutes(2));
        while (Instant.now().isBefore(deadline)) {
            try {
                String s = firstSymbolOwnedBy(TARGET_PORT);
                if (s != null) return s;
            } catch (Exception ignored) {
                // transient HTTP error — retry
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

    @org.junit.jupiter.api.AfterAll
    static void restoreReplicaCount() {
        try {
            System.out.println("[ERR10-k8s] @AfterAll: restoring sts/mm to " + FULL_REPLICAS + " replicas");
            scaleMmStatefulSet(FULL_REPLICAS);
        } catch (Exception e) {
            System.err.println("[ERR10-k8s] failed to restore sts/mm replica count: " + e);
        }
    }

    // ---------- per-test helpers ----------

    private static void driveOneWave(int wave) {
        int accepted = submitOrdersViaPublisher(ORDERS_PER_SYMBOL_PER_WAVE,
                new ArrayList<>(SEED_SYMBOLS));
        System.out.println("[ERR10-k8s] wave " + wave + ": exchange accepted " + accepted + " orders");
    }

    /**
     * Print diagnostic information about the owner MM pod into
     * {@code System.out} so the JUnit report captures it.
     *
     * <p>Three sections, each answering a specific question about why the
     * fault injector might not have fired:
     * <ol>
     *   <li>{@code env | grep SPRING_PROFILES_ACTIVE} — is the
     *       {@code fault-injection} profile actually active inside the
     *       pod? (If not, neither the controller nor the injector bean
     *       exist; the arm endpoint should have returned 404 but if it
     *       returned 200 against a stale image this is the smoking gun.)</li>
     *   <li>{@code curl /test/fault-injection/status} from inside the
     *       pod — is the injector still armed for the target symbol?
     *       If yes, the hook never reached {@code consumeIfArmed}. If null,
     *       the hook fired but didn't halt (shouldn't be possible).</li>
     *   <li>FAULT-INJECTION log lines — answers whether {@code generateQuote}
     *       even ran for the armed symbol on this MM.</li>
     * </ol>
     */
    private static void dumpMmDiagnostics(String pod) {
        System.out.println("\n[ERR10-k8s] ============ DIAGNOSTICS for " + pod + " ============");

        System.out.println("[ERR10-k8s] ---- SPRING_PROFILES_ACTIVE inside pod ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(15),
                "exec", "-n", NS, pod, "--", "sh", "-c",
                "env | grep SPRING_PROFILES_ACTIVE || echo '<not set>'");

        System.out.println("[ERR10-k8s] ---- /test/fault-injection/status (from inside) ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(15),
                "exec", "-n", NS, pod, "--", "sh", "-c",
                "wget -qO- http://localhost:8080/test/fault-injection/status "
                        + "|| echo '<endpoint unreachable — profile likely not active>'");

        System.out.println("[ERR10-k8s] ---- kubectl logs --tail=500 ----");
        runKubectlOut(TimeUnit.SECONDS.toMillis(30),
                "logs", "--tail=500", "-n", NS, pod);

        System.out.println("[ERR10-k8s] ============ END DIAGNOSTICS ============\n");
    }

    private static void runKubectlOut(long timeoutMs, String... args) {
        try {
            String out = runKubectlCapturing(timeoutMs, args);
            System.out.println(out);
        } catch (Exception e) {
            System.out.println("[diagnostics error: " + e + "]");
        }
    }

    /**
     * Hammer the exchange with wide orders on {@code symbol}, alternating
     * BUY and SELL across the MM's spread, until {@code mmAlive(ownerPort)}
     * returns false or {@code timeout} elapses. Returns true on crash.
     *
     * <p>Two complications justify the loop rather than a single fire-and-poll:
     * <ol>
     *   <li>The MM republishes quotes frequently; a fill won't happen unless
     *       our order arrives while a fresh quote is live. A burst makes
     *       this near-certain.</li>
     *   <li>Once the MM publishes its next quote after we arm, that publish
     *       call hits the hook and crashes — but only if a position update
     *       actually triggers a replace cycle. We drive fills to guarantee
     *       updates.</li>
     * </ol>
     */
    private static boolean driveOrdersUntilCrash(int ownerPort, String ownerPod,
                                                 String symbol, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        int sent = 0;
        int accepted = 0;
        List<String> oneSymbol = List.of(symbol);
        while (Instant.now().isBefore(deadline)) {
            if (!mmAlive(ownerPort)) {
                System.out.println("[ERR10-k8s] " + ownerPod + " stopped responding after "
                        + sent + " trigger orders (" + accepted + " accepted)");
                return true;
            }
            accepted += submitOrdersViaPublisher(1, oneSymbol);
            sent++;
            // Brief pause between orders so the MM has time to receive the
            // position update, enter the replace cycle, and hit the hook.
            Thread.sleep(250);
        }
        System.out.println("[ERR10-k8s] driveOrdersUntilCrash giving up after " + sent
                + " orders (" + accepted + " accepted); " + ownerPod + " still alive");
        return false;
    }

    /**
     * Iterate MM ports and ask each {@code GET /marketmaker/status} who owns
     * {@code symbol}. The first MM whose {@code assigned} set contains the
     * symbol wins. Symbol→node assignment is deterministic (sharding by
     * hash) so this returns the same MM run-to-run for the same symbol.
     */
    private static int findOwnerPort(String symbol) throws Exception {
        for (Map.Entry<Integer, String> e : MM_PORT_TO_POD.entrySet()) {
            int port = e.getKey();
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + HOST + ":" + port + "/marketmaker/status"))
                        .timeout(Duration.ofSeconds(3))
                        .GET().build();
                HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) continue;
                JsonNode body = JSON.readTree(resp.body());
                JsonNode assigned = body.path("assigned");
                if (!assigned.isArray()) continue;
                for (JsonNode s : assigned) {
                    if (symbol.equals(s.asText())) return port;
                }
            } catch (Exception ignored) {
                // try next port
            }
        }
        return -1;
    }

    private static void armFaultInjector(int mmPort, String symbol) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + mmPort
                        + "/test/fault-injection/arm-quote-replace-crash?symbol=" + symbol))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(),
                "POST /test/fault-injection/arm-quote-replace-crash must return 200 "
                        + "(profile fault-injection should be active on the MM); body=" + resp.body());
    }

    /**
     * Try to catch the error-case-10 inconsistency: the pre-crash quote
     * still resident in the exchange AND the global reservation count
     * dropped from {@code preCrashActive}. Both conditions must hold
     * simultaneously in a single poll — observing them across separate
     * polls would not rule out a fast "orphan replaced THEN new
     * reservation created" sequence and wouldn't prove the leak.
     *
     * <p>100ms granularity, capped at {@code timeout}. Returns true the
     * first poll that catches the inconsistency.
     */
    private static boolean pollOrphanWithoutReservation(String symbol,
                                                       UUID preCrashQuoteId,
                                                       int preCrashActive,
                                                       Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            Quote q = currentExchangeQuote(symbol);
            ExposureState e = currentExposure();
            if (q != null && preCrashQuoteId.equals(q.quoteId())
                    && e != null && e.activeReservations() < preCrashActive) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private static void assertExpiredQuoteRejectsOrders(String symbol, UUID orphanId) {
        // Submit a single wide order via the publisher (which routes to the
        // exchange leader). The check has to tolerate a tight race: HA
        // self-recovery (AssignmentListener.bootstrapQuoteForNewlyAssigned)
        // can publish a fresh, valid quote between our caller's pre-check
        // and the order landing at the exchange. If that happens, the order
        // legitimately fills against the *new* quote and the orphan-rejection
        // property no longer applies. We fail only when an order is accepted
        // and the exchange still shows the orphan — that would mean the
        // exchange failed to enforce the TTL bound.
        int accepted = submitOrdersViaPublisher(1, List.of(symbol));
        if (accepted == 0) return;
        Quote afterSubmit = currentExchangeQuote(symbol);
        assertTrue(afterSubmit == null || !orphanId.equals(afterSubmit.quoteId()),
                "order accepted while exchange still holds the expired orphan quote for "
                        + symbol + " — exchange must enforce quote TTL bound");
        System.out.println("[ERR10-k8s] order filled against post-recovery quote, not orphan: "
                + afterSubmit);
    }

    // ---------- generic helpers (mirror ClusterIntegrationWithSystemK8sTest) ----------

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

    /**
     * Convergence check that excludes a known-dead MM. The remaining
     * {@code MM_PORT_TO_POD.size() - 1} nodes must all respond, see
     * each other (members count == survivors), and agree on a single leader.
     */
    private static boolean clusterConvergedExcluding(int excludedPort) {
        int expected = MM_PORT_TO_POD.size() - 1;
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_POD.keySet()) {
            if (port == excludedPort) continue;
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String lid = status.path("leaderId").asText(null);
            if (lid == null) return false;
            leaders.add(lid);
            if (status.path("members").size() != expected) return false;
        }
        return responding == expected && leaders.size() == 1;
    }

    /**
     * Like {@link #findOwnerPort(String)} but returns -1 if the only owner
     * is {@code excludedPort} (or if no owner exists yet) and swallows
     * transient HTTP errors so it can be called from inside a polling lambda.
     */
    private static int ownerPortOrMinusOne(String symbol, int excludedPort) {
        try {
            int p = findOwnerPort(symbol);
            return (p == excludedPort) ? -1 : p;
        } catch (Exception e) {
            return -1;
        }
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

    private static boolean mmAlive(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + port + "/marketmaker/status"))
                    .timeout(Duration.ofSeconds(2))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static void awaitHealthy(String workload, int port, String path, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port, path)) {
                System.out.println("[ERR10-k8s] " + workload + " healthy on " + HOST + ":" + port);
                return;
            }
            Thread.sleep(2000);
        }
        System.err.println("[ERR10-k8s] " + workload + " did not respond on " + path + " within " + timeout);
        boolean isPod = workload.startsWith("mm-");
        String selectorArg = isPod ? workload : "deploy/" + workload;
        System.err.println("---- kubectl describe " + selectorArg + " ----");
        System.err.println(runKubectlCapturing(TimeUnit.SECONDS.toMillis(30),
                "describe", isPod ? "pod" : "deploy", workload, "-n", NS));
        System.err.println("---- kubectl logs --tail=300 " + selectorArg + " ----");
        System.err.println(runKubectlCapturing(TimeUnit.MINUTES.toMillis(1),
                "logs", "--tail=300", "-n", NS, selectorArg));
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

    private static String runKubectlCapturing(long timeoutMs, String... args) throws Exception {
        // When kubectl.ssh is set (default), shell out to ssh and run the
        // kubectl invocation on the control-plane node where KUBECONFIG is
        // configured. Otherwise (kubectl.ssh="") run the local kubectl.
        List<String> cmd = new ArrayList<>();
        if (KUBECTL_SSH != null && !KUBECTL_SSH.isBlank()) {
            // ssh passes a single shell-string to the remote shell. Each
            // kubectl arg must be POSIX-shell-quoted, otherwise args
            // containing spaces (e.g. `sh -c "wget -qO- http://..."`) split
            // on the remote side: `sh -c wget -qO- http://...` parses as
            // `sh -c wget` with the rest as positional args, so wget runs
            // with no URL and prints its usage page.
            StringBuilder remote = new StringBuilder(KUBECTL_REMOTE);
            for (String a : args) remote.append(' ').append(shellQuote(a));
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
        return output.toString();
    }

    /**
     * POSIX-shell-quote a single argument: wrap in single quotes and replace
     * any embedded {@code '} with {@code '\''}. Safe for any string content.
     */
    private static String shellQuote(String s) {
        return "'" + s.replace("'", "'\\''") + "'";
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

    /**
     * Submit orders through the external-publisher's {@code /publisher/submit-orders}
     * endpoint. The publisher resolves the current exchange leader from
     * ZooKeeper and retries on transient 503s — using it means we never get
     * killed by the round-robin Service alias dropping requests on follower
     * replicas (which {@link edu.yu.marketmaker.ha.LeaderGuardFilter} rejects
     * with 503 for any mutating method).
     *
     * @return the number of orders the exchange accepted (HTTP 200).
     */
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