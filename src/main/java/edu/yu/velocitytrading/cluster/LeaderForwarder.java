package edu.yu.velocitytrading.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.velocitytrading.model.StateSnapshot;
import jakarta.annotation.PreDestroy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Leader-only: subscribes to the trading-state RSocket
 * {@code state.stream} and fans each {@link StateSnapshot} out to the single
 * worker assigned to that symbol.
 *
 * Transport is raw TCP, one newline-delimited JSON frame per snapshot, per
 * the deployment design: the leader fires-and-forgets; workers never ACK and
 * the leader never learns what the worker did with the update.
 *
 * Leadership state drives all resources:
 * <ul>
 *   <li>On acquire: start an assignments {@link CuratorCache} to track the
 *       symbol&rarr;owner map, subscribe to {@code state.stream} with an
 *       infinite retry backoff (so the trading-state service can come up
 *       late without wedging us), and lazily open one TCP sender per
 *       worker.</li>
 *   <li>On lose: dispose the subscription, close the cache, tear down
 *       sender threads and sockets.</li>
 * </ul>
 * Every field that lives across a leadership cycle is guarded by
 * {@link #leading} so spurious duplicate latch events don't double-install
 * watchers or leak threads.
 */
@Component
@Profile("market-maker-node")
public class LeaderForwarder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LeaderForwarder.class);
    private static final String TRADING_STATE_HOST = "trading-state";
    private static final int TRADING_STATE_RSOCKET_PORT = 7000;
    private static final Duration STREAM_RECONNECT_DELAY = Duration.ofSeconds(2);

    private final CuratorFramework curator;
    private final ZkPaths paths;
    private final ClusterNode clusterNode;
    private final RSocketRequester.Builder rsocketBuilder;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicBoolean leading = new AtomicBoolean(false);

    /** symbol -> owning worker nodeId. Rebuilt on every assignments znode event. */
    private final ConcurrentHashMap<String, String> symbolToNodeId = new ConcurrentHashMap<>();
    /** nodeId -> per-worker sender thread + socket. */
    private final ConcurrentHashMap<String, Sender> senders = new ConcurrentHashMap<>();

    private CuratorCache assignmentsCache;
    private CuratorCacheListener assignmentsListener;
    private volatile Disposable stateSubscription;

    public LeaderForwarder(CuratorFramework curator,
                           ZkPaths paths,
                           ClusterNode clusterNode,
                           RSocketRequester.Builder rsocketBuilder) {
        this.curator = curator;
        this.paths = paths;
        this.clusterNode = clusterNode;
        this.rsocketBuilder = rsocketBuilder;
    }

    @Override
    public void run(ApplicationArguments args) {
        clusterNode.getLeaderLatch().addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                onAcquireLeadership();
            }

            @Override
            public void notLeader() {
                onLoseLeadership();
            }
        });
        if (clusterNode.getLeaderLatch().hasLeadership()) {
            onAcquireLeadership();
        }
    }

    private synchronized void onAcquireLeadership() {
        if (!leading.compareAndSet(false, true)) return;
        log.info("LeaderForwarder: acquired leadership; starting state-stream subscription");

        this.assignmentsCache = CuratorCache.build(curator, paths.assignments());
        this.assignmentsListener = (type, oldData, data) -> refreshAssignments();
        this.assignmentsCache.listenable().addListener(assignmentsListener);
        this.assignmentsCache.start();
        refreshAssignments();

        // Flux.defer so every retry actually reconnects (a fresh requester per
        // attempt) instead of riding a dead TCP connection.
        this.stateSubscription = Flux.defer(() ->
                        rsocketBuilder.tcp(TRADING_STATE_HOST, TRADING_STATE_RSOCKET_PORT)
                                .route("state.stream")
                                .retrieveFlux(StateSnapshot.class))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, STREAM_RECONNECT_DELAY)
                        .doBeforeRetry(sig -> log.warn(
                                "state.stream subscription error, retrying: {}",
                                sig.failure().toString())))
                .subscribe(this::dispatch,
                        err -> log.error("state.stream subscription terminated", err));
    }

    private synchronized void onLoseLeadership() {
        if (!leading.compareAndSet(true, false)) return;
        log.info("LeaderForwarder: lost leadership; tearing down");

        if (stateSubscription != null) {
            stateSubscription.dispose();
            stateSubscription = null;
        }
        if (assignmentsCache != null) {
            if (assignmentsListener != null) {
                assignmentsCache.listenable().removeListener(assignmentsListener);
                assignmentsListener = null;
            }
            assignmentsCache.close();
            assignmentsCache = null;
        }
        symbolToNodeId.clear();
        senders.values().forEach(Sender::close);
        senders.clear();
    }

    /** Rebuild {@link #symbolToNodeId} from every assignment znode. */
    private void refreshAssignments() {
        if (!leading.get()) return;
        try {
            List<String> children;
            try {
                children = curator.getChildren().forPath(paths.assignments());
            } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                children = List.of();
            }
            Map<String, String> next = new HashMap<>();
            for (String nodeId : children) {
                byte[] data;
                try {
                    data = curator.getData().forPath(paths.assignmentFor(nodeId));
                } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                    continue;
                }
                if (data == null || data.length == 0) continue;
                List<String> syms = mapper.readValue(data, new TypeReference<List<String>>() {});
                for (String s : syms) next.put(s, nodeId);
            }
            symbolToNodeId.clear();
            symbolToNodeId.putAll(next);
            log.debug("LeaderForwarder: assignment map refreshed ({} symbols)", next.size());
        } catch (Exception e) {
            log.warn("LeaderForwarder: failed to refresh assignments: {}", e.toString());
        }
    }

    /** Serialize once and hand off to the per-worker sender. */
    private void dispatch(StateSnapshot snap) {
        if (!leading.get()) return;
        if (snap == null || snap.position() == null || snap.position().symbol() == null) return;
        String symbol = snap.position().symbol();
        String owner = symbolToNodeId.get(symbol);
        if (owner == null) {
            // no worker currently owns this symbol — drop (fire-and-forget)
            return;
        }
        byte[] frame;
        try {
            frame = mapper.writeValueAsBytes(snap);
        } catch (Exception e) {
            log.warn("LeaderForwarder: failed to serialize snapshot for {}: {}", symbol, e.toString());
            return;
        }
        Sender sender = senders.computeIfAbsent(owner, this::openSender);
        if (sender != null) {
            sender.offer(frame);
        }
    }

    private Sender openSender(String nodeId) {
        Optional<ForwardEndpoint> endpoint = clusterNode.forwardEndpointOf(nodeId);
        if (endpoint.isEmpty()) {
            log.warn("LeaderForwarder: no forward endpoint advertised for {}", nodeId);
            return null;
        }
        return new Sender(nodeId, endpoint.get());
    }

    @PreDestroy
    public void shutdown() {
        onLoseLeadership();
    }

    /**
     * One thread per destination worker: drains a bounded queue of pre-
     * serialized frames to a single TCP socket. On write failure, closes the
     * socket and reconnects with a short sleep — fire-and-forget, so we
     * tolerate drops rather than block the dispatcher.
     */
    private static final class Sender {

        private static final int QUEUE_CAPACITY = 4096;
        private static final int CONNECT_TIMEOUT_MS = 3_000;
        private static final long RECONNECT_BACKOFF_MS = 500;

        private final String nodeId;
        private final ForwardEndpoint endpoint;
        private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        private final Thread thread;
        private volatile boolean running = true;

        Sender(String nodeId, ForwardEndpoint endpoint) {
            this.nodeId = nodeId;
            this.endpoint = endpoint;
            this.thread = new Thread(this::run, "forward-sender-" + nodeId);
            this.thread.setDaemon(true);
            this.thread.start();
        }

        void offer(byte[] frame) {
            if (!queue.offer(frame)) {
                log.debug("sender queue full for {}, dropping frame", nodeId);
            }
        }

        private void run() {
            Socket socket = null;
            OutputStream out = null;
            while (running) {
                try {
                    if (socket == null) {
                        socket = new Socket();
                        socket.connect(
                                new InetSocketAddress(endpoint.host(), endpoint.forwardPort()),
                                CONNECT_TIMEOUT_MS);
                        out = new BufferedOutputStream(socket.getOutputStream());
                    }
                    byte[] frame = queue.poll(1, TimeUnit.SECONDS);
                    if (frame == null || out == null) continue;
                    out.write(frame);
                    out.write('\n');
                    out.flush();
                } catch (IOException e) {
                    log.warn("forward to {} at {}:{} failed: {}",
                            nodeId, endpoint.host(), endpoint.forwardPort(), e.toString());
                    closeQuietly(socket);
                    socket = null;
                    out = null;
                    sleep(RECONNECT_BACKOFF_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            closeQuietly(socket);
        }

        private static void closeQuietly(Socket s) {
            if (s != null) {
                try { s.close(); } catch (IOException ignored) {}
            }
        }

        private static void sleep(long ms) {
            try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        void close() {
            running = false;
            thread.interrupt();
        }
    }
}
