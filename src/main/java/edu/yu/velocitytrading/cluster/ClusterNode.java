package edu.yu.velocitytrading.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Cluster membership and identity for a single market-maker JVM.
 *
 * On startup this bean:
 * <ol>
 *   <li>Ensures the base znodes exist.</li>
 *   <li>Creates an ephemeral-sequential znode under {@code /marketmaker/members/};
 *       its short name (e.g. {@code n-0000000004}) is the node's id for the
 *       life of the JVM.</li>
 *   <li>Joins a Curator {@link LeaderLatch} so exactly one node is leader at
 *       any time.</li>
 *   <li>Starts a {@link CuratorCache} over {@code /marketmaker/members} for
 *       other components (notably the Coordinator) to listen on.</li>
 * </ol>
 *
 * Liveness comes from ZK's ephemeral nodes — no separate heartbeat. When
 * this JVM dies, ZK reaps the member znode and latch entry on session timeout.
 */
@Component
@Profile("market-maker-node")
public class ClusterNode {

    private static final Logger log = LoggerFactory.getLogger(ClusterNode.class);

    private final CuratorFramework curator;
    private final ZkPaths paths;
    private final ClusterProperties props;
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile String nodeId;
    private volatile String memberZnodePath;
    private LeaderLatch leaderLatch;
    private CuratorCache membersCache;
    private PersistentNode persistentMember;

    public ClusterNode(CuratorFramework curator, ZkPaths paths, ClusterProperties props) {
        this.curator = curator;
        this.paths = paths;
        this.props = props;
    }

    /**
     * Registers membership and starts the leader latch. Runs at bean
     * construction time so that other beans (notably the Coordinator) can
     * safely read {@link #getLeaderLatch()} and {@link #getMembersCache()}
     * from their own {@code ApplicationRunner.run()} methods without racing
     * an arbitrary Spring-driven initialization order.
     *
     * @throws Exception if any ZK operation fails
     */
    @PostConstruct
    public void init() throws Exception {
        ensurePath(paths.base());
        ensurePath(paths.members());
        ensurePath(paths.assignments());

        byte[] memberData = mapper.writeValueAsBytes(Map.of(
                "host", props.getAdvertiseHost(),
                "forwardPort", props.getForwardPort()));

        String configuredId = props.getNodeId();
        if (configuredId != null && !configuredId.isBlank()) {
            // Stable id: deterministic znode path that Curator's PersistentNode
            // recreates after every ZK reconnect. Without this, a session loss
            // (or a ZK pod restart) silently strips this node from
            // /marketmaker/members and the next rebalance excludes it.
            this.nodeId = configuredId.trim();
            this.memberZnodePath = paths.members() + "/" + nodeId;
            this.persistentMember = new PersistentNode(
                    curator, CreateMode.EPHEMERAL, /*useProtection*/ false,
                    memberZnodePath, memberData);
            this.persistentMember.start();
            if (!this.persistentMember.waitForInitialCreate(30, TimeUnit.SECONDS)) {
                throw new ClusterException("timed out registering member znode at " + memberZnodePath);
            }
            log.info("registered cluster member id={} at {} (persistent)", nodeId, memberZnodePath);
        } else {
            // Legacy fallback: ephemeral-sequential, used by unit tests against
            // a TestingServer that don't set cluster.node-id. Loses re-registration
            // on session reset, but those tests don't exercise reconnect.
            this.memberZnodePath = curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(paths.memberNodePrefix(), memberData);
            this.nodeId = memberZnodePath.substring(memberZnodePath.lastIndexOf('/') + 1);
            log.info("registered cluster member id={} at {} (ephemeral-sequential)", nodeId, memberZnodePath);
        }

        this.membersCache = CuratorCache.build(curator, paths.members());
        this.membersCache.start();

        this.leaderLatch = new LeaderLatch(curator, paths.election(), nodeId);
        this.leaderLatch.start();
        log.info("leader latch started for nodeId={}", nodeId);
    }

    /**
     * @return this JVM's cluster id (the ephemeral-sequential suffix)
     * @throws IllegalStateException if called before {@link #init()}
     */
    public String getNodeId() {
        if (nodeId == null) {
            throw new IllegalStateException("cluster node not yet registered");
        }
        return nodeId;
    }

    /** @return {@code true} if this JVM holds the leader latch. */
    public boolean isLeader() {
        return leaderLatch != null && leaderLatch.hasLeadership();
    }

    /**
     * @return the Curator {@link LeaderLatch} so callers can attach listeners
     *         or query participants. Non-null after {@link #init()}.
     */
    public LeaderLatch getLeaderLatch() {
        return leaderLatch;
    }

    /**
     * @return the {@link CuratorCache} over {@code /marketmaker/members}; the
     *         Coordinator attaches a listener here to detect membership churn.
     *         Non-null after {@link #init()}.
     */
    public CuratorCache getMembersCache() {
        return membersCache;
    }

    /**
     * Read live members directly from ZK (bypassing the cache). Used by the
     * Coordinator at rebalance time for an authoritative snapshot.
     *
     * @return live member ids, sorted
     * @throws ClusterException if ZK cannot be queried
     */
    public Set<String> getLiveMembers() {
        Set<String> ids = new TreeSet<>();
        try {
            List<String> children = curator.getChildren().forPath(paths.members());
            ids.addAll(children);
        } catch (KeeperException.NoNodeException e) {
            return ids;
        } catch (Exception e) {
            throw new ClusterException("failed to list members", e);
        }
        return ids;
    }

    /**
     * Look up the forwarding endpoint that peer {@code nodeId} advertised in
     * its member znode. Returns empty if the node is gone or its payload is
     * unreadable (e.g. legacy empty payload).
     */
    public Optional<ForwardEndpoint> forwardEndpointOf(String nodeId) {
        try {
            byte[] data = curator.getData().forPath(paths.members() + "/" + nodeId);
            if (data == null || data.length == 0) return Optional.empty();
            JsonNode json = mapper.readTree(data);
            JsonNode host = json.get("host");
            JsonNode port = json.get("forwardPort");
            if (host == null || port == null) return Optional.empty();
            return Optional.of(new ForwardEndpoint(host.asText(), port.asInt()));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (Exception e) {
            log.warn("failed to read forward endpoint for {}: {}", nodeId, e.toString());
            return Optional.empty();
        }
    }

    /**
     * Idempotently create a persistent znode (and parents). Tolerates races
     * with concurrent creators.
     */
    private void ensurePath(String path) throws Exception {
        if (curator.checkExists().forPath(path) == null) {
            try {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            } catch (KeeperException.NodeExistsException ignored) {
                // another node beat us to it
            }
        }
    }

    /**
     * Shutdown hook: close the latch and members cache, then proactively
     * delete our member znode so peers see us leave immediately instead of
     * waiting for session timeout.
     */
    @PreDestroy
    public void shutdown() {
        log.info("cluster node shutting down");
        try {
            if (leaderLatch != null) {
                leaderLatch.close(LeaderLatch.CloseMode.NOTIFY_LEADER);
            }
        } catch (Exception e) {
            log.warn("error closing leader latch", e);
        }
        try {
            if (membersCache != null) {
                membersCache.close();
            }
        } catch (Exception e) {
            log.warn("error closing members cache", e);
        }
        // PersistentNode closes its own ephemeral on shutdown. For the legacy
        // path we still delete explicitly so peers see us leave immediately.
        if (persistentMember != null) {
            try {
                persistentMember.close();
            } catch (Exception e) {
                log.debug("error closing persistent member node", e);
            }
        } else {
            try {
                if (memberZnodePath != null && curator.getZookeeperClient().isConnected()) {
                    curator.delete().guaranteed().forPath(memberZnodePath);
                }
            } catch (Exception e) {
                log.debug("member znode delete on shutdown failed (likely already gone)", e);
            }
        }
    }
}
