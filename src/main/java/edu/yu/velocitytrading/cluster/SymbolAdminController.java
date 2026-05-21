package edu.yu.velocitytrading.cluster;

import org.apache.curator.framework.recipes.leader.Participant;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Operator HTTP API for inspecting cluster state and editing the symbol list.
 *
 * Endpoints:
 * <ul>
 *   <li>{@code GET  /cluster/status} — any node; returns this node's view of
 *       membership, leadership, and the symbol list.</li>
 *   <li>{@code POST /cluster/symbols} — leader-only; adds a symbol.</li>
 *   <li>{@code DELETE /cluster/symbols/{symbol}} — leader-only; removes one.</li>
 * </ul>
 *
 * Mutations against a non-leader return 503 with an {@code X-Leader} header
 * naming the current leader so a client can retry on the right node.
 */
@RestController
@RequestMapping("/cluster")
@Profile("market-maker-node")
public class SymbolAdminController {

    private final ClusterNode clusterNode;
    private final ConfigStore configStore;

    public SymbolAdminController(ClusterNode clusterNode, ConfigStore configStore) {
        this.clusterNode = clusterNode;
        this.configStore = configStore;
    }

    /** Request body for {@code POST /cluster/symbols}. */
    public record SymbolRequest(String symbol) {}

    /**
     * Response body for {@code GET /cluster/status}.
     *
     * @param nodeId   this node's cluster id (e.g. "n-0000000003")
     * @param leader   {@code true} if this node is the leader
     * @param leaderId id of the current leader (null during a transition)
     * @param members  ids of all live members at the time of the call
     * @param symbols  current symbol list from ZK
     */
    public record StatusView(String nodeId,
                             boolean leader,
                             String leaderId,
                             Set<String> members,
                             List<String> symbols) {}

    /**
     * Snapshot the cluster state as seen from this node. Available on every
     * node (handy for round-robin debugging).
     */
    @GetMapping("/status")
    public StatusView status() {
        return new StatusView(
                clusterNode.getNodeId(),
                clusterNode.isLeader(),
                currentLeaderId(),
                clusterNode.getLiveMembers(),
                configStore.readSymbols()
        );
    }

    /**
     * Append a symbol. Leader-only; the znode change triggers a rebalance.
     *
     * @return 200 with the new symbol list, 400 on missing symbol, or 503
     *         with an {@code X-Leader} header if this node is not the leader
     */
    @PostMapping("/symbols")
    public ResponseEntity<?> addSymbol(@RequestBody SymbolRequest req) {
        ResponseEntity<?> leaderGuard = requireLeader();
        if (leaderGuard != null) return leaderGuard;
        if (req == null || req.symbol() == null || req.symbol().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "symbol required"));
        }
        boolean added = configStore.addSymbol(req.symbol());
        return ResponseEntity.ok(Map.of(
                "symbol", req.symbol().trim().toUpperCase(),
                "added", added,
                "symbols", configStore.readSymbols()
        ));
    }

    /**
     * Remove a symbol. Leader-only; the znode change triggers a rebalance.
     *
     * @return 200 with the new symbol list, or 503 with an {@code X-Leader}
     *         header if this node is not the leader
     */
    @DeleteMapping("/symbols/{symbol}")
    public ResponseEntity<?> removeSymbol(@PathVariable String symbol) {
        ResponseEntity<?> leaderGuard = requireLeader();
        if (leaderGuard != null) return leaderGuard;
        boolean removed = configStore.removeSymbol(symbol);
        return ResponseEntity.ok(Map.of(
                "symbol", symbol.trim().toUpperCase(),
                "removed", removed,
                "symbols", configStore.readSymbols()
        ));
    }

    /**
     * @return {@code null} if this node is leader (caller may proceed), or a
     *         503 response naming the current leader otherwise
     */
    private ResponseEntity<?> requireLeader() {
        if (clusterNode.isLeader()) return null;
        String leader = currentLeaderId();
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("X-Leader", leader == null ? "" : leader)
                .body(Map.of(
                        "error", "not leader",
                        "leader", leader == null ? "" : leader,
                        "thisNode", clusterNode.getNodeId()
                ));
    }

    /**
     * @return the current leader's id from the Curator LeaderLatch, or
     *         {@code null} if indeterminate (mid-failover or ZK unreachable)
     */
    private String currentLeaderId() {
        try {
            Collection<Participant> participants = clusterNode.getLeaderLatch().getParticipants();
            if (participants == null) return null;
            return participants.stream()
                    .filter(Participant::isLeader)
                    .map(Participant::getId)
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            return null;
        }
    }
}
