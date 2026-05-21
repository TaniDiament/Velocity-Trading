package edu.yu.velocitytrading.marketmaker;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

/**
 * Per-node market-maker introspection. Separate from
 * {@code cluster.SymbolAdminController} (which is about cluster membership/
 * leadership) so "what am I, the market-maker, currently doing" lives in the
 * market-maker package.
 *
 * {@code GET /marketmaker/status} returns this node's forwarded-snapshot
 * counts per symbol. Used by the cluster integration test to verify that the
 * leader is routing each symbol's updates to the one assigned worker.
 */
@RestController
@RequestMapping("/marketmaker")
@Profile("market-maker-node")
public class MarketMakerStatusController {

    private final MarketMaker marketMaker;

    public MarketMakerStatusController(MarketMaker marketMaker) {
        this.marketMaker = marketMaker;
    }

    /**
     * @param assigned         symbols the coordinator has assigned to this node
     * @param forwardsBySymbol symbols this node has received forwards for,
     *                         mapped to the count of frames delivered
     */
    public record Status(Set<String> assigned, Map<String, Long> forwardsBySymbol) {}

    @GetMapping("/status")
    public Status status() {
        return new Status(marketMaker.assignedSymbols(), marketMaker.forwardedCounts());
    }
}
