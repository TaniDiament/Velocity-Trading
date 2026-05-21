package edu.yu.velocitytrading.cluster;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link EvenSplitStrategy}. Pure function, so no ZK or
 * Spring needed — these tests pin down the behaviors the Coordinator relies
 * on: even sharing, deterministic output, and empty-input handling.
 */
class EvenSplitStrategyTest {

    /** Divisible counts: every worker gets the same number of symbols. */
    @Test
    void splitsEvenlyWhenDivisible() {
        Map<String, List<String>> out = EvenSplitStrategy.split(
                List.of("n1", "n2"),
                List.of("AAPL", "MSFT", "GOOG", "TSLA")
        );
        assertEquals(2, out.get("n1").size());
        assertEquals(2, out.get("n2").size());
    }

    /**
     * Output is a deterministic function of the input sets — iteration order
     * doesn't matter and total symbol count is preserved even on uneven splits.
     */
    @Test
    void distributesRemainderDeterministically() {
        Map<String, List<String>> a = EvenSplitStrategy.split(
                List.of("n3", "n1", "n2"),
                List.of("D", "A", "C", "B")
        );
        Map<String, List<String>> b = EvenSplitStrategy.split(
                List.of("n1", "n2", "n3"),
                List.of("A", "B", "C", "D")
        );
        assertEquals(a, b);
        int total = a.values().stream().mapToInt(List::size).sum();
        assertEquals(4, total);
    }

    /**
     * No workers: return an empty map (don't throw) so the Coordinator can
     * call this during transient single-node startup.
     */
    @Test
    void emptyWorkersReturnsEmptyMap() {
        Map<String, List<String>> out = EvenSplitStrategy.split(List.of(), List.of("AAPL"));
        assertTrue(out.isEmpty());
    }

    /**
     * No symbols: every worker still appears in the map (empty list) so the
     * Coordinator can write empty-assignment znodes that drain workers cleanly.
     */
    @Test
    void emptySymbolsYieldsEmptyListsPerWorker() {
        Map<String, List<String>> out = EvenSplitStrategy.split(List.of("n1", "n2"), List.of());
        assertEquals(Set.of("n1", "n2"), out.keySet());
        assertTrue(out.get("n1").isEmpty());
        assertTrue(out.get("n2").isEmpty());
    }

    /**
     * {@link EvenSplitStrategy#unchangedWorkers} returns only workers whose
     * list is element-wise identical between snapshots — any size delta counts
     * as changed.
     */
    @Test
    void unchangedWorkersIdentifiesStableAssignments() {
        Map<String, List<String>> prev = Map.of(
                "n1", List.of("A", "B"),
                "n2", List.of("C")
        );
        Map<String, List<String>> next = Map.of(
                "n1", List.of("A", "B"),
                "n2", List.of("C", "D")
        );
        assertEquals(Set.of("n1"), EvenSplitStrategy.unchangedWorkers(prev, next));
    }
}
