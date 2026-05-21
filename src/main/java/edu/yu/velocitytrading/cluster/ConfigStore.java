package edu.yu.velocitytrading.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Read/write facade over the {@code /marketmaker/symbols} znode, which
 * holds the cluster's ticker list as a JSON array.
 *
 * Responsibilities:
 * <ul>
 *   <li>Serialize/deserialize the symbol list to/from ZK.</li>
 *   <li>Atomic add/remove (JVM-level synchronized; only the leader writes).</li>
 *   <li>Seed the znode from {@code symbols.txt} on first leadership so the
 *       cluster boots with a sensible default.</li>
 * </ul>
 *
 * Symbols are uppercased on the way in.
 */
@Component
@Profile("market-maker-node")
public class ConfigStore {

    private static final Logger log = LoggerFactory.getLogger(ConfigStore.class);
    private static final TypeReference<List<String>> LIST_OF_STRING = new TypeReference<>() {};

    private final CuratorFramework curator;
    private final ZkPaths paths;
    private final ClusterProperties props;
    private final ObjectMapper mapper = new ObjectMapper();

    public ConfigStore(CuratorFramework curator, ZkPaths paths, ClusterProperties props) {
        this.curator = curator;
        this.paths = paths;
        this.props = props;
    }

    /**
     * @return the symbol list, or empty if the znode is missing or empty
     * @throws ClusterException if the znode cannot be read or parsed
     */
    public List<String> readSymbols() {
        try {
            byte[] bytes = curator.getData().forPath(paths.symbols());
            if (bytes == null || bytes.length == 0) {
                return List.of();
            }
            return mapper.readValue(bytes, LIST_OF_STRING);
        } catch (KeeperException.NoNodeException e) {
            return List.of();
        } catch (Exception e) {
            throw new ClusterException("failed to read symbols znode", e);
        }
    }

    /**
     * Replace the symbol list with {@code symbols}, deduplicating while
     * preserving insertion order. Creates the znode if missing.
     *
     * @throws ClusterException on serialization or ZK failure
     */
    public void writeSymbols(List<String> symbols) {
        List<String> normalized = new ArrayList<>(new LinkedHashSet<>(symbols));
        byte[] bytes;
        try {
            bytes = mapper.writeValueAsBytes(normalized);
        } catch (IOException e) {
            throw new ClusterException("failed to serialize symbols", e);
        }
        try {
            Stat stat = curator.checkExists().forPath(paths.symbols());
            if (stat == null) {
                curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(paths.symbols(), bytes);
            } else {
                curator.setData().forPath(paths.symbols(), bytes);
            }
        } catch (Exception e) {
            throw new ClusterException("failed to write symbols znode", e);
        }
    }

    /**
     * Append a symbol. No-op if already present.
     *
     * @param symbol ticker to add (case-insensitive; trimmed)
     * @return {@code true} if newly added; {@code false} if it was already there
     * @throws IllegalArgumentException if the symbol is null or blank
     */
    public synchronized boolean addSymbol(String symbol) {
        String normalized = normalize(symbol);
        List<String> current = readSymbols();
        if (current.contains(normalized)) {
            return false;
        }
        List<String> updated = new ArrayList<>(current);
        updated.add(normalized);
        writeSymbols(updated);
        return true;
    }

    /**
     * Remove a symbol. No-op if not present.
     *
     * @param symbol ticker to remove (case-insensitive; trimmed)
     * @return {@code true} if removed; {@code false} otherwise
     * @throws IllegalArgumentException if the symbol is null or blank
     */
    public synchronized boolean removeSymbol(String symbol) {
        String normalized = normalize(symbol);
        List<String> current = readSymbols();
        if (!current.contains(normalized)) {
            return false;
        }
        List<String> updated = new ArrayList<>(current);
        updated.remove(normalized);
        writeSymbols(updated);
        return true;
    }

    /**
     * Seed the symbol znode from {@code cluster.symbols-seed-file} if empty.
     * Idempotent — safe to call on every leadership acquisition.
     */
    public void seedIfEmpty() {
        List<String> existing = readSymbols();
        if (!existing.isEmpty()) {
            log.info("symbols znode already populated with {} entries; skipping seed", existing.size());
            return;
        }
        List<String> seed = readSeedFile();
        if (seed.isEmpty()) {
            log.info("seed file {} is empty or missing; initializing empty symbol list", props.getSymbolsSeedFile());
            writeSymbols(Collections.emptyList());
            return;
        }
        log.info("seeding symbols znode with {} entries from {}", seed.size(), props.getSymbolsSeedFile());
        writeSymbols(seed);
    }

    /**
     * Parse the seed file, skipping blank lines and {@code '#'} comments.
     *
     * @return distinct, normalized symbols (empty if the file is absent)
     * @throws ClusterException if the file exists but cannot be read
     */
    private List<String> readSeedFile() {
        Path path = Path.of(props.getSymbolsSeedFile());
        if (!Files.exists(path)) {
            return List.of();
        }
        try {
            return Files.readAllLines(path, StandardCharsets.UTF_8).stream()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .filter(line -> !line.startsWith("#"))
                    .map(ConfigStore::normalize)
                    .distinct()
                    .toList();
        } catch (IOException e) {
            throw new ClusterException("failed to read seed file " + path, e);
        }
    }

    /**
     * Trim and uppercase a symbol, rejecting null/blank input.
     *
     * @throws IllegalArgumentException if null or blank after trimming
     */
    private static String normalize(String symbol) {
        if (symbol == null) {
            throw new IllegalArgumentException("symbol must not be null");
        }
        String trimmed = symbol.trim().toUpperCase();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        return trimmed;
    }
}
