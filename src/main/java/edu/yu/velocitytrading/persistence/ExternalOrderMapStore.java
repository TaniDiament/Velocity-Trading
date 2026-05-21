package edu.yu.velocitytrading.persistence;

import com.hazelcast.map.MapStore;
import edu.yu.velocitytrading.model.ExternalOrder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Hazelcast MapStore implementation for ExternalOrder records.
 * This class bridges the Hazelcast IMap with PostgreSQL persistence
 * by converting between ExternalOrder records and ExternalOrderEntity objects.
 */
public class ExternalOrderMapStore implements MapStore<UUID, ExternalOrder> {

    private final BaseJpaRepository<ExternalOrderEntity, UUID> repository;

    /**
     * Constructor for ExternalOrderMapStore.
     * @param repository repo to store entities
     */
    public ExternalOrderMapStore(BaseJpaRepository<ExternalOrderEntity, UUID> repository) {
        this.repository = repository;
    }

    // --- MapStore Write Methods ---

    @Override
    public void store(UUID key, ExternalOrder order) {
        ExternalOrderEntity entity = ExternalOrderEntity.fromRecord(order);
        repository.save(entity);
    }

    @Override
    public void storeAll(Map<UUID, ExternalOrder> map) {
        var entities = map.values().stream()
                .map(ExternalOrderEntity::fromRecord)
                .collect(Collectors.toList());
        repository.saveAll(entities);
    }

    @Override
    public void delete(UUID key) {
        repository.deleteById(key);
    }

    @Override
    public void deleteAll(Collection<UUID> keys) {
        repository.deleteAllById(keys);
    }

    // --- MapLoader Read Methods ---

    @Override
    public ExternalOrder load(UUID key) {
        return repository.findById(key)
                .map(ExternalOrderEntity::toRecord)
                .orElse(null);
    }

    @Override
    public Map<UUID, ExternalOrder> loadAll(Collection<UUID> keys) {
        return repository.findAllById(keys).stream()
                .collect(Collectors.toMap(
                        ExternalOrderEntity::getId,
                        ExternalOrderEntity::toRecord
                ));
    }

    @Override
    public Iterable<UUID> loadAllKeys() {
        return repository.findAll().stream()
                .map(ExternalOrderEntity::getId)
                .collect(Collectors.toList());
    }
}

