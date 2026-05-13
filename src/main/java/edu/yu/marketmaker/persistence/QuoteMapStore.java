package edu.yu.marketmaker.persistence;

import com.hazelcast.map.MapStore;
import edu.yu.marketmaker.model.Quote;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hazelcast MapStore implementation for Quote records.
 * This class bridges the Hazelcast IMap with PostgreSQL persistence
 * by converting between Quote records and QuoteEntity objects.
 */
public class QuoteMapStore implements MapStore<String, Quote> {

    private final BaseJpaRepository<QuoteEntity, String> repository;
    private final edu.yu.marketmaker.persistence.interfaces.JpaQuoteRepository quoteRepository;

    public QuoteMapStore(edu.yu.marketmaker.persistence.interfaces.JpaQuoteRepository repository) {
        this.repository = repository;
        this.quoteRepository = repository;
    }

    // --- MapStore Write Methods ---

    @Override
    public void store(String key, Quote quote) {
        // Delete any existing quote for this symbol and insert the new one.
        // This ensures only the latest quote per symbol persists.
        quoteRepository.deleteBySymbol(key);
        QuoteEntity entity = QuoteEntity.fromRecord(quote);
        repository.save(entity);
    }

    @Override
    public void storeAll(Map<String, Quote> map) {
        var entities = map.values().stream()
                .map(QuoteEntity::fromRecord)
                .collect(Collectors.toList());
        repository.saveAll(entities);
    }

    @Override
    public void delete(String key) {
        quoteRepository.deleteBySymbol(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        // Inefficient but functional default; optimize with a custom query if needed
        keys.forEach(quoteRepository::deleteBySymbol);
    }

    // --- MapLoader Read Methods ---

    @Override
    public Quote load(String key) {
        return quoteRepository.findFirstBySymbolOrderByExpiresAtDesc(key)
                .map(QuoteEntity::toRecord)
                .orElse(null);
    }

    @Override
    public Map<String, Quote> loadAll(Collection<String> keys) {
        // The quotes table holds historical rows per symbol; collapse to one
        // entry per symbol by keeping the latest-expiring quote, matching
        // load(key) semantics above.
        return quoteRepository.findAllBySymbolIn(keys).stream()
                .collect(Collectors.toMap(
                        QuoteEntity::getSymbol,
                        QuoteEntity::toRecord,
                        (a, b) -> a.expiresAt() >= b.expiresAt() ? a : b
                ));
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return repository.findAll().stream()
                .map(QuoteEntity::getSymbol)
                .distinct()
                .collect(Collectors.toList());
    }
}
