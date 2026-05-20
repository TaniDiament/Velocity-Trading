package edu.yu.marketmaker.persistence.interfaces;

import edu.yu.marketmaker.persistence.BaseJpaRepository;
import edu.yu.marketmaker.persistence.QuoteEntity;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * JPA Repository interface for QuoteEntity persistence.
 * Spring Data JPA requires concrete interfaces to create proxy implementations.
 */
@Repository
public interface JpaQuoteRepository extends BaseJpaRepository<QuoteEntity, String> {
    // Multiple rows per symbol can accumulate (each MM-generated quote inserts
    // a new quote_id row). MapStore.load needs a single result, so pick the
    // most recently expiring one — that's the freshest version of the symbol.
    Optional<QuoteEntity> findFirstBySymbolOrderByExpiresAtDesc(String symbol);

    // @Transactional is required because Hazelcast invokes MapStore.store() from
    // its own executor thread, outside any Spring transaction. Without it, the
    // delete throws TransactionRequiredException.
    @Modifying
    @Transactional
    @Query("delete from QuoteEntity q where q.symbol = :symbol")
    void deleteBySymbol(@Param("symbol") String symbol);

    List<QuoteEntity> findAllBySymbolIn(Collection<String> symbols);
}
