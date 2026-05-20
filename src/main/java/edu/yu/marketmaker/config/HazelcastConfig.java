package edu.yu.marketmaker.config;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import edu.yu.marketmaker.memory.HazelcastRepository;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.*;
import edu.yu.marketmaker.exposurereservation.ExposureReservationService;
import edu.yu.marketmaker.persistence.*;
import edu.yu.marketmaker.persistence.interfaces.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import com.hazelcast.config.TcpIpConfig;

import java.util.UUID;

/**
 * Hazelcast configuration for the Market Maker application.
 * Configures an embedded Hazelcast instance with MapStores
 * backed by PostgreSQL for data persistence.
 */
@Configuration
@Profile("!external-publisher & !position-ui")
public class HazelcastConfig {

    @Value("${hazelcast.members:trading-state,exchange,exposure-reservation}")
    private String hazelcastMembers;

    private static final String POSITIONS_MAP_NAME = "positions";
    private static final String FILLS_MAP_NAME = "fills";
    private static final String QUOTES_MAP_NAME = "quotes";
    private static final String EXTERNAL_ORDERS_MAP_NAME = "external-orders";
    private static final String RESERVATIONS_MAP_NAME = "reservations";

    /**
     * Create Hazelcast Instance with all necessary tables.
     * @param positionRepository
     * @param fillRepository
     * @param quoteRepository
     * @param externalOrderRepository
     * @param reservationRepository
     * @return
     */
    @Bean
    public HazelcastInstance hazelcastInstance(
            JpaPositionRepository positionRepository,
            JpaFillRepository fillRepository,
            JpaQuoteRepository quoteRepository,
            JpaExternalOrderRepository externalOrderRepository,
            JpaReservationRepository reservationRepository) {

        Config config = new Config();
        config.setInstanceName("market-maker-hazelcast");

        // Configure all maps with MapStores
        config.addMapConfig(createPositionsMapConfig(positionRepository));
        config.addMapConfig(createFillsMapConfig(fillRepository));
        config.addMapConfig(createQuotesMapConfig(quoteRepository));
        config.addMapConfig(createExternalOrdersMapConfig(externalOrderRepository));
        config.addMapConfig(createReservationsMapConfig(reservationRepository));

        // Network configuration for embedded mode
        configureNetwork(config);

        return Hazelcast.newHazelcastInstance(config);
    }

    /**
     * Method creates the Position Map
     * @param repository
     * @return
     */
    private MapConfig createPositionsMapConfig(JpaPositionRepository repository) {
        MapConfig mapConfig = new MapConfig(POSITIONS_MAP_NAME);
        configureMapStore(mapConfig, new PositionMapStore(repository));
        return mapConfig;
    }

    /**
     * Method creates the Fill Map
     * @param repository
     * @return
     */
    private MapConfig createFillsMapConfig(JpaFillRepository repository) {
        MapConfig mapConfig = new MapConfig(FILLS_MAP_NAME);
        configureMapStore(mapConfig, new FillMapStore(repository));
        return mapConfig;
    }

    /**
     * Method creates the Quote Map
     * @param repository
     * @return
     */
    private MapConfig createQuotesMapConfig(JpaQuoteRepository repository) {
        MapConfig mapConfig = new MapConfig(QUOTES_MAP_NAME);
        configureMapStore(mapConfig, new QuoteMapStore(repository));
        return mapConfig;
    }

    /**
     * Method creates the External Order Map
     * @param repository
     * @return
     */
    private MapConfig createExternalOrdersMapConfig(JpaExternalOrderRepository repository) {
        MapConfig mapConfig = new MapConfig(EXTERNAL_ORDERS_MAP_NAME);
        configureMapStore(mapConfig, new ExternalOrderMapStore(repository));
        return mapConfig;
    }

    /**
     * Method creates the Reservation Map
     * @param repository
     * @return
     */
    private MapConfig createReservationsMapConfig(JpaReservationRepository repository) {
        MapConfig mapConfig = new MapConfig(RESERVATIONS_MAP_NAME);
        configureMapStore(mapConfig, new ReservationMapStore(repository));
        return mapConfig;
    }

    /**
     * Method to set parameters for DB interaction.
     * @param mapConfig config object to be used for parameters
     * @param mapStoreImpl implementation of MapStore
     */
    private void configureMapStore(MapConfig mapConfig, Object mapStoreImpl) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(mapStoreImpl);
        mapStoreConfig.setEnabled(true);

        // Write-behind configuration for better performance
        // Writes are batched and flushed to DB asynchronously
        mapStoreConfig.setWriteDelaySeconds(0); // 0 for write-through, >0 for write-behind
        mapStoreConfig.setWriteBatchSize(100);
        mapStoreConfig.setWriteCoalescing(true);

        // Load all keys on startup
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);

        mapConfig.setMapStoreConfig(mapStoreConfig);

        // Eviction policy - keep all data in memory (no eviction)
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.NONE);
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.PER_NODE);
        mapConfig.setEvictionConfig(evictionConfig);

        // Backup configuration. Two synchronous backups so each partition has
        // three copies. The full-system-restart test (error case 11) rolling-
        // restarts four stateful tiers (trading-state, exposure-reservation,
        // exchange, mm) that all share one 16-member Hazelcast cluster; even
        // when restarts are serialized per StatefulSet, partition migration
        // overlaps with the readiness probe and a partition whose primary +
        // single backup land on adjacent-bouncing members can lose both
        // copies before persistence catches up. Three copies survive two
        // simultaneous member failures, which is the worst case here.
        mapConfig.setBackupCount(2);
        mapConfig.setAsyncBackupCount(0);
    }

    /**
     * Network configuration for Hazelcast Nodes
     * @param config config object to be used for parameters of Hazelcast
     */
private void configureNetwork(Config config) {
    NetworkConfig networkConfig = config.getNetworkConfig();
    JoinConfig joinConfig = networkConfig.getJoin();
    joinConfig.getMulticastConfig().setEnabled(false);

    // TCP-IP discovery using docker DNS.
    // Each service-name resolves to ALL replica IPs inside the compose network.
    // Hazelcast will try each until it finds another cluster member.
    TcpIpConfig tcpConfig = joinConfig.getTcpIpConfig();
    tcpConfig.setEnabled(true);
    for (String member : hazelcastMembers.split(",")) {
        String trimmed = member.trim();
        if (!trimmed.isEmpty()) {
            tcpConfig.addMember(trimmed);
        }
    }

    // Bind to all interfaces so other members can connect to this JVM
    networkConfig.getInterfaces().setEnabled(false);
}

    // --- IMap Beans for Dependency Injection ---


    /**
     * Provides the positions IMap for dependency injection.
     */
    @Bean
    public IMap<String, Position> positionsMap(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap(POSITIONS_MAP_NAME);
    }

    /**
     * Provides the fills IMap for dependency injection.
     */
    @Bean
    public IMap<UUID, Fill> fillsMap(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap(FILLS_MAP_NAME);
    }

    /**
     * Provides the quotes IMap for dependency injection.
     */
    @Bean
    public IMap<String, Quote> quotesMap(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap(QUOTES_MAP_NAME);
    }

    /**
     * Provides the external orders IMap for dependency injection.
     */
    @Bean
    public IMap<UUID, ExternalOrder> externalOrdersMap(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap(EXTERNAL_ORDERS_MAP_NAME);
    }

    /**
     * Provides the reservations IMap for dependency injection.
     */
    @Bean
    public IMap<String, Reservation> reservationsMap(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap(RESERVATIONS_MAP_NAME);
    }

    // --- Repository Beans using Generic HazelcastRepository ---

    /**
     * Provides the Position repository for dependency injection.
     */
    @Bean
    public Repository<String, Position> positionRepository(IMap<String, Position> positionsMap) {
        return new HazelcastRepository<>(positionsMap);
    }

    /**
     * Provides the Fill repository for dependency injection.
     */
    @Bean
    public Repository<UUID, Fill> fillRepository(IMap<UUID, Fill> fillsMap) {
        return new HazelcastRepository<>(fillsMap);
    }

    /**
     * Provides the Quote repository for dependency injection.
     */
    @Bean
    public Repository<String, Quote> quoteRepository(IMap<String, Quote> quotesMap) {
        return new HazelcastRepository<>(quotesMap);
    }

    /**
     * Provides the ExternalOrder repository for dependency injection.
     */
    @Bean
    public Repository<UUID, ExternalOrder> externalOrderRepository(IMap<UUID, ExternalOrder> externalOrdersMap) {
        return new HazelcastRepository<>(externalOrdersMap);
    }

    /**
     * Provides the Reservation repository for dependency injection.
     */
    @Bean
    public Repository<String, Reservation> reservationRepository(IMap<String, Reservation> reservationsMap) {
        return new HazelcastRepository<>(reservationsMap);
    }

    /**
     * Provides the ExposureReservationService for dependency injection.
     */
    @Bean
    public ExposureReservationService exposureReservationService(Repository<String, Reservation> reservationRepository) {
        return new ExposureReservationService(reservationRepository);
    }
}
