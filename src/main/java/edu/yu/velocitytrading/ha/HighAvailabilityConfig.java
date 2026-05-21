package edu.yu.velocitytrading.ha;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Spring configuration for the high-availability subsystem.
 *
 * <p>Activated for every service profile. Your teammate's market-maker code
 * can share the same {@link CuratorFramework} bean for their per-symbol
 * leader election — it's thread-safe.
 *
 * <p>Properties consumed:
 * <ul>
 *   <li>{@code ha.zookeeper.connect} — ZK connection string (e.g. {@code zookeeper:2181})</li>
 *   <li>{@code ha.service.name} — logical role of this service (e.g. {@code trading-state})</li>
 *   <li>{@code ha.instance.id} — optional unique ID; defaults to hostname + UUID suffix</li>
 *   <li>{@code ha.advertise.host} — hostname other services should use to reach this replica</li>
 *   <li>{@code server.port} — HTTP port (Spring Boot standard)</li>
 *   <li>{@code spring.rsocket.server.port} — RSocket port, optional (-1 if absent)</li>
 * </ul>
 */
@Configuration
@Profile("!market-maker-node")
public class HighAvailabilityConfig {

    @Bean(initMethod = "start", destroyMethod = "close")
    public CuratorFramework curatorFramework(
            @Value("${ha.zookeeper.connect:zookeeper:2181}") String connectString) {

        RetryPolicy retry = new ExponentialBackoffRetry(1000, 5);
        return CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(10_000)
                .connectionTimeoutMs(5_000)
                .retryPolicy(retry)
                .build();
    }

    /**
     * Leader election for THIS replica's role.
     *
     * <p>Automatically hooks into the service registry: on leader acquired, publish
     * this replica's endpoint; on leader lost, remove it.
     */
    @Bean
    public LeaderElectionService leaderElectionService(
            CuratorFramework curator,
            ServiceRegistry registry,
            @Value("${ha.service.name}") String serviceName,
            @Value("${ha.instance.id:#{null}}") String configuredInstanceId,
            @Value("${ha.advertise.host:#{null}}") String advertiseHost,
            @Value("${server.port:8080}") int httpPort,
            @Value("${spring.rsocket.server.port:-1}") int rsocketPort) throws Exception {

        String instanceId = configuredInstanceId != null
                ? configuredInstanceId
                : defaultInstanceId();
        String host = advertiseHost != null ? advertiseHost : resolveHost();

        LeaderElectionService election = new LeaderElectionService(curator, serviceName, instanceId);
        election.addListener(new LeaderElectionService.LeadershipListener() {
            @Override
            public void onLeaderAcquired() {
                registry.publishEndpoint(serviceName, host, httpPort, rsocketPort);
            }

            @Override
            public void onLeaderLost() {
                registry.unpublishEndpoint(serviceName);
            }
        });
        return election;
        // afterPropertiesSet() starts the latch
    }

    /**
     * Register the HTTP leader-guard filter. Only mutating methods are blocked;
     * GETs pass through so read traffic can hit any replica.
     */
    @Bean
    public FilterRegistrationBean<LeaderGuardFilter> leaderGuardFilter(LeaderElectionService election) {
        FilterRegistrationBean<LeaderGuardFilter> reg = new FilterRegistrationBean<>();
        reg.setFilter(new LeaderGuardFilter(election));
        reg.addUrlPatterns("/*");
        reg.setOrder(1);
        return reg;
    }

    private static String defaultInstanceId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID().toString().substring(0, 8);
        } catch (UnknownHostException e) {
            return "unknown-" + UUID.randomUUID();
        }
    }

    private static String resolveHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }
}
