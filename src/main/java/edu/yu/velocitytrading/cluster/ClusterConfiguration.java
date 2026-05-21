package edu.yu.velocitytrading.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;

/**
 * Owns the ZK-side beans every cluster component needs: a started
 * {@link CuratorFramework} and a {@link ZkPaths} helper.
 *
 * Only active on the {@code market-maker-node} profile, so other parts of
 * the app don't pull in ZK at boot.
 */
@Configuration
@Profile("market-maker-node")
@EnableConfigurationProperties(ClusterProperties.class)
public class ClusterConfiguration {

    /** @return the shared path helper rooted at the configured base path. */
    @Bean
    public ZkPaths zkPaths(ClusterProperties props) {
        return new ZkPaths(props.getZkBasePath());
    }

    /**
     * Build and start the Curator client. Uses exponential backoff so a
     * transient ZK hiccup doesn't take the app down. Closed on shutdown via
     * {@code destroyMethod = "close"}.
     */
    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework(ClusterProperties props) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(props.getZookeeperConnect())
                .sessionTimeoutMs(props.getSessionTimeoutMs())
                .connectionTimeoutMs(props.getConnectionTimeoutMs())
                .retryPolicy(new ExponentialBackoffRetry(1_000, 5))
                .namespace(null)
                .build();
        client.start();
        return client;
    }
}
