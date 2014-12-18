package org.atlasapi;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ProtocolOptions.Compression;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.AbstractIdleService;


public final class DatastaxCassandraService extends AbstractIdleService {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Cluster.Builder clusterBuilder;

    private Cluster cluster;
    
    public DatastaxCassandraService(Cluster.Builder clusterBuilder) {
        this.clusterBuilder = checkNotNull(clusterBuilder);
    }

    public DatastaxCassandraService(Iterable<String> nodes) {
        this.clusterBuilder = Cluster.builder()
                .addContactPoints(FluentIterable.from(nodes).toArray(String.class))
                .withCompression(Compression.SNAPPY)
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(20000).setConnectTimeoutMillis(20000))
                .withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100, 20000));

    }

    @Override
    protected void startUp() throws Exception {
        connect();
    }

    private void connect() {
        log.info("connecting to   nodes: {}", Joiner.on(", ").join(clusterBuilder.getContactPoints()));
        this.cluster = clusterBuilder.build();
        Metadata metadata = cluster.getMetadata();
        log.info("connected  to cluster: {}", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            log.debug("datacenter: {}; host: {}; rack: {}",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
    }

    public Session getSession(String keyspace) {
        checkRunning();
        return cluster.connect(keyspace);
    }

    private void checkRunning() {
        checkState(isRunning(), this + " is not running");
    }

    public Cluster getCluster() {
        checkRunning();
        return cluster;
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("disconnecting");
        cluster.close();
    }
}
