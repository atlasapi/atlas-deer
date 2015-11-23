package org.atlasapi.entity;

import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


public class CassandraHelper {

    //TODO: externalize
    private static String seeds = "127.0.0.1";
    private static int clientThreads = 25;
    private static int connectionTimeout = 60000;
    private static int port = 9160;

    public static final AstyanaxContext<Keyspace> testCassandraContext() {
        return new AstyanaxContext.Builder()
            .forCluster("Atlas")
            .forKeyspace("atlas_testing")
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                .setAsyncExecutor(Executors.newFixedThreadPool(clientThreads,
                    new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("astyanax-%d")
                    .build()
                ))
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("Atlas")
                .setPort(port)
                .setMaxBlockedThreadsPerHost(clientThreads)
                .setMaxConnsPerHost(clientThreads)
                .setMaxConns(clientThreads * 5)
                .setConnectTimeout(connectionTimeout)
                .setSeeds(seeds)
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());
    }
    
    public static void createKeyspace(AstyanaxContext<Keyspace> context) throws ConnectionException {
        context.getClient().createKeyspace(ImmutableMap.<String, Object> builder()
            .put("strategy_options", ImmutableMap.<String, Object> builder()
                .put("replication_factor", "1")
                .build())
            .put("strategy_class", "SimpleStrategy")
            .build()
        );
    }

    public static void createColumnFamily(AstyanaxContext<Keyspace> context, 
              String name, Serializer<?> keySerializer,Serializer<?> colSerializer) throws ConnectionException {
        context.getClient().createColumnFamily(
            ColumnFamily.newColumnFamily(name, keySerializer, colSerializer),
            ImmutableMap.<String,Object>of()
        );
    }

    public static void createColumnFamily(AstyanaxContext<Keyspace> context, 
            String name, Serializer<?> keySerializer,Serializer<?> colSerializer, Serializer<?> valSerializer) throws ConnectionException {
        context.getClient().createColumnFamily(
                ColumnFamily.newColumnFamily(name, keySerializer, colSerializer, valSerializer),
                ImmutableMap.<String,Object>of()
        );
    }

    public static void clearColumnFamily(AstyanaxContext<Keyspace> context, String cfName) throws ConnectionException {
        context.getClient().truncateColumnFamily(cfName);        
    }
}
