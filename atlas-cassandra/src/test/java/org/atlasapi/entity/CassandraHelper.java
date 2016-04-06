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
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraHelper {

    private static final String CLUSTER_NAME = "Build";
    private static final String KEYSPACE_NAME = "atlas_testing";
    private static final String SEEDS = "127.0.0.1";
    private static final int CLIENT_THREADS = 5;
    private static final int CONNECTION_TIMEOUT = 1000;
    private static final int PORT = 9160;

    public static AstyanaxContext<Keyspace> testCassandraContext() {
        return new AstyanaxContext.Builder()
                .forCluster(CLUSTER_NAME)
                .forKeyspace(KEYSPACE_NAME)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                        .setAsyncExecutor(Executors.newFixedThreadPool(
                                CLIENT_THREADS,
                                new ThreadFactoryBuilder().setDaemon(true)
                                        .setNameFormat("astyanax-%d")
                                        .build()
                        ))
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("Atlas")
                        .setPort(PORT)
                        .setMaxBlockedThreadsPerHost(CLIENT_THREADS)
                        .setMaxConnsPerHost(CLIENT_THREADS)
                        .setMaxConns(CLIENT_THREADS * 5)
                        .setConnectTimeout(CONNECTION_TIMEOUT)
                        .setSeeds(SEEDS)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
    }

    public static void createKeyspace(AstyanaxContext<Keyspace> context)
            throws ConnectionException {
        context.getClient().createKeyspaceIfNotExists(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class", "SimpleStrategy")
                .build()
        );
    }

    public static void createColumnFamily(AstyanaxContext<Keyspace> context,
            String name, Serializer<?> keySerializer, Serializer<?> colSerializer)
            throws ConnectionException {
        ColumnFamilyDefinition cfDef = context.getClient()
                .describeKeyspace()
                .getColumnFamily(name);
        if (cfDef == null) {
            context.getClient().createColumnFamily(
                    ColumnFamily.newColumnFamily(name, keySerializer, colSerializer),
                    ImmutableMap.of()
            );
        }
    }

    public static void createColumnFamily(AstyanaxContext<Keyspace> context,
            String name, Serializer<?> keySerializer, Serializer<?> colSerializer,
            Serializer<?> valSerializer) throws ConnectionException {
        ColumnFamilyDefinition cfDef = context.getClient()
                .describeKeyspace()
                .getColumnFamily(name);
        if (cfDef == null) {
            context.getClient().createColumnFamily(
                    ColumnFamily.newColumnFamily(name, keySerializer, colSerializer, valSerializer),
                    ImmutableMap.of()
            );
        }
    }

    public static void clearColumnFamily(AstyanaxContext<Keyspace> context, String cfName)
            throws ConnectionException {
        context.getClient().truncateColumnFamily(cfName);
    }
}
