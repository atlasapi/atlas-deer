package org.atlasapi;

import java.util.concurrent.Executors;

import org.atlasapi.PersistenceModule;
import org.atlasapi.content.CassandraContentStore;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.equiv.CassandraEquivalenceRecordStore;
import org.atlasapi.schedule.CassandraScheduleStore;
import org.atlasapi.topic.CassandraTopicStore;
import org.atlasapi.topic.Topic;

import com.google.common.base.Equivalence;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


public class CassandraPersistenceModule extends AbstractIdleService implements PersistenceModule {

    private final AstyanaxContext<Keyspace> context;
    private final CassandraContentStore contentStore;
    private final CassandraTopicStore topicStore;
    private final CassandraScheduleStore scheduleStore;
    private final CassandraEquivalenceRecordStore equivalenceRecordStore;
    
    public CassandraPersistenceModule(Iterable<String> seeds, int port, 
      String cluster, String keyspace, int threadCount, int connectionTimeout, 
      IdGeneratorBuilder idGeneratorBuilder, ContentHasher hasher) {
        context = new AstyanaxContext.Builder()
            .forCluster(cluster)
            .forKeyspace(keyspace)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                .setAsyncExecutor(Executors.newFixedThreadPool(
                    threadCount,
                    new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("astyanax-%d")
                        .build()
                ))
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("altas")
                .setSeeds(Joiner.on(",").join(seeds))
                .setPort(port)
                .setConnectTimeout(connectionTimeout)
                .setMaxBlockedThreadsPerHost(threadCount)
                .setMaxConnsPerHost(threadCount)
                .setMaxConns(threadCount * 5)
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());

        this.contentStore = CassandraContentStore.builder(context, "content", 
            hasher, idGeneratorBuilder.generator("content"))
            .withReadConsistency(ConsistencyLevel.CL_ONE)
            .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
            .build();
        this.topicStore = CassandraTopicStore.builder(context, "topics", 
            topicEquivalence(), idGeneratorBuilder.generator("topic"))
            .withReadConsistency(ConsistencyLevel.CL_ONE)
            .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
            .build();
        this.scheduleStore = CassandraScheduleStore.builder(context, "schedule", contentStore)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();
        this.equivalenceRecordStore = new CassandraEquivalenceRecordStore(
            context, "equivalence_record", ConsistencyLevel.CL_ONE, ConsistencyLevel.CL_QUORUM
        );
    }

    @Override
    protected void startUp() throws Exception {
        context.start();
    }

    @Override
    protected void shutDown() throws Exception {
        context.shutdown();
    }
    
    public AstyanaxContext<Keyspace> getContext() {
        return this.context;
    }
    
    @Override
    public CassandraContentStore contentStore() {
        return contentStore; 
    }

    @Override
    public CassandraTopicStore topicStore() {
        return topicStore;
    }
    
    @Override
    public CassandraScheduleStore scheduleStore() {
        return this.scheduleStore;
    }
    
    private Equivalence<? super Topic> topicEquivalence() {
        return new Equivalence<Topic>(){

            @Override
            protected boolean doEquivalent(Topic a, Topic b) {
                return false;
            }

            @Override
            protected int doHash(Topic t) {
                return 0;
            }
        };
    }
    
    public CassandraEquivalenceRecordStore getEquivalenceRecordStore() {
        return this.equivalenceRecordStore;
    }
    
}
