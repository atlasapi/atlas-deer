package org.atlasapi.content;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import org.atlasapi.entity.AliasIndex;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastaxCassandraContentStoreIT extends CassandraContentStoreIT{

    private final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private final String keyspace = "atlas_testing";
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;

    @Override
    protected ContentStore provideContentStore() {
        DatastaxCassandraService cassandraService = new DatastaxCassandraService(seeds, 8, 2);
        cassandraService.startAsync().awaitRunning();
        Session session = cassandraService.getCluster().connect(keyspace);
        return new DatastaxCassandraContentStore(
                hasher,
                idGenerator,
                sender,
                clock,
                session, writeConsistency, readConsistency, AliasIndex.create(context.getClient(), "content" + "_aliases"));
    }
}
