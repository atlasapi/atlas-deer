package org.atlasapi.content;

import org.atlasapi.entity.AliasIndex;

import com.datastax.driver.core.ConsistencyLevel;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastaxCassandraContentStoreIT extends CassandraContentStoreIT {

    private final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;

    @Override
    protected ContentStore provideContentStore() {
        return new DatastaxCassandraContentStore(
                hasher,
                idGenerator,
                sender,
                graphStore,
                clock,
                session,
                writeConsistency,
                readConsistency,
                AliasIndex.create(context.getClient(), "content" + "_aliases")
        );
    }
}
