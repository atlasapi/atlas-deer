package org.atlasapi.content;

import com.codahale.metrics.MetricRegistry;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AstyanaxCassandraContentStoreIT extends CassandraContentStoreIT {

    @Override
    protected ContentStore provideContentStore() {
        return AstyanaxCassandraContentStore
                .builder(context, CONTENT_TABLE, hasher, sender, idGenerator, graphStore)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .withMetricRegistry(new MetricRegistry())
                .withMetricPrefix("test.AstyanaxCassandraContentStore.")
                .build();
    }
}
