package org.atlasapi.content;

import com.codahale.metrics.MetricRegistry;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AstyanaxCassandraContentStoreIT extends CassandraContentStoreIT {

    @Override
    protected ContentStore provideContentStore() {
        return AstyanaxCassandraContentStore
                .builder(context, CONTENT_TABLE, hasher, comparer, sender, idGenerator, graphStore)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .withMetricRegistry(new MetricRegistry())
                .withMetricPrefix("test.AstyanaxCassandraContentStore.")
                .build();
    }

    @Override
    protected Class<?> provideContentComparisonClass() {
        return Content.class;
    }

    @Ignore("This is a known bug. Given this store is due to be decommissioned it is only being "
            + "fixed in the CqlContentStore")
    @Test
    @Override
    public void writingContentWithoutContainerRemovesExistingContainer() throws Exception {
    }
}
