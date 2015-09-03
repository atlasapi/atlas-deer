package org.atlasapi.content;

import com.netflix.astyanax.model.ConsistencyLevel;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AstyanaxCassandraContentStoreIT extends CassandraContentStoreIT {

    @Override
    protected ContentStore provideContentStore() {
        return AstyanaxCassandraContentStore
                .builder(context, CONTENT_TABLE, hasher, sender, idGenerator)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .build();
    }
}
