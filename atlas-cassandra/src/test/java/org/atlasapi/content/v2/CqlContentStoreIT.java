package org.atlasapi.content.v2;

import org.atlasapi.content.CassandraContentStoreIT;
import org.atlasapi.content.ContentStore;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CqlContentStoreIT extends CassandraContentStoreIT {

    @Override
    protected ContentStore provideContentStore() {
        return new CqlContentStore(session, sender, idGenerator, clock, hasher, graphStore);
    }

    @Ignore("this used to test that an exception was thrown for mangled protobuf rows")
    @Test
    @Override
    public void testWritingResolvingContainerWhichOnlyChildRefsThrowsCorrectException() {}
}