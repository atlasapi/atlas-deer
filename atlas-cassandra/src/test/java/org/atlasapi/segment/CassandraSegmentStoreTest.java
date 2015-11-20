package org.atlasapi.segment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class CassandraSegmentStoreTest {

    private TestCassandraPersistenceModule testPersistenceModule = new TestCassandraPersistenceModule();
    private SegmentStore segmentStore;

    @Before
    public void setUp() throws Exception {
        testPersistenceModule.startAsync().awaitRunning();
        segmentStore = testPersistenceModule.segmentStore();
    }

    @Test
    public void testWriteAndRead() throws Exception {
        Segment segment = new Segment();
        Id id = Id.valueOf(10L);
        segment.setId(id);
        segment.setDuration(Duration.standardSeconds(10L));
        segment.setType(SegmentType.VIDEO);
        segment.setPublisher(Publisher.BBC);
        segmentStore.writeSegment(segment);
        Optional<Segment> writtenSegment = Optional.fromNullable(
                Iterables.getOnlyElement(segmentStore.resolveSegments(ImmutableList.of(id)), null)
        );
        assertThat(writtenSegment.isPresent(), is(true));
        assertThat(writtenSegment.get().equals(segment), is(true));
    }
}