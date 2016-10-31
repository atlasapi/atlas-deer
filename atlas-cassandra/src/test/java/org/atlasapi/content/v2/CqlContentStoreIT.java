package org.atlasapi.content.v2;

import java.util.List;

import org.atlasapi.content.CassandraContentStoreIT;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Description;
import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.segment.SegmentEvent;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CqlContentStoreIT extends CassandraContentStoreIT {

    @Override
    protected ContentStore provideContentStore() {
        return CqlContentStore.builder()
                .withSession(session)
                .withSender(sender)
                .withIdGenerator(idGenerator)
                .withClock(clock)
                .withHasher(hasher)
                .withGraphStore(graphStore)
                .withMetricRegistry(new MetricRegistry())
                .withMetricPrefix("test.CqlContentStore.")
                .build();
    }

    @Ignore("this used to test that an exception was thrown for mangled protobuf rows")
    @Test
    @Override
    public void testWritingResolvingContainerWhichOnlyChildRefsThrowsCorrectException() {}

    @Ignore("MBST-17328")
    @Test
    @Override
    public void writingContentWithoutContainerRemovesExistingContainer() throws Exception {
    }

    @Test
    public void writesSegmentsWithDescription() throws Exception {
        when(clock.now()).thenReturn(DateTime.now());

        SegmentEvent segmentEvent = new SegmentEvent();
        Description segmentDescr = new Description("title", "synopsis", "image", "thumbnail");
        segmentEvent.setDescription(segmentDescr);

        Item item = create(new Item());
        item.setSegmentEvents(ImmutableList.of(segmentEvent));

        WriteResult<Item, Content> result = store.writeContent(item);
        Item written = result.getResource();

        Item resolved = (Item) store.resolveIds(ImmutableList.of(written.getId()))
                .get()
                .getResources()
                .first()
                .get();
        List<SegmentEvent> resolvedSegEvents = resolved.getSegmentEvents();
        SegmentEvent resolvedSegEvent = resolvedSegEvents.get(0);
        Description resolvedDescr = resolvedSegEvent.getDescription();

        assertThat(resolvedDescr.getTitle(), is(segmentDescr.getTitle()));
        assertThat(resolvedDescr.getImage(), is(segmentDescr.getImage()));
        assertThat(resolvedDescr.getSynopsis(), is(segmentDescr.getSynopsis()));
        assertThat(resolvedDescr.getThumbnail(), is(segmentDescr.getThumbnail()));
    }
}
