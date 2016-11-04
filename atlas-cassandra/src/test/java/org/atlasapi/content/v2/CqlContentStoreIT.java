package org.atlasapi.content.v2;

import java.util.List;

import org.atlasapi.content.Brand;
import org.atlasapi.content.CassandraContentStoreIT;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Description;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.segment.SegmentEvent;

import com.metabroadcast.common.time.DateTimeZones;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
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

    @Test
    @Override
    public void writingContentWithoutContainerRemovesExistingContainer() throws Exception {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setTitle("Title 1");
        episode.setImage("image1");
        episode.setDescription("description");
        episode.setEpisodeNumber(42);

        long episodeId = 1235L;
        when(idGenerator.generateRaw()).thenReturn(episodeId);
        store.writeContent(episode);

        Episode resolved = (Episode) resolve(episodeId);
        assertThat(resolved.getContainerSummary(), notNullValue());

        Item item = new Item();
        Item.copyTo(episode, item);
        item.setContainerRef(null);

        when(hasher.hash(Mockito.any())).thenReturn("foo", "bar");
        store.writeContent(item);

        Item resolvedItem = (Item) resolve(episodeId);
        assertThat(resolvedItem.getContainerSummary(), nullValue());
    }

    @Test
    public void multipleChildUpdatesPreserveSingleRefInParent() throws Exception {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        long brandId = 1234L;
        when(idGenerator.generateRaw()).thenReturn(brandId);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series = create(new Series());
        series.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        long seriesId = 1235L;
        when(idGenerator.generateRaw()).thenReturn(seriesId);
        WriteResult<Series, Content> seriesWriteResult = store.writeContent(series);

        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setSeries(seriesWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(2));
        long episodeId = 1236L;
        when(idGenerator.generateRaw()).thenReturn(episodeId);
        store.writeContent(episode);

        // brand should have a ref to episode here
        Brand resolvedBrand = (Brand) store.resolveIds(ImmutableList.of(Id.valueOf(brandId)))
                .get()
                .getResources()
                .first()
                .orNull();

        ImmutableList<ItemRef> itemRefs = resolvedBrand.getItemRefs();
        assertThat(itemRefs.size(), is(1));

        ItemRef episodeRef = itemRefs.get(0);
        assertThat(episodeRef.getId().longValue(), is(episodeId));

        episode.setTitle("some dodgy title to trigger an update");

        when(hasher.hash(any(Content.class))).thenReturn("one", "two");
        when(clock.now()).thenReturn(now.plusHours(3));
        store.writeContent(episode);

        resolvedBrand = (Brand) store.resolveIds(ImmutableList.of(Id.valueOf(brandId)))
                .get()
                .getResources()
                .first()
                .orNull();

        itemRefs = resolvedBrand.getItemRefs();
        assertThat(itemRefs.size(), is(1));

        episodeRef = itemRefs.get(0);
        assertThat(episodeRef.getId().longValue(), is(episodeId));
    }

    @Test
    public void multipleChildUpdatesAddMultipleBroadcastRefsToContainerUpcoming() throws Exception {

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
