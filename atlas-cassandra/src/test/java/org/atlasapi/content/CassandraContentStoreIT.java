package org.atlasapi.content;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public abstract class CassandraContentStoreIT {

    protected static final AstyanaxContext<Keyspace> context =
        CassandraHelper.testCassandraContext();
    
    @Mock protected ContentHasher hasher;
    @Mock protected IdGenerator idGenerator;
    @Mock protected MessageSender<ResourceUpdatedMessage> sender;
    @Mock protected Clock clock;

    private ContentStore store;

    protected static final String CONTENT_TABLE = "content";

    protected abstract ContentStore provideContentStore();
    @Before
    public void before() {
        store = provideContentStore();
    }
    
    static Logger root = Logger.getRootLogger();
    
    @BeforeClass
    public static void setup() throws ConnectionException {
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
        context.start();
        tearDown();
        CassandraHelper.createKeyspace(context);
        CassandraHelper.createColumnFamily(context, CONTENT_TABLE, LongSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, "content_aliases", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    }
    
    @AfterClass
    public static void tearDown() throws ConnectionException {
        try {
            context.getClient().dropKeyspace();
        } catch (BadRequestException ire) { }
    }
    
    @After
    public void clearCf() throws ConnectionException {
        context.getClient().truncateColumnFamily(CONTENT_TABLE);
        context.getClient().truncateColumnFamily("content_aliases");
    }
    
    @Test
    public void testWriteAndReadTopLevelItem() throws Exception {
        Content content = create(new Item());
        content.setTitle("title");
        
        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        
        WriteResult<Content, Content> writeResult = store.writeContent(content);
        assertTrue(writeResult.written());
        assertThat(writeResult.getResource().getId().longValue(), is(1234l));
        assertFalse(writeResult.getPrevious().isPresent());
        
        verify(sender).sendMessage(argThat(isA(ResourceUpdatedMessage.class)));
        
        Content item = resolve(content.getId().longValue());
        
        assertThat(item.getId(), is(writeResult.getResource().getId()));
        assertThat(item.getTitle(), is(content.getTitle()));
        assertThat(item.getFirstSeen(), is(now));
        assertThat(item.getLastUpdated(), is(now));
        assertThat(item.getThisOrChildLastUpdated(), is(now));
        
    }

    @Test
    public void testWriteAndReadTopLevelItemWithActivelyPublishedFalse() throws Exception {
        Content content = create(new Item());
        content.setTitle("title");
        content.setActivelyPublished(false);

        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);

        WriteResult<Content, Content> writeResult = store.writeContent(content);
        assertTrue(writeResult.written());
        assertThat(writeResult.getResource().getId().longValue(), is(1234l));
        assertFalse(writeResult.getPrevious().isPresent());
        Content item = resolve(content.getId().longValue());

        assertThat(item.getId(), is(writeResult.getResource().getId()));
        assertThat(item.getTitle(), is(content.getTitle()));
        assertThat(item.getFirstSeen(), is(now));
        assertThat(item.getLastUpdated(), is(now));
        assertThat(item.getThisOrChildLastUpdated(), is(now));
        assertThat(item.isActivelyPublished(), is(false));

    }

    @Test
    public void testWriteAndReadTopLevelItemWithBroadcast() throws Exception {
        Item item = create(new Item());
        item.setTitle("title");

        Broadcast broadcast = new Broadcast(Id.valueOf(1), new DateTime(), new DateTime().plusHours(1));
        broadcast.withId("pa:107472720");
        broadcast.setBlackoutRestriction(new BlackoutRestriction(true));
        item.addBroadcast(broadcast);


        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);

        WriteResult<Item, Content> writeResult = store.writeContent(item);
        assertTrue(writeResult.written());
        assertThat(writeResult.getResource().getId().longValue(), is(1234l));
        assertFalse(writeResult.getPrevious().isPresent());

        verify(sender).sendMessage(argThat(isA(ResourceUpdatedMessage.class)));

        Item read = (Item) resolve(item.getId().longValue());

        assertThat(read.getId(), is(writeResult.getResource().getId()));
        assertThat(read.getTitle(), is(read.getTitle()));
        assertThat(read.getFirstSeen(), is(now));
        assertThat(read.getLastUpdated(), is(now));
        assertThat(read.getThisOrChildLastUpdated(), is(now));
        assertThat(Iterables.getOnlyElement(read.getBroadcasts()).getBlackoutRestriction().isPresent(), is(true));

    }
    
    @Test
    public void testContentNotWrittenWhenHashNotChanged() throws Exception {
        Content content = create(new Item());
        content.setTitle("title");
        
        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        
        WriteResult<Content, Content> writeResult = store.writeContent(content);
        assertTrue(writeResult.written());
        
        when(hasher.hash(argThat(isA(Content.class)))).thenReturn("same");

        writeResult = store.writeContent(writeResult.getResource());
        assertFalse(writeResult.written());
        
        verify(hasher, times(2)).hash(argThat(isA(Content.class)));
        verify(idGenerator, times(1)).generateRaw();
        verify(clock, times(1)).now();
        
        Content item = resolve(content.getId().longValue());
        
        assertThat(item.getId(), is(content.getId()));
        assertThat(item.getTitle(), is(content.getTitle()));
        assertThat(item.getFirstSeen(), is(now));
        assertThat(item.getLastUpdated(), is(now));
        assertThat(item.getThisOrChildLastUpdated(), is(now));
        
    }

    @Test
    public void testContentWrittenWhenHashChanged() throws Exception {
        Content content = create(new Item());
        content.setTitle("title");
        
        DateTime now = new DateTime(DateTimeZones.UTC);
        DateTime next = now.plusHours(1);
        when(clock.now())
            .thenReturn(now)
            .thenReturn(next);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        
        WriteResult<Content, Content> writeResult = store.writeContent(content);
        assertTrue(writeResult.written());
        
        Content resolved = resolve(content.getId().longValue());
        assertThat(resolved.getTitle(), is(content.getTitle()));
        
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("different")
            .thenReturn("differentAgain");

        writeResult = store.writeContent(writeResult.getResource());
        assertTrue(writeResult.written());
        
        verify(hasher, times(2)).hash(argThat(isA(Content.class)));
        verify(idGenerator, times(1)).generateRaw();
        verify(clock, times(2)).now();
        
        Content item = resolve(content.getId().longValue());
        
        assertThat(item.getId(), is(content.getId()));
        assertThat(item.getTitle(), is(content.getTitle()));
        assertThat(item.getFirstSeen(), is(now));
        assertThat(item.getLastUpdated(), is(next));
        assertThat(item.getThisOrChildLastUpdated(), is(next));
        
    }
    
    @Test
    public void testResolvesExistingContentByAlias() throws Exception {

        Item bbcItem = new Item();
        bbcItem.setPublisher(Publisher.BBC);
        bbcItem.addAlias(new Alias("shared", "alias"));
        bbcItem.setTitle("title");

        Item c4Item = new Item();
        c4Item.setPublisher(Publisher.C4);
        c4Item.addAlias(new Alias("shared", "alias"));

        when(clock.now()).thenReturn(new DateTime(DateTimeZones.UTC));
        when(idGenerator.generateRaw())
            .thenReturn(1234L)
            .thenReturn(1235L);
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("different")
            .thenReturn("differentAgain");
        
        store.writeContent(bbcItem);
        store.writeContent(c4Item);
        
        Item resolvedItem = (Item) resolve(1234L);
        assertThat(resolvedItem.getTitle(), is(bbcItem.getTitle()));
        
        bbcItem.setTitle("newTitle");
        bbcItem.setId(null);
        WriteResult<Item, Content> writtenContent = store.writeContent(bbcItem);
        assertThat(writtenContent.getPrevious().get().getTitle(), is("title"));
        
        resolvedItem = (Item) resolve(1234L);
        assertThat(resolvedItem.getTitle(), is(bbcItem.getTitle()));
        
        verify(clock, times(3)).now();
        verify(idGenerator, times(2)).generateRaw();
        verify(hasher, times(2)).hash(argThat(isA(Content.class)));
    }

    @Test(expected=WriteException.class)
    public void testWritingItemWithMissingBrandFails() throws Exception {
        Item item = create(new Item());
        item.setContainerRef(new BrandRef(Id.valueOf(1235), item.getSource()));
        
        store.writeContent(item);
        
        verify(idGenerator, never()).generateRaw();
        
    }

    @Test(expected=WriteException.class)
    public void testWritingSeriesWithMissingBrandFails() throws Exception {
        try {
            Series series = create(new Series());
            series.setBrandRef(new BrandRef(Id.valueOf(1235), series.getSource()));
            
            store.writeContent(series);
        } finally {
            verify(idGenerator, never()).generateRaw();
        }
    }

    @Test
    public void testWritingSeriesWithoutBrandSucceeds() throws Exception {
        Series series = create(new Series());
        series.setAliases(ImmutableSet.of(new Alias("namespace", "value")));
        series.setBrandRef(null);
        
        when(idGenerator.generateRaw())
            .thenReturn(1234L);
        
        store.writeContent(series);
        
        Series resolved = (Series) resolve(1234L);
        
        assertThat(resolved.getAliases(), is(series.getAliases()));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testWritingEpisodeWithoutBrandRefFails() throws Exception {
        try {
                
            Episode episode = create(new Episode());
    
            store.writeContent(episode);
        
        }finally {
            verify(idGenerator, never()).generateRaw();
        }
    }
    
    @Test(expected=WriteException.class)
    public void testWritingEpisodeWithoutBrandWrittenFails() throws Exception {
        try {
                
            Series series = create(new Series());
            series.setBrandRef(new BrandRef(Id.valueOf(666), series.getSource()));
            
            Episode episode = create(new Episode());
    
            episode.setContainerRef(new BrandRef(Id.valueOf(666), episode.getSource()));
            episode.setSeriesRef(new SeriesRef(Id.valueOf(999), episode.getSource()));
            
            store.writeContent(episode);
        
        }finally {
            verify(idGenerator, never()).generateRaw();
        }
    }

    @Test(expected = WriteException.class)
    public void testWritingEpisodeWithSeriesRefWithoutSeriesWrittenFails() throws Exception {
        try {
            Brand brand = create(new Brand());

            Series series = create(new Series());
            series.setBrandRef(new BrandRef(Id.valueOf(666), series.getSource()));

            Episode episode = create(new Episode());

            episode.setContainerRef(new BrandRef(Id.valueOf(666), episode.getSource()));
            episode.setSeriesRef(new SeriesRef(Id.valueOf(999), episode.getSource()));

            when(clock.now()).thenReturn(new DateTime(DateTimeZones.UTC));
            when(idGenerator.generateRaw()).thenReturn(1234L);

            WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);
            assertThat(brandWriteResult.getResource().getId().longValue(), is(1234L));
            store.writeContent(episode);
        } finally {
            //generate for brand but not episode
            verify(idGenerator, times(1)).generateRaw();
        }
    }
    
    @Test
    public void testWritingItemWritesRefIntoParent() throws Exception {
        
        when(clock.now()).thenReturn(new DateTime(DateTimeZones.UTC));
        when(idGenerator.generateRaw())
            .thenReturn(1234L)
            .thenReturn(1235L);

        Brand brand = create(new Brand());
        
        store.writeContent(brand);
        
        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getItemRefs(), is(empty()));
        
        Item item = create(new Item());
        item.setContainer(resolvedBrand);
        
        store.writeContent(item);
        
        Item resolvedItem = (Item) resolve(1235L);
        
        assertThat(resolvedItem.getContainerRef().getId().longValue(), is(1234L));
        
    }
    
    @Test
    public void testWritingFullContentHierarchy() throws Exception {

        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> series1WriteResult = store.writeContent(series1);

        Series series2 = create(new Series());
        series2.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1236L);
        WriteResult<Series, Content> series2WriteResult = store.writeContent(series2);

        Episode episode1 = create(new Episode());
        episode1.setContainer(brandWriteResult.getResource());
        episode1.setSeries(series1WriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeContent(episode1);

        Episode episode2 = create(new Episode());
        episode2.setContainer(brandWriteResult.getResource());
        episode2.setSeries(series2WriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1238L);
        store.writeContent(episode2);

        Episode episode3 = create(new Episode());
        episode3.setContainer(brandWriteResult.getResource());
        episode3.setSeries(series1WriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(3));
        when(idGenerator.generateRaw()).thenReturn(1239L);
        store.writeContent(episode3);

        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getFirstSeen(), is(now));
        assertThat(resolvedBrand.getLastUpdated(), is(now));
        assertThat(resolvedBrand.getThisOrChildLastUpdated(), is(now.plusHours(3)));
        assertThat(resolvedBrand.getSeriesRefs().size(), is(2));
        assertThat(resolvedBrand.getItemRefs().size(), is(3));

        Series resolvedSeries1 = (Series) resolve(1235L);
        assertThat(resolvedSeries1.getFirstSeen(), is(now.plusHours(1)));
        assertThat(resolvedSeries1.getLastUpdated(), is(now.plusHours(1)));
        assertThat(resolvedSeries1.getThisOrChildLastUpdated(), is(now.plusHours(3)));
        assertThat(resolvedSeries1.getBrandRef().getId().longValue(), is(1234L));
        assertThat(resolvedSeries1.getItemRefs().size(), is(2));

        Series resolvedSeries2 = (Series) resolve(1236L);
        assertThat(resolvedSeries2.getFirstSeen(), is(now.plusHours(1)));
        assertThat(resolvedSeries2.getLastUpdated(), is(now.plusHours(1)));
        assertThat(resolvedSeries2.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedSeries2.getBrandRef().getId().longValue(), is(1234L));
        assertThat(resolvedSeries2.getItemRefs().size(), is(1));

        Episode resolvedEpisode1 = (Episode) resolve(1237L);
        assertThat(resolvedEpisode1.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode1.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode1.getContainerSummary().getTitle(), is("Brand"));

        Episode resolvedEpisode2 = (Episode) resolve(1238L);
        assertThat(resolvedEpisode2.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode2.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode2.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode2.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode2.getSeriesRef().getId().longValue(), is(1236L));
        assertThat(resolvedEpisode2.getContainerSummary().getTitle(), is("Brand"));

        Episode resolvedEpisode3 = (Episode) resolve(1239L);
        assertThat(resolvedEpisode3.getFirstSeen(), is(now.plusHours(3)));
        assertThat(resolvedEpisode3.getLastUpdated(), is(now.plusHours(3)));
        assertThat(resolvedEpisode3.getThisOrChildLastUpdated(), is(now.plusHours(3)));
        assertThat(resolvedEpisode3.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode3.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode3.getContainerSummary().getTitle(), is("Brand"));
    }
    
    @Test
    public void testRewritingBrandReturnsChildRefsInWriteResultBrand() throws Exception {
        
        DateTime now = new DateTime(DateTimeZones.UTC);
        
        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);
        
        Series series = create(new Series());
        series.setBrand(brandWriteResult.getResource());
        
        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> seriesWriteResult = store.writeContent(series);

        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setSeries(seriesWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeContent(episode);
        
        Brand writtenBrand = brandWriteResult.getResource();
        writtenBrand.setTitle("new title");
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("different")
            .thenReturn("differentAgain");
        
        brandWriteResult = store.writeContent(writtenBrand);
        writtenBrand = brandWriteResult.getResource();
        
        assertThat(writtenBrand.getItemRefs().size(), is(1));
        assertThat(writtenBrand.getSeriesRefs().size(), is(1));

        Series writtenSeries = seriesWriteResult.getResource();
        writtenSeries.setTitle("new title");
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("different")
            .thenReturn("differentAgain");
        
        seriesWriteResult = store.writeContent(writtenSeries);
        writtenSeries = seriesWriteResult.getResource();
        
        assertThat(writtenSeries.getBrandRef().getId(), is(writtenBrand.getId()));
        assertThat(writtenSeries.getItemRefs().size(), is(1));
        
    }
    
    @Test
    public void testResolvingByAlias() throws Exception {
        
        DateTime now = new DateTime(DateTimeZones.UTC);

        Alias bbcBrandAlias = new Alias("brand", "alias");
        Alias bbcSeriesAlias = new Alias("series", "alias");
        
        Brand brand = create(new Brand());
        brand.addAlias(bbcBrandAlias);

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        store.writeContent(brand);
        
        OptionalMap<Alias, Content> resolved = store.resolveAliases(
                ImmutableSet.of(bbcBrandAlias, bbcSeriesAlias), Publisher.BBC);
        
        assertThat(resolved.size(), is(1));
        assertThat(resolved.get(bbcBrandAlias).get().getId(), is(Id.valueOf(1234L)));
        
        Series series = create(new Series());
        series.addAlias(bbcSeriesAlias);
        
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1235L);
        store.writeContent(series);
        
        resolved = store.resolveAliases(
                ImmutableSet.of(bbcBrandAlias, bbcSeriesAlias), Publisher.BBC);
        
        assertThat(resolved.size(), is(2));
        assertThat(resolved.get(bbcBrandAlias).get().getId(), is(Id.valueOf(1234L)));
        assertThat(resolved.get(bbcSeriesAlias).get().getId(), is(Id.valueOf(1235L)));
        
    }

    @Test
    public void testResolvingByAliasDoesntResolveContentFromAnotherSource() throws Exception {
        
        DateTime now = new DateTime(DateTimeZones.UTC);
        
        Brand bbcBrand = create(new Brand());
        Alias sharedAlias = new Alias("shared", "alias");
        bbcBrand.addAlias(sharedAlias);
        
        Brand c4Brand = create(new Brand());
        c4Brand.setPublisher(Publisher.C4);
        c4Brand.addAlias(sharedAlias);
        
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        store.writeContent(bbcBrand);
        
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1235L);
        store.writeContent(c4Brand);
        
        OptionalMap<Alias, Content> resolved = store.resolveAliases(
            ImmutableSet.of(sharedAlias), Publisher.BBC);
        
        assertThat(resolved.size(), is(1));
        assertThat(resolved.get(sharedAlias).get().getId(), is(Id.valueOf(1234L)));
        
        resolved = store.resolveAliases(
            ImmutableSet.of(sharedAlias), Publisher.C4);
        
        assertThat(resolved.size(), is(1));
        assertThat(resolved.get(sharedAlias).get().getId(), is(Id.valueOf(1235L)));
        
    }
    
    @Test
    public void testResolvingMissingContentReturnsEmptyResolved() throws Exception {
        
        ListenableFuture<Resolved<Content>> resolved = store.resolveIds(ImmutableSet.of(Id.valueOf(4321)));
        
        assertTrue(resolved.get(1, TimeUnit.SECONDS).getResources().isEmpty());
        
    }
    
    @Test
    public void testResolvingMissingContentByAliasReturnsNothing() throws Exception {
        
        Alias alias = new Alias("missing","alias");
        OptionalMap<Alias,Content> resolveAliases = store.resolveAliases(ImmutableList.of(alias), Publisher.BBC);
        
        assertThat(resolveAliases.get(alias), is(Optional.<Content>absent()));
    }
    
    @Test
    public void testSwitchingFromBrandToSeries() throws WriteException {
        
        Brand brand = create(new Brand());
        Alias sharedAlias = new Alias("shared", "alias");
        brand.addAlias(sharedAlias);
        
        when(idGenerator.generateRaw()).thenReturn(1234L);
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("different")
            .thenReturn("differentAgain");
        
        WriteResult<Brand, Content> writtenBrand = store.writeContent(brand);
        assertTrue(writtenBrand.written());
        
        Series series = create(new Series());
        series.setId(writtenBrand.getResource().getId());
        series.addAlias(sharedAlias);
        
        WriteResult<Series, Content> writtenSeries = store.writeContent(series);
        assertTrue(writtenSeries.written());
        assertTrue(writtenSeries.getPrevious().get() instanceof Brand);
        
        verify(idGenerator, times(1)).generateRaw();
    }

    @Test
    public void testSwitchingFromSeriesToBrand() throws WriteException {
        
        Series series = create(new Series());
        Alias sharedAlias = new Alias("shared", "alias");
        series.addAlias(sharedAlias);
        
        when(idGenerator.generateRaw()).thenReturn(1234L);
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("different")
            .thenReturn("differentAgain");
        
        WriteResult<Series, Content> writtenSeries = store.writeContent(series);
        assertTrue(writtenSeries.written());
        
        Brand brand = create(new Brand());
        brand.addAlias(sharedAlias);
        brand.setId(writtenSeries.getResource().getId());
        
        WriteResult<Brand, Content> writtenBrand = store.writeContent(brand);
        assertTrue(writtenBrand.written());
        assertTrue(writtenBrand.getPrevious().get() instanceof Series);
        
        verify(idGenerator, times(1)).generateRaw();
    }


    @Test
    public void testWriteUpcomingContentForBrands() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> series1WriteResult = store.writeContent(series1);


        Episode episode1 = create(new Episode());
        episode1.setContainer(brandWriteResult.getResource());
        episode1.setSeries(series1WriteResult.getResource());
        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");
        Set<Broadcast> broadcasts = ImmutableSet.of(
                broadcast1,
                new Broadcast(
                        Id.valueOf(1),
                        DateTime.now(DateTimeZone.UTC).minusHours(2),
                        DateTime.now(DateTimeZone.UTC).minusHours(1)
                ).withId("sourceId:2")
        );
        episode1.setBroadcasts(
            broadcasts
        );



        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeContent(episode1);

        Map<ItemRef, Iterable<BroadcastRef>> expectedUpcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>> builder()
                .put(episode1.toRef(), ImmutableList.of(broadcast1.toRef()))
                .build();

        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getItemRefs().size(), is(1));
        assertThat(resolvedBrand.getUpcomingContent(), is(expectedUpcomingContent));

        Series resolvedSeries1 = (Series) resolve(1235L);
        assertThat(resolvedSeries1.getBrandRef().getId().longValue(), is(1234L));
        assertThat(resolvedSeries1.getItemRefs().size(), is(1));
        assertThat(resolvedSeries1.getUpcomingContent(), is(expectedUpcomingContent));


        Episode resolvedEpisode1 = (Episode) resolve(1237L);
        assertThat(resolvedEpisode1.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode1.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode1.getContainerSummary().getTitle(), is("Brand"));
        assertThat(resolvedEpisode1.getBroadcasts(), is(broadcasts));

    }


    @Test
    public void testFilterStaleUpcomingContentForBrands() throws WriteException, InterruptedException, ExecutionException, TimeoutException, ConnectionException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> series1WriteResult = store.writeContent(series1);



        Episode episode1 = create(new Episode());
        episode1.setContainer(brandWriteResult.getResource());
        episode1.setSeries(series1WriteResult.getResource());
        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");
        Set<Broadcast> broadcasts = ImmutableSet.of(
                broadcast1
        );
        episode1.setBroadcasts(
                broadcasts
        );



        Episode episode2 = create(new Episode());
        episode2.setContainer(brandWriteResult.getResource());
        episode2.setSeries(series1WriteResult.getResource());
        Broadcast pastBroadcast = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).minusHours(2),
                DateTime.now(DateTimeZone.UTC).minusHours(1)
        ).withId("sourceId:2");
        Set<Broadcast> broadcasts2= ImmutableSet.of(
                pastBroadcast
        );
        episode2.setBroadcasts(
                broadcasts2
        );


        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeContent(episode1);

        when(idGenerator.generateRaw()).thenReturn(1238L);
        store.writeContent(episode2);

        ContentProtos.Content.Builder contentBuilder = ContentProtos.Content.newBuilder();

        contentBuilder.addUpcomingContent(
                new ItemAndBroadcastRefSerializer()
                        .serialize(
                                episode2.toRef(),
                                ImmutableList.of(pastBroadcast.toRef()
                                )
                        )
        );

        ColumnFamily<Long, String> columnFamily = ColumnFamily.newColumnFamily(
                CONTENT_TABLE,
                LongSerializer.get(),
                StringSerializer.get()
        );

        MutationBatch batch = context.getClient().prepareMutationBatch();
        ColumnListMutation<String> mutation = batch.withRow(columnFamily, 1234L);
        mutation.putColumn("UPCOMING_BROADCASTS:1238", contentBuilder.build().toByteArray());

        ColumnListMutation<String> mutation2 = batch.withRow(columnFamily, 1235L);
        mutation2.putColumn("UPCOMING_BROADCASTS:1238", contentBuilder.build().toByteArray());
        batch.execute();



        Map<ItemRef, Iterable<BroadcastRef>> expectedUpcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>> builder()
                .put(episode1.toRef(), ImmutableList.of(broadcast1.toRef()))
                .build();

        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getItemRefs().size(), is(2));
        assertThat(resolvedBrand.getUpcomingContent(), is(expectedUpcomingContent));

        Series resolvedSeries1 = (Series) resolve(1235L);
        assertThat(resolvedSeries1.getBrandRef().getId().longValue(), is(1234L));
        assertThat(resolvedSeries1.getItemRefs().size(), is(2));
        assertThat(resolvedSeries1.getUpcomingContent(), is(expectedUpcomingContent));


        Episode resolvedEpisode1 = (Episode) resolve(1237L);
        assertThat(resolvedEpisode1.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode1.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode1.getContainerSummary().getTitle(), is("Brand"));
        assertThat(resolvedEpisode1.getBroadcasts(), is(broadcasts));


        Episode resolvedEpisode2 = (Episode) resolve(1238L);
        assertThat(resolvedEpisode2.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode2.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode2.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode2.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode2.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode2.getContainerSummary().getTitle(), is("Brand"));
        assertThat(resolvedEpisode2.getBroadcasts(), is(broadcasts2));

    }

    @Test
    public void testWriteAvailableContentForBrands() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> series1WriteResult = store.writeContent(series1);



        Episode episode1 = create(new Episode());
        episode1.setContainer(brandWriteResult.getResource());
        episode1.setSeries(series1WriteResult.getResource());

        Encoding encoding1 = new Encoding();
        Encoding encoding2 = new Encoding();

        Location location1 = new Location();
        location1.setAvailable(true);
        location1.setUri("location1");

        Location location2 = new Location();
        location2.setAvailable(true);
        location2.setUri("location2");

        Location location3 = new Location();
        location3.setAvailable(true);
        location3.setUri("location3");
        Policy policy1 = new Policy();
        policy1.setAvailabilityStart(now.minusHours(1));
        policy1.setAvailabilityEnd(now.plusHours(1));
        location3.setPolicy(policy1);

        Location location4 = new Location();
        location4.setAvailable(true);
        location4.setUri("location4");
        Policy policy2 = new Policy();
        policy2.setAvailabilityStart(now.plusHours(1));
        policy2.setAvailabilityEnd(now.plusHours(2));
        location4.setPolicy(policy2);

        encoding1.setAvailableAt(ImmutableSet.of(location1, location2));
        encoding2.setAvailableAt(ImmutableSet.of(location3, location4));
        episode1.setManifestedAs(ImmutableSet.of(encoding1, encoding2));


        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeContent(episode1);

        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getItemRefs().size(), is(1));
        assertThat(resolvedBrand.getAvailableContent().size(), is(1));
        assertThat(
                resolvedBrand.getAvailableContent().get(episode1.toRef()),
                containsInAnyOrder(location1.toSummary(), location2.toSummary(), location3.toSummary())
        );

        Series resolvedSeries1 = (Series) resolve(1235L);
        assertThat(resolvedSeries1.getBrandRef().getId().longValue(), is(1234L));
        assertThat(resolvedSeries1.getItemRefs().size(), is(1));
        assertThat(resolvedSeries1.getAvailableContent().size(), is(1));
        assertThat(
                resolvedSeries1.getAvailableContent().get(episode1.toRef()),
                containsInAnyOrder(location1.toSummary(), location2.toSummary(), location3.toSummary())
        );


        Episode resolvedEpisode1 = (Episode) resolve(1237L);
        assertThat(resolvedEpisode1.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode1.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode1.getContainerSummary().getTitle(), is("Brand"));

    }


    @Test
    public void testFilterContentNoLongerAvailableForBrands() throws WriteException, InterruptedException, ExecutionException, TimeoutException, ConnectionException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> series1WriteResult = store.writeContent(series1);



        Episode episode1 = create(new Episode());
        episode1.setContainer(brandWriteResult.getResource());
        episode1.setSeries(series1WriteResult.getResource());

        Encoding encoding1 = new Encoding();
        Encoding encoding2 = new Encoding();

        Location location1 = new Location();
        location1.setAvailable(true);
        location1.setUri("location1");

        Location location2 = new Location();
        location2.setAvailable(true);
        location2.setUri("location2");

        Location location3 = new Location();
        location3.setAvailable(true);
        location3.setUri("location3");
        Policy policy1 = new Policy();
        policy1.setAvailabilityStart(now.minusHours(1));
        policy1.setAvailabilityEnd(now.plusHours(1));
        location3.setPolicy(policy1);

        Location location4 = new Location();
        location4.setAvailable(true);
        location4.setUri("location4");
        Policy policy2 = new Policy();
        policy2.setAvailabilityStart(now.plusHours(1));
        policy2.setAvailabilityEnd(now.plusHours(2));
        location4.setPolicy(policy2);

        encoding1.setAvailableAt(ImmutableSet.of(location1, location2, location3));
        encoding2.setAvailableAt(ImmutableSet.of(location4));
        episode1.setManifestedAs(ImmutableSet.of(encoding1));


        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeContent(episode1);

        Episode episode2 = create(new Episode());
        episode2.setContainer(brandWriteResult.getResource());
        episode2.setSeries(series1WriteResult.getResource());
        episode2.setManifestedAs(ImmutableSet.of(encoding2));

        when(idGenerator.generateRaw()).thenReturn(1238L);
        store.writeContent(episode2);

        ContentProtos.Content.Builder contentBuilder = ContentProtos.Content.newBuilder();

        contentBuilder.addAvailableContent(
                new ItemAndLocationSummarySerializer()
                        .serialize(
                                episode2.toRef(),
                                ImmutableList.of(location4.toSummary())
                        )
        );

        ColumnFamily<Long, String> columnFamily = ColumnFamily.newColumnFamily(
                CONTENT_TABLE,
                LongSerializer.get(),
                StringSerializer.get()
        );

        MutationBatch batch = context.getClient().prepareMutationBatch();
        ColumnListMutation<String> mutation = batch.withRow(columnFamily, 1234L);
        mutation.putColumn("AVAILABLE:1238", contentBuilder.build().toByteArray());

        ColumnListMutation<String> mutation2 = batch.withRow(columnFamily, 1235L);
        mutation2.putColumn("AVAILABLE:1238", contentBuilder.build().toByteArray());
        batch.execute();



        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getItemRefs().size(), is(2));
        assertThat(resolvedBrand.getAvailableContent().size(), is(1));
        assertThat(
                resolvedBrand.getAvailableContent().get(episode1.toRef()),
                containsInAnyOrder(location1.toSummary(), location2.toSummary(), location3.toSummary())
        );

        Series resolvedSeries1 = (Series) resolve(1235L);
        assertThat(resolvedSeries1.getBrandRef().getId().longValue(), is(1234L));
        assertThat(resolvedSeries1.getItemRefs().size(), is(2));
        assertThat(resolvedSeries1.getAvailableContent().size(), is(1));
        assertThat(
                resolvedSeries1.getAvailableContent().get(episode1.toRef()),
                containsInAnyOrder(location1.toSummary(), location2.toSummary(), location3.toSummary())
        );


        Episode resolvedEpisode1 = (Episode) resolve(1237L);
        assertThat(resolvedEpisode1.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode1.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode1.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode1.getContainerSummary().getTitle(), is("Brand"));

    }

    @Test(expected=CorruptContentException.class)
    public void testWritingResolvingContainerWhichOnlyChildRefsThrowsCorrectException() throws Exception {

        DateTime now = new DateTime(DateTimeZones.UTC);


        ContentProtos.Content.Builder contentBuilder = ContentProtos.Content.newBuilder();

        Episode episode2 = create(new Episode());
        episode2.setId(12345L);
        episode2.setThisOrChildLastUpdated(now);

        Location location4 = new Location();
        location4.setAvailable(true);
        location4.setUri("location4");
        Policy policy2 = new Policy();
        policy2.setAvailabilityStart(now.plusHours(1));
        policy2.setAvailabilityEnd(now.plusHours(2));
        location4.setPolicy(policy2);

        contentBuilder.addAvailableContent(
                new ItemAndLocationSummarySerializer()
                        .serialize(
                                episode2.toRef(),
                                ImmutableList.of(location4.toSummary())
                        )
        );

        ColumnFamily<Long, String> columnFamily = ColumnFamily.newColumnFamily(
                CONTENT_TABLE,
                LongSerializer.get(),
                StringSerializer.get()
        );

        MutationBatch batch = context.getClient().prepareMutationBatch();
        ColumnListMutation<String> mutation = batch.withRow(columnFamily, 1234L);
        mutation.putColumn("AVAILABLE:1238", contentBuilder.build().toByteArray());
        batch.execute();

        try {
            resolve(1234L);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Test
    public void testWriteContentSummaryOnBrandForItem() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Item item = create(new Item());
        item.setContainer(brandWriteResult.getResource());
        item.setTitle("Title 1");
        item.setImage("image1");
        item.setDescription("description");

        when(idGenerator.generateRaw()).thenReturn(1235L);
        store.writeContent(item);

        Brand resolved = (Brand)resolve(1234L);

        ItemSummary storedSummary = Iterables.getOnlyElement(resolved.getItemSummaries());

        assertThat(storedSummary.getItemRef(), is(item.toRef()));
        assertThat(storedSummary.getTitle(), is(item.getTitle()));
        assertThat(storedSummary.getImage().get(), is(item.getImage()));
        assertThat(storedSummary.getDescription().get(), is(item.getDescription()));


    }

    @Test
    public void testWriteContentSummaryOnBrandForEpisodeWithoutSeries() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
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

        when(idGenerator.generateRaw()).thenReturn(1235L);
        store.writeContent(episode);

        Brand resolved = (Brand)resolve(1234L);

        EpisodeSummary storedSummary = (EpisodeSummary)Iterables.getOnlyElement(resolved.getItemSummaries());

        assertThat(storedSummary.getItemRef(), is(episode.toRef()));
        assertThat(storedSummary.getTitle(), is(episode.getTitle()));
        assertThat(storedSummary.getImage().get(), is(episode.getImage()));
        assertThat(storedSummary.getDescription().get(), is(episode.getDescription()));
        assertThat(storedSummary.getEpisodeNumber().get(), is(episode.getEpisodeNumber()));

    }
    @Test
    public void testWriteContentSummaryOnSeriesForEpisodeWithSeries() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> seriesWriteResult = store.writeContent(series1);

        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setSeries(seriesWriteResult.getResource());
        episode.setTitle("Title 1");
        episode.setImage("image1");
        episode.setEpisodeNumber(42);
        episode.setDescription("description");

        when(idGenerator.generateRaw()).thenReturn(1236L);
        store.writeContent(episode);

        Brand resolvedBrand = (Brand)resolve(1234L);

        Series resolvedSeries = (Series)resolve(1235L);

        assertThat(resolvedBrand.getItemSummaries().isEmpty(), is(true));

        EpisodeSummary storedSummary = (EpisodeSummary)Iterables.getOnlyElement(resolvedSeries.getItemSummaries());

        assertThat(storedSummary.getItemRef(), is(episode.toRef()));
        assertThat(storedSummary.getTitle(), is(episode.getTitle()));
        assertThat(storedSummary.getImage().get(), is(episode.getImage()));
        assertThat(storedSummary.getDescription().get(), is(episode.getDescription()));
        assertThat(storedSummary.getEpisodeNumber().get(), is(episode.getEpisodeNumber()));

    }

    @Test
    public void testDeleteSeriesReferenceIfContentIsNotActivelyPublished() throws Exception {

        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        store.writeContent(series1);

        Series series2 = create(new Series());
        series2.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1236L);
        store.writeContent(series2);


        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getSeriesRefs(), is(ImmutableList.of(series1.toRef(), series2.toRef())));

        series1.setActivelyPublished(false);

        when(hasher.hash(argThat(isA(Content.class)))).thenReturn("hash").thenReturn("anotherHash");

        store.writeContent(series1);

        resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getSeriesRefs(), is(ImmutableList.of(series2.toRef())));
    }


    private <T extends Content> T create(T content) {
        content.setPublisher(Publisher.BBC);
        content.setTitle(content.getClass().getSimpleName());
        return content;
    }
    
    private Content resolve(Long id) throws InterruptedException, ExecutionException, TimeoutException {
        Resolved<Content> resolved = store.resolveIds(ImmutableList.of(Id.valueOf(id))).get(1, TimeUnit.SECONDS);
        return Iterables.getOnlyElement(resolved.getResources());
    }


    @Test
    public void testDeletesItemReferencesFromContainersWhenContentIsNoLongerActivelyPublished() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> seriesWriteResult = store.writeContent(series1);

        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setSeries(seriesWriteResult.getResource());
        episode.setTitle("Title 1");
        episode.setImage("image1");
        episode.setEpisodeNumber(42);
        episode.setDescription("description");

        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");
        Set<Broadcast> broadcasts = ImmutableSet.of(
                broadcast1,
                new Broadcast(
                        Id.valueOf(1),
                        DateTime.now(DateTimeZone.UTC).minusHours(2),
                        DateTime.now(DateTimeZone.UTC).minusHours(1)
                ).withId("sourceId:2")
        );
        episode.setBroadcasts(
                broadcasts
        );

        Encoding encoding = new Encoding();

        Location location = new Location();
        location.setAvailable(true);
        location.setUri("location");
        Policy policy1 = new Policy();
        policy1.setAvailabilityStart(now.minusHours(1));
        policy1.setAvailabilityEnd(now.plusHours(1));
        location.setPolicy(policy1);


        encoding.setAvailableAt(ImmutableSet.of(location));
        episode.setManifestedAs(ImmutableSet.of(encoding));


        when(idGenerator.generateRaw()).thenReturn(1236L);
        store.writeContent(episode);

        Map<ItemRef, Iterable<BroadcastRef>> expectedUpcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>> builder()
                .put(episode.toRef(), ImmutableList.of(broadcast1.toRef()))
                .build();

        Brand resolvedBrand = (Brand)resolve(1234L);

        Series resolvedSeries = (Series)resolve(1235L);


        EpisodeSummary storedSummary = (EpisodeSummary)Iterables.getOnlyElement(resolvedSeries.getItemSummaries());

        assertThat(storedSummary.getItemRef().getId(), is(episode.getId()));
        assertThat(resolvedSeries.getItemRefs(), is(ImmutableList.of(episode.toRef())));
        assertThat(resolvedSeries.getUpcomingContent(), is(expectedUpcomingContent));
        assertThat(resolvedSeries.getAvailableContent().get(episode.toRef()), containsInAnyOrder(location.toSummary()));

        assertThat(resolvedBrand.getUpcomingContent(), is(expectedUpcomingContent));
        assertThat(resolvedBrand.getAvailableContent().get(episode.toRef()), containsInAnyOrder(location.toSummary()));

        episode.setActivelyPublished(false);
        when(hasher.hash(argThat(isA(Content.class)))).thenReturn("hash").thenReturn("anotherHash");

        store.writeContent(episode);

        resolvedBrand = (Brand)resolve(1234L);

        resolvedSeries = (Series)resolve(1235L);

        assertThat(resolvedSeries.getItemSummaries().isEmpty(), is(true) );
        assertThat(resolvedSeries.getItemRefs().isEmpty(), is(true));
        assertThat(resolvedSeries.getUpcomingContent().isEmpty(), is(true));
        assertThat(resolvedSeries.getAvailableContent().isEmpty(), is(true));

        assertThat(resolvedBrand.getUpcomingContent().isEmpty(), is(true));
        assertThat(resolvedBrand.getAvailableContent().isEmpty(), is(true));
        assertThat(resolvedBrand.getItemRefs().isEmpty(), is(true));

    }

    @Test
    public void testDeletesItemWithoutSeriesReferencesFromContainersWhenContentIsNoLongerActivelyPublished() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);



        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setTitle("Title 1");
        episode.setImage("image1");
        episode.setEpisodeNumber(42);
        episode.setDescription("description");

        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");
        Set<Broadcast> broadcasts = ImmutableSet.of(
                broadcast1,
                new Broadcast(
                        Id.valueOf(1),
                        DateTime.now(DateTimeZone.UTC).minusHours(2),
                        DateTime.now(DateTimeZone.UTC).minusHours(1)
                ).withId("sourceId:2")
        );
        episode.setBroadcasts(
                broadcasts
        );

        Encoding encoding = new Encoding();

        Location location = new Location();
        location.setAvailable(true);
        location.setUri("location");
        Policy policy1 = new Policy();
        policy1.setAvailabilityStart(now.minusHours(1));
        policy1.setAvailabilityEnd(now.plusHours(1));
        location.setPolicy(policy1);


        encoding.setAvailableAt(ImmutableSet.of(location));
        episode.setManifestedAs(ImmutableSet.of(encoding));


        when(idGenerator.generateRaw()).thenReturn(1236L);
        store.writeContent(episode);

        Map<ItemRef, Iterable<BroadcastRef>> expectedUpcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>> builder()
                .put(episode.toRef(), ImmutableList.of(broadcast1.toRef()))
                .build();

        Brand resolvedBrand = (Brand)resolve(1234L);

        EpisodeSummary storedSummary = (EpisodeSummary)Iterables.getOnlyElement(resolvedBrand.getItemSummaries());

        assertThat(storedSummary.getItemRef().getId(), is(episode.getId()));

        assertThat(resolvedBrand.getItemRefs(), is(ImmutableList.of(episode.toRef())));
        assertThat(resolvedBrand.getUpcomingContent(), is(expectedUpcomingContent));
        assertThat(resolvedBrand.getAvailableContent().get(episode.toRef()), containsInAnyOrder(location.toSummary()));

        episode.setActivelyPublished(false);
        when(hasher.hash(argThat(isA(Content.class)))).thenReturn("hash").thenReturn("anotherHash");

        store.writeContent(episode);

        resolvedBrand = (Brand)resolve(1234L);

        assertThat(resolvedBrand.getItemRefs().isEmpty(), is(true));
        assertThat(resolvedBrand.getItemSummaries().isEmpty(), is(true) );
        assertThat(resolvedBrand.getUpcomingContent().isEmpty(), is(true));
        assertThat(resolvedBrand.getAvailableContent().isEmpty(), is(true));
    }


    @Test
    public void testDoesntOverwriteExistingBroadcastWhileWritingBroadcasts() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        Film film = create(new Film());
        film.setId(12345L);
        film.setThisOrChildLastUpdated(DateTime.now(DateTimeZone.UTC));
        film.setLastUpdated(DateTime.now(DateTimeZone.UTC));

        when(clock.now()).thenReturn(DateTime.now(DateTimeZone.UTC));

        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");

        Broadcast broadcast2 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).minusHours(2),
                DateTime.now(DateTimeZone.UTC).minusHours(1)
        ).withId("sourceId:2");
        Set<Broadcast> broadcasts = ImmutableSet.of(
                broadcast1,
                broadcast2
        );

        film.setBroadcasts(broadcasts);

        store.writeContent(film);

        Item resolved = (Item) resolve(12345L);

        assertThat(resolved.getBroadcasts(), is(ImmutableSet.of(broadcast1, broadcast2)));
        Broadcast broadcast3 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).minusHours(2),
                DateTime.now(DateTimeZone.UTC).minusHours(1)
        ).withId("sourceId:3");

        store.writeBroadcast(film.toRef(), Optional.absent(), Optional.absent(), broadcast3);

        resolved = (Item) resolve(12345L);
        assertThat(resolved.getBroadcasts(), is(ImmutableSet.of(broadcast1, broadcast2, broadcast3)));

    }

    @Test
    public void testWritesUpcomingBroadcastsToContainerWhenWritingBroadcast() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);
        Episode episode = create(new Episode());
        episode.setId(12345L);
        episode.setThisOrChildLastUpdated(DateTime.now(DateTimeZone.UTC));
        episode.setLastUpdated(DateTime.now(DateTimeZone.UTC));

        when(clock.now()).thenReturn(DateTime.now(DateTimeZone.UTC));

        Broadcast broadcast = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");


        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> series1WriteResult = store.writeContent(series1);


        episode.setContainer(brandWriteResult.getResource());
        episode.setSeries(series1WriteResult.getResource());



        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeBroadcast(episode.toRef(), Optional.of(brand.toRef()), Optional.of(series1.toRef()), broadcast);

        Map<ItemRef, Iterable<BroadcastRef>> expectedUpcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>> builder()
                .put(episode.toRef(), ImmutableList.of(broadcast.toRef()))
                .build();

        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getUpcomingContent(), is(expectedUpcomingContent));

        Series resolvedSeries1 = (Series) resolve(1235L);
        assertThat(resolvedSeries1.getUpcomingContent(), is(expectedUpcomingContent));

    }

    @Test
    public void testUpdatingContainerUpdatesContainerSummaryInChildItemAndSendsResourceUpdatedMessage() throws Exception {

        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());


        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Series series1 = create(new Series());
        series1.setBrand(brandWriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(1));
        when(idGenerator.generateRaw()).thenReturn(1235L);
        WriteResult<Series, Content> series1WriteResult = store.writeContent(series1);

        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setSeries(series1WriteResult.getResource());

        when(clock.now()).thenReturn(now.plusHours(2));
        when(idGenerator.generateRaw()).thenReturn(1237L);
        store.writeContent(episode);

        when(hasher.hash(argThat(isA(Content.class))))
                .thenReturn("different")
                .thenReturn("differentAgain");


        Episode resolvedEpisode = (Episode) resolve(1237L);
        assertThat(resolvedEpisode.getFirstSeen(), is(now.plusHours(2)));
        assertThat(resolvedEpisode.getLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode.getThisOrChildLastUpdated(), is(now.plusHours(2)));
        assertThat(resolvedEpisode.getContainerRef().getId().longValue(), is(1234L));
        assertThat(resolvedEpisode.getSeriesRef().getId().longValue(), is(1235L));
        assertThat(resolvedEpisode.getContainerSummary().getTitle(), is("Brand"));

        brand.setTitle("NewBrand");
        store.writeContent(brand);

        Brand resolvedBrand = (Brand) resolve(1234L);
        assertThat(resolvedBrand.getTitle(), is("NewBrand"));

        ArgumentCaptor<ResourceUpdatedMessage> captor = ArgumentCaptor.forClass(ResourceUpdatedMessage.class);

        verify(sender, times(5)).sendMessage(captor.capture());

        List<ResourceUpdatedMessage> messagesSent = captor.getAllValues();

        assertThat(messagesSent.size(), is(5));
        assertThat(messagesSent.get(3).getUpdatedResource().getId().longValue(), is(1237L));

        Episode newResolvedEpisode = (Episode) resolve(1237L);
        assertThat(newResolvedEpisode.getContainerSummary().getTitle(), is("NewBrand"));

    }

    @Test
    public void testReferencesOfItemAreDeletedWhenContainerChanges() throws WriteException, InterruptedException, ExecutionException, TimeoutException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Brand brand2 = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(12345L);
        WriteResult<Brand, Content> brandWriteResult2 = store.writeContent(brand2);



        Episode episode = create(new Episode());
        episode.setContainer(brandWriteResult.getResource());
        episode.setTitle("Title 1");
        episode.setImage("image1");
        episode.setEpisodeNumber(42);
        episode.setDescription("description");

        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");
        Set<Broadcast> broadcasts = ImmutableSet.of(
                broadcast1,
                new Broadcast(
                        Id.valueOf(1),
                        DateTime.now(DateTimeZone.UTC).minusHours(2),
                        DateTime.now(DateTimeZone.UTC).minusHours(1)
                ).withId("sourceId:2")
        );
        episode.setBroadcasts(
                broadcasts
        );

        Encoding encoding = new Encoding();

        Location location = new Location();
        location.setAvailable(true);
        location.setUri("location");
        Policy policy1 = new Policy();
        policy1.setAvailabilityStart(now.minusHours(1));
        policy1.setAvailabilityEnd(now.plusHours(1));
        location.setPolicy(policy1);


        encoding.setAvailableAt(ImmutableSet.of(location));
        episode.setManifestedAs(ImmutableSet.of(encoding));


        when(idGenerator.generateRaw()).thenReturn(1236L);
        store.writeContent(episode);

        Map<ItemRef, Iterable<BroadcastRef>> expectedUpcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>> builder()
                .put(episode.toRef(), ImmutableList.of(broadcast1.toRef()))
                .build();

        Brand resolvedBrand = (Brand)resolve(1234L);
        Brand resolvedBrand2 = (Brand)resolve(12345L);

        EpisodeSummary storedSummary = (EpisodeSummary)Iterables.getOnlyElement(resolvedBrand.getItemSummaries());

        assertThat(storedSummary.getItemRef().getId(), is(episode.getId()));

        assertThat(resolvedBrand.getItemRefs(), is(ImmutableList.of(episode.toRef())));
        assertThat(resolvedBrand.getUpcomingContent(), is(expectedUpcomingContent));
        assertThat(resolvedBrand.getAvailableContent().get(episode.toRef()), containsInAnyOrder(location.toSummary()));

        assertThat(resolvedBrand2.getItemRefs().isEmpty(), is(true));
        assertThat(resolvedBrand2.getItemSummaries().isEmpty(), is(true) );
        assertThat(resolvedBrand2.getUpcomingContent().isEmpty(), is(true));
        assertThat(resolvedBrand2.getAvailableContent().isEmpty(), is(true));

        episode.setContainer(brandWriteResult2.getResource());
        when(hasher.hash(argThat(isA(Content.class)))).thenReturn("hash").thenReturn("anotherHash");

        store.writeContent(episode);

        resolvedBrand = (Brand)resolve(1234L);

        assertThat(resolvedBrand.getItemRefs().isEmpty(), is(true));
        assertThat(resolvedBrand.getItemSummaries().isEmpty(), is(true));
        assertThat(resolvedBrand.getUpcomingContent().isEmpty(), is(true));
        assertThat(resolvedBrand.getAvailableContent().isEmpty(), is(true));

        resolvedBrand2 = (Brand)resolve(12345L);

        assertThat(resolvedBrand2.getItemRefs(), is(ImmutableList.of(episode.toRef())));
        assertThat(resolvedBrand2.getUpcomingContent(), is(expectedUpcomingContent));
        assertThat(resolvedBrand2.getAvailableContent().get(episode.toRef()), containsInAnyOrder(location.toSummary()));
    }

    @Test
    public void testReferencesOfItemAreDeletedWhenContainerGetsRemoved() throws InterruptedException, ExecutionException, TimeoutException, WriteException {
        DateTime now = new DateTime(DateTimeZones.UTC);

        Brand brand = create(new Brand());

        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L);
        WriteResult<Brand, Content> brandWriteResult = store.writeContent(brand);

        Item item = create(new Item());
        item.setContainer(brandWriteResult.getResource());
        item.setTitle("Title 1");
        item.setImage("image1");
        item.setDescription("description");

        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        ).withId("sourceID:1");
        Set<Broadcast> broadcasts = ImmutableSet.of(
                broadcast1,
                new Broadcast(
                        Id.valueOf(1),
                        DateTime.now(DateTimeZone.UTC).minusHours(2),
                        DateTime.now(DateTimeZone.UTC).minusHours(1)
                ).withId("sourceId:2")
        );
        item.setBroadcasts(
                broadcasts
        );

        Encoding encoding = new Encoding();

        Location location = new Location();
        location.setAvailable(true);
        location.setUri("location");
        Policy policy1 = new Policy();
        policy1.setAvailabilityStart(now.minusHours(1));
        policy1.setAvailabilityEnd(now.plusHours(1));
        location.setPolicy(policy1);

        encoding.setAvailableAt(ImmutableSet.of(location));
        item.setManifestedAs(ImmutableSet.of(encoding));

        when(idGenerator.generateRaw()).thenReturn(1236L);
        store.writeContent(item);

        Map<ItemRef, Iterable<BroadcastRef>> expectedUpcomingContent = ImmutableMap.<ItemRef, Iterable<BroadcastRef>> builder()
                .put(item.toRef(), ImmutableList.of(broadcast1.toRef()))
                .build();

        Brand resolvedBrand = (Brand)resolve(1234L);

        EpisodeSummary storedSummary = (EpisodeSummary)Iterables.getOnlyElement(resolvedBrand.getItemSummaries());

        assertThat(storedSummary.getItemRef().getId(), is(item.getId()));

        assertThat(resolvedBrand.getItemRefs(), is(ImmutableList.of(item.toRef())));
        assertThat(resolvedBrand.getUpcomingContent(), is(expectedUpcomingContent));
        assertThat(resolvedBrand.getAvailableContent().get(item.toRef()), containsInAnyOrder(location.toSummary()));

        item.setContainer(null);
        when(hasher.hash(argThat(isA(Content.class)))).thenReturn("hash").thenReturn("anotherHash");

        store.writeContent(item);

        resolvedBrand = (Brand)resolve(1234L);

        assertThat(resolvedBrand.getItemRefs().isEmpty(), is(true));
        assertThat(resolvedBrand.getItemSummaries().isEmpty(), is(true));
        assertThat(resolvedBrand.getUpcomingContent().isEmpty(), is(true));
        assertThat(resolvedBrand.getAvailableContent().isEmpty(), is(true));

    }


}
