package org.atlasapi.content;

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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
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

@RunWith(MockitoJUnitRunner.class)
public class CassandraContentStoreIT {

    private static final AstyanaxContext<Keyspace> context = 
        CassandraHelper.testCassandraContext();
    
    @Mock private ContentHasher hasher;
    @Mock private IdGenerator idGenerator;
    @Mock private MessageSender<ResourceUpdatedMessage> sender;
    @Mock private Clock clock;
    
    private CassandraContentStore store;
    
    @Before
    public void before() {
        store = CassandraContentStore
                .builder(context, "Content", hasher, sender, idGenerator)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .build();
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
        CassandraHelper.createColumnFamily(context, "Content", LongSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, "Content_aliases", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    }
    
    @AfterClass
    public static void tearDown() throws ConnectionException {
        try {
            context.getClient().dropKeyspace();
        } catch (BadRequestException ire) { }
    }
    
    @After
    public void clearCf() throws ConnectionException {
        context.getClient().truncateColumnFamily("Content");
        context.getClient().truncateColumnFamily("Content_aliases");
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

    private <T extends Content> T create(T content) {
        content.setPublisher(Publisher.BBC);
        content.setTitle(content.getClass().getSimpleName());
        return content;
    }
    
    private Content resolve(Long id) throws InterruptedException, ExecutionException, TimeoutException {
        Resolved<Content> resolved = store.resolveIds(ImmutableList.of(Id.valueOf(id))).get(1, TimeUnit.SECONDS);
        return Iterables.getOnlyElement(resolved.getResources());
    }

}
