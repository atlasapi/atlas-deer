package org.atlasapi.elasticsearch.content;

//import java.io.IOException;
//import java.math.BigInteger;
//import java.util.Currency;
//import java.util.Map;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//
//import org.atlasapi.EsSchema;
//import org.atlasapi.channel.ChannelGroup;
//import org.atlasapi.channel.ChannelGroupRef;
//import org.atlasapi.channel.ChannelGroupResolver;
//import org.atlasapi.channel.ChannelNumbering;
//import org.atlasapi.channel.ChannelRef;
//import org.atlasapi.content.ContentType;
//import org.atlasapi.content.IndexException;
//import org.atlasapi.content.IndexQueryResult;
//import org.atlasapi.content.Tag;
//import org.atlasapi.criteria.AttributeQuery;
//import org.atlasapi.criteria.AttributeQuerySet;
//import org.atlasapi.criteria.EnumAttributeQuery;
//import org.atlasapi.criteria.IdAttributeQuery;
//import org.atlasapi.criteria.StringAttributeQuery;
//import org.atlasapi.criteria.attribute.Attributes;
//import org.atlasapi.criteria.operator.Operators;
//import org.atlasapi.entity.Id;
//import org.atlasapi.entity.util.Resolved;
//import org.atlasapi.media.entity.Publisher;
//import org.atlasapi.util.CassandraSecondaryIndex;
//import org.atlasapi.util.ElasticSearchHelper;
//
//import com.metabroadcast.common.currency.Price;
//import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
//import com.metabroadcast.common.intl.Countries;
//import com.metabroadcast.common.query.Selection;
//
//import com.google.common.collect.FluentIterable;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.util.concurrent.Futures;
//import com.google.common.util.concurrent.ListenableFuture;
//import org.apache.log4j.ConsoleAppender;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PatternLayout;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.client.Requests;
//import org.joda.time.DateTime;
//import org.joda.time.LocalDate;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import static org.atlasapi.content.ComplexItemTestDataBuilder.complexItem;
//import static org.hamcrest.Matchers.containsInAnyOrder;
//import static org.hamcrest.Matchers.equalTo;
//import static org.hamcrest.Matchers.is;
//import static org.hamcrest.Matchers.not;
//import static org.junit.Assert.assertThat;
//import static org.mockito.Matchers.any;
//import static org.mockito.Matchers.anyList;
//import static org.mockito.Matchers.eq;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;

/**
 * Tests are commented out since the migration to Sherlock. They will either be moved to the Sherlock
 * project, or the Sherlock project will need to gain the functionality for creating indices.
 */
public class EsUnequivalentContentIndexTest {

//    private static final Client esClient = ElasticSearchHelper.testNode().client();
//
//    private static final SubstitutionTableNumberCodec codec =
//            SubstitutionTableNumberCodec.lowerCaseOnly();
//
//    private final CassandraSecondaryIndex equivIdIndex = mock(CassandraSecondaryIndex.class);
//    private final ChannelGroupResolver channelGroupResolver = mock(ChannelGroupResolver.class);
//
//    private EsUnequivalentContentIndex index;
//
//    @BeforeClass
//    public static void before() throws Exception {
//        Logger root = Logger.getRootLogger();
//        root.addAppender(new ConsoleAppender(
//                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
//        root.setLevel(Level.INFO);
//    }
//
//    @AfterClass
//    public static void after() throws Exception {
//        esClient.close();
//    }
//
//    @Before
//    public void setup() {
//        index = EsUnequivalentContentIndex.create(
//                esClient, EsSchema.CONTENT_INDEX,
//                channelGroupResolver, equivIdIndex, 60_000
//        );
//        index.startAsync().awaitRunning();
//    }
//
//    @After
//    public void teardown() throws Exception {
//        ElasticSearchHelper.clearIndices(esClient);
//        ElasticSearchHelper.refresh(esClient);
//    }
//
//    @Test
//    public void testGenreQuery() throws Exception {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                10L,
//                10L
//        )));
//        Item item = complexItem().withId(10L).build();
//        item.setGenres(ImmutableSet.of("horror", "action"));
//        indexAndRefresh(item);
//
//        AttributeQuery<String> genreQuery = Attributes.GENRE.createQuery(
//                Operators.EQUALS, ImmutableList.of("horror")
//        );
//        Set<AttributeQuery<?>> querySet = AttributeQuerySet.create(ImmutableList.of(genreQuery));
//        IndexQueryResult result = Futures.get(
//                index.query(
//                        querySet,
//                        ImmutableList.of(Publisher.BBC),
//                        Selection.all()
//                ),
//                Exception.class
//        );
//        assertThat(result.getIds().first().get(), is(Id.valueOf(10L)));
//    }
//
//    @Test
//    public void testBrandFilterWithTopicFilterForEpisode() throws Exception {
//        when(equivIdIndex.lookup(anyList()))
//                .thenReturn(Futures.immediateFuture(ImmutableMap.of(10L, 10L, 20L, 20L)));
//        when(equivIdIndex.reverseLookup(Id.valueOf(10L)))
//                .thenReturn(Futures.immediateFuture(ImmutableSet.of(10L)));
//        when(equivIdIndex.reverseLookup(Id.valueOf(20L)))
//                .thenReturn(Futures.immediateFuture(ImmutableSet.of(20L)));
//
//        Brand brand = new Brand(Id.valueOf(10L), Publisher.METABROADCAST);
//        brand.setTitle("Test Brand");
//
//        Episode episode = new Episode(Id.valueOf(20L), Publisher.METABROADCAST);
//        episode.setTitle("Test episode");
//        episode.setContainerRef(brand.toRef());
//        episode.setTags(
//                ImmutableList.of(
//                        new Tag(
//                                Id.valueOf(25L),
//                                0.0f,
//                                false,
//                                Tag.Relationship.ABOUT
//                        )
//                )
//        );
//        indexAndRefresh(brand, episode);
//
//        ListenableFuture<IndexQueryResult> future = index.query(
//                AttributeQuerySet.create(
//                        ImmutableSet.of(
//                                new StringAttributeQuery(
//                                        Attributes.SEARCH_TOPIC_ID,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(codec.encode(BigInteger.valueOf(25L)))
//                                ),
//                                new IdAttributeQuery(
//                                        Attributes.BRAND_ID,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(Id.valueOf(10L))
//                                )
//                        )
//                ),
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        );
//        IndexQueryResult result = Futures.get(future, IOException.class);
//        assertThat(result.getIds().size(), is(1));
//    }
//
//    @Test
//    public void testActionableContentWithLocationAvailable() throws Exception {
//        Publisher publisher = Publisher.METABROADCAST;
//
//        when(equivIdIndex.lookup(any()))
//                .thenReturn(Futures.immediateFuture(
//                        ImmutableMap.of(1L, 1L, 2L, 2L)
//                ));
//
//        Policy availablePolicy = new Policy();
//        availablePolicy.setAvailabilityStart(DateTime.now().minusDays(1));
//        availablePolicy.setAvailabilityEnd(DateTime.now().plusDays(30));
//
//        Policy nonAvailablePolicy = new Policy();
//        nonAvailablePolicy.setAvailabilityStart(DateTime.now().plusDays(1));
//        nonAvailablePolicy.setAvailabilityEnd(DateTime.now().plusDays(3));
//
//        Item availableItem = getItemWithPolicy(Id.valueOf(1L), publisher, availablePolicy);
//
//        Item nonAvailableItem = getItemWithPolicy(Id.valueOf(2L), publisher, nonAvailablePolicy);
//
//        indexAndRefresh(availableItem, nonAvailableItem);
//
//        ListenableFuture<IndexQueryResult> resultFuture = index.query(
//                AttributeQuerySet.create(
//                        ImmutableSet.of(
//                                new EnumAttributeQuery<>(
//                                        Attributes.CONTENT_TYPE,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(ContentType.ITEM)
//                                ),
//                                new StringAttributeQuery(
//                                        Attributes.ACTIONABLE_FILTER_PARAMETERS,
//                                        Operators.EQUALS,
//                                        ImmutableList.of("location.available:true")
//                                )
//                        )
//                ),
//                ImmutableList.of(publisher),
//                Selection.all()
//        );
//        IndexQueryResult result = Futures.get(resultFuture, IOException.class);
//        FluentIterable<Id> ids = result.getIds();
//        assertThat(ids, containsInAnyOrder(Id.valueOf(1L)));
//        assertThat(ids, not(containsInAnyOrder(Id.valueOf(2L))));
//
//    }
//
//    @Test
//    public void testActionableContentWithBroadcastAvailable() throws Exception {
//        Publisher publisher = Publisher.METABROADCAST;
//
//        when(equivIdIndex.lookup(any()))
//                .thenReturn(Futures.immediateFuture(
//                        ImmutableMap.of(1L, 1L, 2L, 2L)
//                ));
//
//        Item availableItem = new Item(Id.valueOf(1L), publisher);
//        availableItem.setBroadcasts(ImmutableSet.of(
//                new Broadcast(Id.valueOf(1L), DateTime.now(), DateTime.now().plusHours(1))
//        ));
//
//        Item nonAvailableItem = new Item(Id.valueOf(2L), publisher);
//        nonAvailableItem.setBroadcasts(ImmutableSet.of(
//                new Broadcast(Id.valueOf(2L), DateTime.now().plusHours(10),
//                        DateTime.now().plusHours(11)
//                )
//        ));
//
//        indexAndRefresh(availableItem, nonAvailableItem);
//
//        ListenableFuture<IndexQueryResult> resultFuture = index.query(
//                AttributeQuerySet.create(
//                        ImmutableSet.of(
//                                new EnumAttributeQuery<>(
//                                        Attributes.CONTENT_TYPE,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(ContentType.ITEM)
//                                ),
//                                new StringAttributeQuery(
//                                        Attributes.ACTIONABLE_FILTER_PARAMETERS,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(
//                                                "broadcast.time.gt:"
//                                                        + DateTime.now().minusHours(2).toString(),
//                                                "broadcast.time.lt:"
//                                                        + DateTime.now().plusHours(2).toString()
//                                        )
//                                )
//                        )
//                ),
//                ImmutableList.of(publisher),
//                Selection.all()
//        );
//        IndexQueryResult result = Futures.get(resultFuture, IOException.class);
//        FluentIterable<Id> ids = result.getIds();
//        assertThat(ids.contains(Id.valueOf(1L)), is(true));
//        assertThat(ids.contains(Id.valueOf(2L)), is(false));
//
//    }
//
//    @Test
//    public void testActionableBrandWithChildWithLocationAvailable() throws Exception {
//        Publisher publisher = Publisher.METABROADCAST;
//
//        when(equivIdIndex.lookup(any()))
//                .thenReturn(Futures.immediateFuture(
//                        ImmutableMap.of(1L, 1L, 2L, 2L, 3L, 3L)
//                ));
//
//        Policy availablePolicy = new Policy();
//        availablePolicy.setAvailabilityStart(DateTime.now().minusDays(1));
//        availablePolicy.setAvailabilityEnd(DateTime.now().plusDays(30));
//
//        Item availableItem = getItemWithPolicy(Id.valueOf(1L), publisher, availablePolicy);
//
//        Brand availableBrand = new Brand(Id.valueOf(2L), publisher);
//
//        Brand nonAvailableBrand = new Brand(Id.valueOf(3L), publisher);
//
//        availableItem.setContainerRef(availableBrand.toRef());
//
//        indexAndRefresh(availableBrand, availableItem, nonAvailableBrand);
//
//        ListenableFuture<IndexQueryResult> resultFuture = index.query(
//                AttributeQuerySet.create(
//                        ImmutableSet.of(
//                                new EnumAttributeQuery<>(
//                                        Attributes.CONTENT_TYPE,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(ContentType.BRAND)
//                                ),
//                                new StringAttributeQuery(
//                                        Attributes.ACTIONABLE_FILTER_PARAMETERS,
//                                        Operators.EQUALS,
//                                        ImmutableList.of("location.available:true")
//                                )
//                        )
//                ),
//                ImmutableList.of(publisher),
//                Selection.all()
//        );
//        IndexQueryResult result = Futures.get(resultFuture, IOException.class);
//        FluentIterable<Id> ids = result.getIds();
//        assertThat(ids, containsInAnyOrder(availableBrand.getId()));
//        assertThat(ids, not(containsInAnyOrder(nonAvailableBrand.getId())));
//    }
//
//    @Test
//    public void testActionableBrandWithChildWithBroadcastAvailable() throws Exception {
//        Publisher publisher = Publisher.METABROADCAST;
//
//        when(equivIdIndex.lookup(any()))
//                .thenReturn(Futures.immediateFuture(
//                        ImmutableMap.of(1L, 1L, 2L, 2L, 3L, 3L)
//                ));
//
//        Item availableItem = new Item(Id.valueOf(1L), publisher);
//        availableItem.setBroadcasts(ImmutableSet.of(
//                new Broadcast(Id.valueOf(1L), DateTime.now(), DateTime.now().plusHours(1))
//        ));
//
//        Brand availableBrand = new Brand(Id.valueOf(2L), publisher);
//
//        Brand nonAvailableBrand = new Brand(Id.valueOf(3L), publisher);
//
//        availableItem.setContainerRef(availableBrand.toRef());
//
//        indexAndRefresh(availableBrand, availableItem);
//
//        ListenableFuture<IndexQueryResult> resultFuture = index.query(
//                AttributeQuerySet.create(
//                        ImmutableSet.of(
//                                new EnumAttributeQuery<>(
//                                        Attributes.CONTENT_TYPE,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(ContentType.BRAND)
//                                ),
//                                new StringAttributeQuery(
//                                        Attributes.ACTIONABLE_FILTER_PARAMETERS,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(
//                                                "broadcast.time.gt:"
//                                                        + DateTime.now().minusHours(2).toString(),
//                                                "broadcast.time.lt"
//                                                        + DateTime.now().plusHours(2).toString()
//                                        )
//                                )
//                        )
//                ),
//                ImmutableList.of(publisher),
//                Selection.all()
//        );
//        IndexQueryResult result = Futures.get(resultFuture, IOException.class);
//        FluentIterable<Id> ids = result.getIds();
//        assertThat(ids, containsInAnyOrder(availableBrand.getId()));
//        assertThat(ids, not(containsInAnyOrder(nonAvailableBrand.getId())));
//    }
//
//    @Test
//    public void testActionableBrandWithChildWithBroadcastAvailableAndRegionFilter()
//            throws Exception {
//        Publisher publisher = Publisher.METABROADCAST;
//
//        when(equivIdIndex.lookup(any()))
//                .thenReturn(Futures.immediateFuture(
//                        ImmutableMap.of(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L)
//                ));
//
//        Id regionId = Id.valueOf(10L);
//        Id channelId = Id.valueOf(11L);
//
//        ChannelNumbering channelNumbering = new ChannelNumbering(
//                new ChannelGroupRef(regionId, publisher),
//                new ChannelRef(channelId, publisher),
//                LocalDate.now().minusYears(1),
//                LocalDate.now().plusYears(1),
//                "1"
//        );
//        ChannelGroup<ChannelNumbering> channelGroup = new ChannelGroup<>(
//                regionId,
//                publisher,
//                ImmutableSet.of(channelNumbering),
//                ImmutableSet.of(Countries.GB),
//                ImmutableSet.of()
//        );
//        when(channelGroupResolver.resolveIds(ImmutableList.of(regionId)))
//                .thenReturn(Futures.immediateFuture(
//                        Resolved.valueOf(ImmutableList.of(channelGroup))
//                ));
//
//        Item availableItem = new Item(Id.valueOf(1L), publisher);
//        availableItem.setBroadcasts(ImmutableSet.of(
//                new Broadcast(channelId, DateTime.now(), DateTime.now().plusHours(1))
//        ));
//
//        Brand availableBrand = new Brand(Id.valueOf(2L), publisher);
//
//        availableItem.setContainerRef(availableBrand.toRef());
//
//        Item nonAvailableItem = new Item(Id.valueOf(3L), publisher);
//        nonAvailableItem.setBroadcasts(ImmutableSet.of(
//                new Broadcast(Id.valueOf(100L), DateTime.now(), DateTime.now().plusHours(1))
//        ));
//
//        Brand nonAvailableBrand = new Brand(Id.valueOf(4L), publisher);
//
//        nonAvailableItem.setContainerRef(nonAvailableBrand.toRef());
//
//        indexAndRefresh(availableBrand, availableItem, nonAvailableBrand);
//
//        ListenableFuture<IndexQueryResult> resultFuture = index.query(
//                AttributeQuerySet.create(
//                        ImmutableSet.of(
//                                new EnumAttributeQuery<>(
//                                        Attributes.CONTENT_TYPE,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(ContentType.BRAND)
//                                ),
//                                new IdAttributeQuery(
//                                        Attributes.REGION,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(regionId)
//                                ),
//                                new StringAttributeQuery(
//                                        Attributes.ACTIONABLE_FILTER_PARAMETERS,
//                                        Operators.EQUALS,
//                                        ImmutableList.of(
//                                                "broadcast.time.gt:"
//                                                        + DateTime.now().minusHours(2).toString(),
//                                                "broadcast.time.lt"
//                                                        + DateTime.now().plusHours(2).toString()
//                                        )
//                                )
//                        )
//                ),
//                ImmutableList.of(publisher),
//                Selection.all()
//        );
//        IndexQueryResult result = Futures.get(resultFuture, IOException.class);
//        FluentIterable<Id> ids = result.getIds();
//        assertThat(ids, containsInAnyOrder(availableBrand.getId()));
//        assertThat(ids, not(containsInAnyOrder(nonAvailableBrand.getId())));
//    }
//
//    @Test
//    public void testPricingOrdering() throws Exception {
//        Policy policy1 = new Policy();
//        Policy policy2 = new Policy();
//
//        policy1.setPrice(new Price(Currency.getInstance("GBP"), 10));
//        policy2.setPrice(new Price(Currency.getInstance("GBP"), 20));
//
//        Location location1 = new Location();
//        Location location2 = new Location();
//
//        Encoding encoding1 = new Encoding();
//        Encoding encoding2 = new Encoding();
//
//        encoding1.setAvailableAt(ImmutableSet.of(location1));
//        encoding2.setAvailableAt(ImmutableSet.of(location2));
//
//        Item item1 = complexItem().withTitle("test!").withId(30L).build();
//        Item item2 = complexItem().withTitle("not!").withId(20L).build();
//
//        item1.setManifestedAs(ImmutableSet.of(encoding1));
//        item2.setManifestedAs(ImmutableSet.of(encoding2));
//        //TODO finish this test
//    }
//
//    @Test
//    public void testTitlePrefixQuery() throws Exception {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                20L,
//                20L,
//                30L,
//                30L
//        )));
//        Item item1 = complexItem().withTitle("test!").withId(30L).build();
//        Item item2 = complexItem().withTitle("not!").withId(20L).build();
//
//        indexAndRefresh(item1, item2);
//
//        AttributeQuery<String> query = Attributes.CONTENT_TITLE_PREFIX
//                .createQuery(Operators.BEGINNING, ImmutableList.of("te"));
//        IndexQueryResult result = Futures.get(
//                index.query(
//                        AttributeQuerySet.create(ImmutableList.of(query)),
//                        ImmutableList.of(Publisher.BBC),
//                        Selection.all()
//                ),
//                Exception.class
//        );
//        assertThat(result.getIds().size(), is(1));
//        assertThat(result.getIds().first().get(), is(item1.getId()));
//    }
//
//    @Test
//    public void testTitlePrefixQueryWithNonLetterCharacter() throws Exception {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                20L,
//                20L,
//                30L,
//                30L
//        )));
//        Item item1 = complexItem().withTitle("1test").withId(30L).build();
//        Item item2 = complexItem().withTitle("not!").withId(20L).build();
//
//        indexAndRefresh(item1, item2);
//
//        AttributeQuery<String> query = Attributes.CONTENT_TITLE_PREFIX
//                .createQuery(Operators.BEGINNING, ImmutableList.of("#"));
//        IndexQueryResult result = Futures.get(
//                index.query(
//                        AttributeQuerySet.create(ImmutableList.of(query)),
//                        ImmutableList.of(Publisher.BBC),
//                        Selection.all()
//                ),
//                Exception.class
//        );
//        assertThat(result.getIds().size(), is(1));
//        assertThat(result.getIds().first().get(), is(item1.getId()));
//    }
//
//    @Test
//    public void testSourceQuery() throws Exception {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                1l,
//                1l
//        )));
//        Content content = new Episode();
//        content.setId(1);
//        content.setPublisher(Publisher.METABROADCAST);
//
//        indexAndRefresh(content);
//
//        AttributeQuery<Publisher> query = Attributes.SOURCE
//                .createQuery(Operators.EQUALS, ImmutableList.of(Publisher.METABROADCAST));
//
//        Set<AttributeQuery<?>> querySet = AttributeQuerySet.create(ImmutableList.of(query));
//        ListenableFuture<IndexQueryResult> result = index.query(
//                querySet,
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        );
//
//        IndexQueryResult ids = result.get(1, TimeUnit.SECONDS);
//        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));
//
//        query = Attributes.SOURCE
//                .createQuery(Operators.EQUALS, ImmutableList.of(Publisher.BBC));
//
//        querySet = AttributeQuerySet.create(ImmutableList.of(query));
//        result = index.query(
//                querySet,
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        );
//
//        ids = result.get(1, TimeUnit.SECONDS);
//        assertThat(ids.getIds().isEmpty(), is(true));
//
//    }
//
//    @Test
//    public void testTopicQuery() throws Exception {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                1L,
//                1L
//        )));
//        Content content = new Episode();
//        content.setId(1);
//        content.setPublisher(Publisher.METABROADCAST);
//        content.setTags(ImmutableList.of(new Tag(
//                2L,
//                1.0f,
//                true,
//                Tag.Relationship.ABOUT
//        )));
//
//        indexAndRefresh(content);
//
//        AttributeQuery<String> query = Attributes.SEARCH_TOPIC_ID.createQuery(
//                Operators.EQUALS,
//                ImmutableList.of(codec.encode(BigInteger.valueOf(2L)))
//        );
//
//        Set<AttributeQuery<?>> querySet = AttributeQuerySet.create(ImmutableList.of(query));
//        ListenableFuture<IndexQueryResult> result = index.query(
//                querySet,
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        );
//
//        IndexQueryResult ids = result.get(1, TimeUnit.SECONDS);
//        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));
//    }
//
//    @Test
//    public void testQueryOrder() throws Exception {
//        for (Long id : ImmutableList.of(1L, 2L, 3L)) {
//            when(equivIdIndex.lookup(eq(ImmutableList.of(id))))
//                    .thenReturn(Futures.immediateFuture(ImmutableMap.of(id, id)));
//        }
//
//        Content episode1 = episode(1);
//        episode1.setTags(ImmutableList.of(new Tag(
//                4L,
//                1.0f,
//                true,
//                Tag.Relationship.ABOUT
//        )));
//        Content episode2 = episode(2);
//        episode2.setTags(ImmutableList.of(new Tag(
//                4L,
//                1.5f,
//                true,
//                Tag.Relationship.ABOUT
//        )));
//        Content episode3 = episode(3);
//        episode3.setTags(ImmutableList.of(new Tag(
//                4L,
//                2.0f,
//                false,
//                Tag.Relationship.ABOUT
//        )));
//
//        indexAndRefresh(episode1, episode2, episode3);
//
//        Set<AttributeQuery<?>> querySet = AttributeQuerySet.create(ImmutableList.of(
//                Attributes.SEARCH_TOPIC_ID.createQuery(
//                        Operators.EQUALS,
//                        ImmutableList.of(codec.encode(BigInteger.valueOf(4L)))
//                ),
//                Attributes.ORDER_BY.createQuery(
//                        Operators.EQUALS,
//                        ImmutableList.of("topics.weighting.desc")
//                )
//        ));
//        ListenableFuture<IndexQueryResult> result = index.query(
//                querySet,
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        );
//
//        IndexQueryResult ids = result.get();
//        assertThat(ids.getIds().get(0), is(Id.valueOf(3)));
//        assertThat(ids.getIds().get(1), is(Id.valueOf(2)));
//        assertThat(ids.getIds().get(2), is(Id.valueOf(1)));
//    }
//
//    @Test
//    public void orderByMissingFieldGetsContentWithNullsLast() throws Exception {
//        for (Long id : ImmutableList.of(1L, 2L, 3L)) {
//            when(equivIdIndex.lookup(eq(ImmutableList.of(id))))
//                    .thenReturn(Futures.immediateFuture(ImmutableMap.of(id, id)));
//        }
//
//        Content episode1 = episode(1);
//        episode1.setTags(ImmutableList.of(new Tag(
//                4L,
//                1.0f,
//                true,
//                Tag.Relationship.ABOUT
//        )));
//        Content episode2 = episode(2);
//        episode2.setTags(ImmutableList.of(new Tag(
//                4L,
//                1.5f,
//                true,
//                Tag.Relationship.ABOUT
//        )));
//        Content episode3 = episode(3);
//
//        indexAndRefresh(episode1, episode2, episode3);
//
//        Set<AttributeQuery<?>> querySet = AttributeQuerySet.create(ImmutableList.of(
//                Attributes.ID.createQuery(
//                        Operators.EQUALS,
//                        ImmutableList.of(Id.valueOf(1), Id.valueOf(2), Id.valueOf(3))
//                ),
//                Attributes.ORDER_BY.createQuery(
//                        Operators.EQUALS,
//                        ImmutableList.of("topics.weighting.desc")
//                )
//        ));
//
//        ListenableFuture<IndexQueryResult> result = index.query(
//                querySet,
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        );
//
//        IndexQueryResult ids = result.get();
//        assertThat(ids.getIds().get(0), is(Id.valueOf(2)));
//        assertThat(ids.getIds().get(1), is(Id.valueOf(1)));
//        assertThat(ids.getIds().get(2), is(Id.valueOf(3)));
//    }
//
//    private Content episode(int id) {
//        Content content = new Episode();
//        content.setId(id);
//        content.setPublisher(Publisher.METABROADCAST);
//        return content;
//    }
//
//    @Test
//    public void testTopicWeightingQuery() throws Exception {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                1l,
//                1l
//        )));
//        Content content = new Episode();
//        content.setId(1);
//        content.setPublisher(Publisher.METABROADCAST);
//        content.setTags(ImmutableList.of(new Tag(
//                2L,
//                1.0f,
//                true,
//                Tag.Relationship.ABOUT
//        )));
//
//        indexAndRefresh(content);
//
//        AttributeQuery<Float> query = Attributes.TAG_WEIGHTING.createQuery(
//                Operators.EQUALS, ImmutableList.of(1.0f));
//
//        IndexQueryResult ids = index.query(
//                AttributeQuerySet.create(ImmutableList.of(query)),
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        )
//                .get(1, TimeUnit.SECONDS);
//        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));
//
//        query = Attributes.TAG_WEIGHTING.createQuery(
//                Operators.LESS_THAN, ImmutableList.of(0.5f));
//
//        ids = index.query(
//                AttributeQuerySet.create(ImmutableList.of(query)),
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        )
//                .get(1, TimeUnit.SECONDS);
//        assertThat(ids.getIds().first().isPresent(), is(false));
//
//        query = Attributes.TAG_WEIGHTING.createQuery(
//                Operators.GREATER_THAN, ImmutableList.of(0.5f));
//
//        ids = index.query(
//                AttributeQuerySet.create(ImmutableList.of(query)),
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        )
//                .get(1, TimeUnit.SECONDS);
//        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));
//
//    }
//
//    private void indexAndRefresh(Content... contents) throws IndexException {
//        for (Content content : contents) {
//            index.index(content);
//        }
//        ElasticSearchHelper.refresh(esClient);
//    }
//
//    @Test
//    public void testUnindexingOfContentThatIsNoLongerPublished()
//            throws IndexException, ExecutionException, InterruptedException {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                10L,
//                10L
//        )));
//        Item item = complexItem().withId(20L).build();
//        item.setPublisher(Publisher.METABROADCAST);
//        item.setActivelyPublished(true);
//        indexAndRefresh(item);
//
//        Set<AttributeQuery<?>> querySet = AttributeQuerySet.create(
//                ImmutableList.of(
//                        Attributes.ID.createQuery(
//                                Operators.EQUALS,
//                                ImmutableList.of(Id.valueOf(20L))
//                        )
//                )
//        );
//        IndexQueryResult resultWithItemPresent = index.query(
//                querySet,
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        )
//                .get();
//        assertThat(resultWithItemPresent.getIds().first().get(), is(Id.valueOf(20L)));
//
//        item.setActivelyPublished(false);
//        indexAndRefresh(item);
//
//        IndexQueryResult resultWithItemAbsent = index.query(
//                querySet,
//                ImmutableList.of(Publisher.METABROADCAST),
//                Selection.all()
//        )
//                .get();
//        assertThat(
//                resultWithItemAbsent.getIds().first(),
//                is(com.google.common.base.Optional.absent())
//        );
//    }
//
//    @Test
//    public void testUpdatesCanonicalIdsCorrectly() throws IndexException {
//        when(equivIdIndex.lookup(any())).thenReturn(Futures.immediateFuture(ImmutableMap.of(
//                10L,
//                10L
//        )));
//        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
//        item.setTitle("Test title!");
//        indexAndRefresh(item);
//
//        Map<String, Object> resolvedFields = esClient.get(Requests.getRequest(EsSchema.CONTENT_INDEX)
//                .id("10")
//                .type(EsContent.TOP_LEVEL_ITEM))
//                .actionGet()
//                .getSource();
//
//        assertThat(resolvedFields.get(EsContent.TITLE), is(equalTo("Test title!")));
//        assertThat(resolvedFields.get(EsContent.CANONICAL_ID), is(equalTo(10)));
//
//        index.updateCanonicalIds(Id.valueOf(20L), ImmutableSet.of(Id.valueOf(10L)));
//
//        ElasticSearchHelper.refresh(esClient);
//
//        resolvedFields = esClient.get(Requests.getRequest(EsSchema.CONTENT_INDEX)
//                .id("10")
//                .type(EsContent.TOP_LEVEL_ITEM))
//                .actionGet()
//                .getSource();
//
//        assertThat(resolvedFields.get(EsContent.TITLE), is(equalTo("Test title!")));
//        assertThat(resolvedFields.get(EsContent.CANONICAL_ID), is(equalTo(20)));
//    }
//
//    private Item getItemWithPolicy(Id id, Publisher publisher, Policy policy) {
//        Item availableItem = new Item(id, publisher);
//
//        Location locationA = new Location();
//        locationA.setPolicy(policy);
//
//        Encoding encodingA = new Encoding();
//        encodingA.setAvailableAt(ImmutableSet.of(locationA));
//
//        availableItem.setManifestedAs(ImmutableSet.of(encodingA));
//        return availableItem;
//    }
}
