package org.atlasapi.content;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.query.Selection;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.NoOpSecondaryIndex;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.atlasapi.content.ComplexItemTestDataBuilder.complexItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class EsContentIndexTest {

    private static final Client esClient = ElasticSearchHelper.testNode().client();

    private EsContentIndex index;

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
    }

    @AfterClass
    public static void after() throws Exception {
        esClient.close();
    }

    @Before
    public void setup() {
        index = new EsContentIndex(esClient, EsSchema.CONTENT_INDEX, 60000, new NoOpContentResolver(), mock(ChannelGroupResolver.class), new NoOpSecondaryIndex());
        index.startAsync().awaitRunning();
    }

    @After
    public void teardown() throws Exception {
        ElasticSearchHelper.clearIndices(esClient);
        ElasticSearchHelper.refresh(esClient);
    }

    @Test
    public void testGenreQuery() throws Exception {
        Item item = complexItem().withId(10l).build();
        item.setGenres(ImmutableSet.of("horror", "action"));
        indexAndRefresh(item);

        AttributeQuery<String> genreQuery = Attributes.GENRE.createQuery(
                Operators.EQUALS, ImmutableList.of("horror")
        );
        AttributeQuerySet querySet = new AttributeQuerySet(ImmutableList.of(genreQuery));
        IndexQueryResult result = Futures.get(
                index.query(
                        querySet,
                        ImmutableList.of(Publisher.BBC),
                        Selection.all(),
                        Optional.empty()),
                Exception.class
        );
        assertThat(result.getIds().first().get(), is(Id.valueOf(10l)));
    }

    @Test
    public void testBrandFilterWithTopicFilterForEpisode() throws Exception {
        Brand brand = new Brand(Id.valueOf(10l), Publisher.METABROADCAST);
        brand.setTitle("Test Brand");

        Episode episode = new Episode(Id.valueOf(20l), Publisher.METABROADCAST);
        episode.setTitle("Test episode");
        episode.setContainerRef(brand.toRef());
        episode.setTags(
                ImmutableList.of(
                        new Tag(Id.valueOf(25l), 0.0f, false, Tag.Relationship.ABOUT)
                )
        );
        indexAndRefresh(brand, episode);

        ListenableFuture<IndexQueryResult> future = index.query(
                new AttributeQuerySet(
                        ImmutableSet.of(
                                new IdAttributeQuery(
                                        Attributes.TOPIC_ID,
                                        Operators.EQUALS,
                                        ImmutableList.of(Id.valueOf(25l))
                                )
                        )
                ),
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(),
                Optional.of(
                        new IndexQueryParams(
                                Optional.<FuzzyQueryParams>empty(),
                                Optional.<QueryOrdering>empty(),
                                Optional.<Id>empty(),
                                Optional.<Float>empty(),
                                Optional.<Float>empty(),
                                Optional.<List<List<InclusionExclusionId>>>empty(),
                                false,
                                Optional.of(Id.valueOf(10l)),
                                Optional.empty(),
                                Optional.empty()
                        )
                )
        );
        IndexQueryResult result = Futures.get(future, IOException.class);
        assertThat(result.getIds().size(), is(1));
    }

    @Test
    public void testActionableContentFilters() throws IndexException, IOException {
        Brand broadcastBrand = new Brand(Id.valueOf(10l), Publisher.METABROADCAST);
        broadcastBrand.setTitle("Broadcast brand");
        Item broadcastItem = new Item(Id.valueOf(1l), Publisher.METABROADCAST);
        broadcastItem.setContainerRef(broadcastBrand.toRef());
        broadcastItem.setBroadcasts(ImmutableSet.of(
                new Broadcast(Id.valueOf(1l), DateTime.now(), DateTime.now().plusHours(1))
        ));

        Brand vodBrand = new Brand(Id.valueOf(20l), Publisher.METABROADCAST);
        vodBrand.setTitle("Vod brand");
        Item vodItem = new Item(Id.valueOf(2l), Publisher.METABROADCAST);
        vodItem.setContainerRef(vodBrand.toRef());

        Policy pol = new Policy();
        pol.setAvailabilityStart(DateTime.now().minusDays(1));
        pol.setAvailabilityEnd(DateTime.now().plusDays(30));

        Location loc = new Location();
        loc.setPolicy(pol);

        Encoding encoding = new Encoding();
        encoding.setAvailableAt(ImmutableSet.of(loc));

        vodItem.setManifestedAs(ImmutableSet.of(encoding));

        Brand notAvailableBrand = new Brand(Id.valueOf(30l), Publisher.METABROADCAST);

        indexAndRefresh(vodBrand, vodItem, broadcastBrand, broadcastItem, notAvailableBrand);

        ListenableFuture<IndexQueryResult> resultFuture = index.query(
                new AttributeQuerySet(
                        ImmutableSet.of(
                                new EnumAttributeQuery<>(
                                        Attributes.CONTENT_TYPE,
                                        Operators.EQUALS,
                                        ImmutableList.of(ContentType.BRAND)
                                )
                        )
                ),
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(),
                Optional.of(
                        new IndexQueryParams(
                                Optional.<FuzzyQueryParams>empty(),
                                Optional.<QueryOrdering>empty(),
                                Optional.<Id>empty(),
                                Optional.<Float>empty(),
                                Optional.<Float>empty(),
                                Optional.empty(),
                                false,
                                Optional.empty(),
                                Optional.of(
                                        ImmutableMap.of(
                                                "location.available", "true",
                                                "broadcast.time.gt", DateTime.now().minusHours(2).toString()
                                                )
                                ),
                                Optional.empty()
                        )
                )
        );
        IndexQueryResult result = Futures.get(resultFuture, IOException.class);
        FluentIterable<Id> ids = result.getIds();
        assertThat(ids, containsInAnyOrder(Id.valueOf(10), Id.valueOf(20)));
        assertThat(ids, not(containsInAnyOrder(Id.valueOf(30))));
    }

    @Test
    public void testPricingOrdering() throws Exception {
        Policy policy1 = new Policy();
        Policy policy2 = new Policy();

        policy1.setPrice(new Price(Currency.getInstance("GBP"), 10));
        policy2.setPrice(new Price(Currency.getInstance("GBP"), 20));

        Location location1 = new Location();
        Location location2 = new Location();

        Encoding encoding1 = new Encoding();
        Encoding encoding2 = new Encoding();

        encoding1.setAvailableAt(ImmutableSet.of(location1));
        encoding2.setAvailableAt(ImmutableSet.of(location2));

        Item item1 = complexItem().withTitle("test!").withId(30l).build();
        Item item2 = complexItem().withTitle("not!").withId(20l).build();

        item1.setManifestedAs(ImmutableSet.of(encoding1));
        item2.setManifestedAs(ImmutableSet.of(encoding2));
        //TODO finish this test
    }

    @Test
    public void testTitlePrefixQuery() throws Exception {
        Item item1 = complexItem().withTitle("test!").withId(30l).build();
        Item item2 = complexItem().withTitle("not!").withId(20l).build();

        indexAndRefresh(item1, item2);

        AttributeQuery<String> query = Attributes.CONTENT_TITLE_PREFIX
                .createQuery(Operators.BEGINNING, ImmutableList.of("te"));
        IndexQueryResult result = Futures.get(
                index.query(
                        new AttributeQuerySet(ImmutableList.of(query)),
                        ImmutableList.of(Publisher.BBC),
                        Selection.all(),
                        Optional.empty()),
                Exception.class
        );
        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().first().get(), is(item1.getId()));
    }


    @Test
    public void testTitlePrefixQueryWithNonLetterCharacter() throws Exception {
        Item item1 = complexItem().withTitle("1test").withId(30l).build();
        Item item2 = complexItem().withTitle("not!").withId(20l).build();

        indexAndRefresh(item1, item2);

        AttributeQuery<String> query = Attributes.CONTENT_TITLE_PREFIX
                .createQuery(Operators.BEGINNING, ImmutableList.of("#"));
        IndexQueryResult result = Futures.get(
                index.query(
                        new AttributeQuerySet(ImmutableList.of(query)),
                        ImmutableList.of(Publisher.BBC),
                        Selection.all(),
                        Optional.empty()),
                Exception.class
        );
        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().first().get(), is(item1.getId()));
    }

    @Test
    public void testSourceQuery() throws Exception {
        Content content = new Episode();
        content.setId(1);
        content.setPublisher(Publisher.METABROADCAST);

        indexAndRefresh(content);

        AttributeQuery<Publisher> query = Attributes.SOURCE
                .createQuery(Operators.EQUALS, ImmutableList.of(Publisher.METABROADCAST));

        AttributeQuerySet querySet = new AttributeQuerySet(ImmutableList.of(query));
        ListenableFuture<IndexQueryResult> result = index.query(querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(), Optional.empty());

        IndexQueryResult ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));

        query = Attributes.SOURCE
                .createQuery(Operators.EQUALS, ImmutableList.of(Publisher.BBC));

        querySet = new AttributeQuerySet(ImmutableList.of(query));
        result = index.query(querySet, ImmutableList.of(Publisher.METABROADCAST), Selection.all(),
                Optional.empty());

        ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.getIds().isEmpty(), is(true));

    }

    @Test
    public void testTopicQuery() throws Exception {
        Content content = new Episode();
        content.setId(1);
        content.setPublisher(Publisher.METABROADCAST);
        content.setTags(ImmutableList.of(new Tag(2L,
                1.0f,
                true,
                Tag.Relationship.ABOUT)));

        indexAndRefresh(content);

        AttributeQuery<Id> query = Attributes.TOPIC_ID.createQuery(Operators.EQUALS,
                ImmutableList.of(Id.valueOf(2)));

        AttributeQuerySet querySet = new AttributeQuerySet(ImmutableList.of(query));
        ListenableFuture<IndexQueryResult> result = index.query(
                querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(),
                Optional.empty());

        IndexQueryResult ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));
    }

    public void testQueryOrder() throws Exception {
        Content episode1 = episode(1);
        episode1.setTags(ImmutableList.of(new Tag(4L,
                1.0f,
                true,
                Tag.Relationship.ABOUT)));
        Content episode2 = episode(2);
        episode2.setTags(ImmutableList.of(new Tag(4L,
                1.5f,
                true,
                Tag.Relationship.ABOUT)));
        Content episode3 = episode(3);
        episode3.setTags(ImmutableList.of(new Tag(4L,
                2.0f,
                false,
                Tag.Relationship.ABOUT)));

        indexAndRefresh(episode1, episode2, episode3);

        AttributeQuery<Id> query = Attributes.TOPIC_ID.createQuery(Operators.EQUALS,
                ImmutableList.of(Id.valueOf(4)));

        AttributeQuerySet querySet = new AttributeQuerySet(ImmutableList.of(query));
        ListenableFuture<IndexQueryResult> result = index.query(querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(), Optional.empty());

        IndexQueryResult ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.getIds().get(0), is(Id.valueOf(2)));
        assertThat(ids.getIds().get(1), is(Id.valueOf(1)));
        assertThat(ids.getIds().get(2), is(Id.valueOf(3)));
    }

    private Content episode(int id) {
        Content content = new Episode();
        content.setId(id);
        content.setPublisher(Publisher.METABROADCAST);
        return content;
    }

    @Test
    public void testTopicWeightingQuery() throws Exception {
        Content content = new Episode();
        content.setId(1);
        content.setPublisher(Publisher.METABROADCAST);
        content.setTags(ImmutableList.of(new Tag(2L,
                1.0f,
                true,
                Tag.Relationship.ABOUT)));

        indexAndRefresh(content);

        AttributeQuery<Float> query = Attributes.TAG_WEIGHTING.createQuery(
                Operators.EQUALS, ImmutableList.of(1.0f));

        IndexQueryResult ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)),
                ImmutableList.of(Publisher.METABROADCAST), Selection.all(), Optional.empty())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));

        query = Attributes.TAG_WEIGHTING.createQuery(
                Operators.LESS_THAN, ImmutableList.of(0.5f));

        ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)),
                ImmutableList.of(Publisher.METABROADCAST), Selection.all(), Optional.empty())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.getIds().first().isPresent(), is(false));

        query = Attributes.TAG_WEIGHTING.createQuery(
                Operators.GREATER_THAN, ImmutableList.of(0.5f));

        ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)),
                ImmutableList.of(Publisher.METABROADCAST), Selection.all(), Optional.empty())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.getIds().first().get(), is(Id.valueOf(1)));

    }

    private void indexAndRefresh(Content... contents) throws IndexException {
        for (Content content : contents) {
            index.index(content);
        }
        ElasticSearchHelper.refresh(esClient);
    }

    @Test
    public void testUnindexingOfContentThatIsNoLongerPublished()
            throws IndexException, ExecutionException, InterruptedException {
        Item item = complexItem().withId(20l).build();
        item.setPublisher(Publisher.METABROADCAST);
        item.setActivelyPublished(true);
        indexAndRefresh(item);

        AttributeQuerySet querySet = new AttributeQuerySet(
                ImmutableList.of(
                        Attributes.ID.createQuery(Operators.EQUALS,
                                ImmutableList.of(Id.valueOf(20l)))
                )
        );
        IndexQueryResult resultWithItemPresent = index.query(querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(),
                Optional.empty())
                .get();
        assertThat(resultWithItemPresent.getIds().first().get(), is(Id.valueOf(20l)));

        item.setActivelyPublished(false);
        indexAndRefresh(item);

        IndexQueryResult resultWithItemAbsent = index.query(querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(),
                Optional.empty())
                .get();
        assertThat(resultWithItemAbsent.getIds().first(), is(com.google.common.base.Optional.absent()));
    }
}
