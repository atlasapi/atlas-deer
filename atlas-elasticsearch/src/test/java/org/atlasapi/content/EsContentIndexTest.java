package org.atlasapi.content;

import static org.atlasapi.content.ComplexItemTestDataBuilder.complexItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Currency;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.EsSchema;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.query.Selection;

public class EsContentIndexTest {

    private static final Node esClient = ElasticSearchHelper.testNode();

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
        index = new EsContentIndex(esClient, EsSchema.CONTENT_INDEX, 60000, new NoOpContentResolver());
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
        FluentIterable<Id> result = Futures.get(
                index.query(
                        querySet,
                        ImmutableList.of(Publisher.BBC),
                        Selection.all()
                ),
                Exception.class
        );
        assertThat(result.first().get(), is(Id.valueOf(10l)));
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
        FluentIterable<Id> result = Futures.get(
                index.query(
                        new AttributeQuerySet(ImmutableList.of(query)),
                        ImmutableList.of(Publisher.BBC),
                        Selection.all()
                ),
                Exception.class
        );
        assertThat(result.size(), is(1));
        assertThat(result.first().get(), is(item1.getId()));
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
        ListenableFuture<FluentIterable<Id>> result = index.query(querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all());

        FluentIterable<Id> ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.first().get(), is(Id.valueOf(1)));

        query = Attributes.SOURCE
                .createQuery(Operators.EQUALS, ImmutableList.of(Publisher.BBC));

        querySet = new AttributeQuerySet(ImmutableList.of(query));
        result = index.query(querySet, ImmutableList.of(Publisher.METABROADCAST), Selection.all());

        ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.isEmpty(), is(true));

    }

    @Test
    public void testTopicQuery() throws Exception {
        Content content = new Episode();
        content.setId(1);
        content.setPublisher(Publisher.METABROADCAST);
        content.setTopicRefs(ImmutableList.of(new TopicRef(2L,
                1.0f,
                true,
                TopicRef.Relationship.ABOUT)));

        indexAndRefresh(content);

        AttributeQuery<Id> query = Attributes.TOPIC_ID.createQuery(Operators.EQUALS,
                ImmutableList.of(Id.valueOf(2)));

        AttributeQuerySet querySet = new AttributeQuerySet(ImmutableList.of(query));
        ListenableFuture<FluentIterable<Id>> result = index.query(
                querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all()
        );

        FluentIterable<Id> ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.first().get(), is(Id.valueOf(1)));
    }

    @Test
    public void testQueryOrder() throws Exception {
        Content episode1 = episode(1);
        episode1.setTopicRefs(ImmutableList.of(new TopicRef(4L,
                1.0f,
                true,
                TopicRef.Relationship.ABOUT)));
        Content episode2 = episode(2);
        episode2.setTopicRefs(ImmutableList.of(new TopicRef(4L,
                1.5f,
                true,
                TopicRef.Relationship.ABOUT)));
        Content episode3 = episode(3);
        episode3.setTopicRefs(ImmutableList.of(new TopicRef(4L,
                2.0f,
                false,
                TopicRef.Relationship.ABOUT)));

        indexAndRefresh(episode1, episode2, episode3);

        AttributeQuery<Id> query = Attributes.TOPIC_ID.createQuery(Operators.EQUALS,
                ImmutableList.of(Id.valueOf(4)));

        AttributeQuerySet querySet = new AttributeQuerySet(ImmutableList.of(query));
        ListenableFuture<FluentIterable<Id>> result = index.query(querySet,
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all());

        FluentIterable<Id> ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.get(0), is(Id.valueOf(2)));
        assertThat(ids.get(1), is(Id.valueOf(1)));
        assertThat(ids.get(2), is(Id.valueOf(3)));
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
        content.setTopicRefs(ImmutableList.of(new TopicRef(2L,
                1.0f,
                true,
                TopicRef.Relationship.ABOUT)));

        indexAndRefresh(content);

        AttributeQuery<Float> query = Attributes.TOPIC_WEIGHTING.createQuery(
                Operators.EQUALS, ImmutableList.of(1.0f));

        FluentIterable<Id> ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)),
                ImmutableList.of(Publisher.METABROADCAST), Selection.all())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.first().get(), is(Id.valueOf(1)));

        query = Attributes.TOPIC_WEIGHTING.createQuery(
                Operators.LESS_THAN, ImmutableList.of(0.5f));

        ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)),
                ImmutableList.of(Publisher.METABROADCAST), Selection.all())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.first().isPresent(), is(false));

        query = Attributes.TOPIC_WEIGHTING.createQuery(
                Operators.GREATER_THAN, ImmutableList.of(0.5f));

        ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)),
                ImmutableList.of(Publisher.METABROADCAST), Selection.all())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.first().get(), is(Id.valueOf(1)));

    }

    private void indexAndRefresh(Content... contents) throws IndexException {
        for (Content content : contents) {
            index.index(content);
        }
        ElasticSearchHelper.refresh(esClient);
    }
}
