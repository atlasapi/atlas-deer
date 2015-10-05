package org.atlasapi.content;

import static org.atlasapi.util.ElasticSearchHelper.refresh;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.NoOpSecondaryIndex;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.time.DateTimeZones;

public final class EsUnequivalentContentIndexingTest {
    
    private static final Node esClient = ElasticSearchHelper.testNode();
    private EsUnequivalentContentIndex contentIndexer;

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }
    
    @AfterClass
    public static void after() {
        esClient.close();
    }

    @Before
    public void setup() throws TimeoutException {
        ElasticSearchHelper.refresh(esClient.client());
        contentIndexer = new EsUnequivalentContentIndex(esClient.client(), EsSchema.CONTENT_INDEX, new NoOpContentResolver(), mock(ChannelGroupResolver.class), new NoOpSecondaryIndex(), 6000);
        contentIndexer.startAsync().awaitRunning(25, TimeUnit.SECONDS);
    }
    
    @After
    public void teardown() throws Exception {
        ElasticSearchHelper.clearIndices(esClient.client());
    }

    @Test
    public void testScheduleQueries() throws Exception {
        DateTime broadcastStart = new DateTime(1980, 10, 10, 10, 10, 10, 10, DateTimeZones.UTC);
        Broadcast broadcast = new Broadcast(Id.valueOf(1), broadcastStart, broadcastStart.plusHours(1));
        Item item = new Item("uri", "curie", Publisher.METABROADCAST);
        item.setId(Id.valueOf(1));
        item.addBroadcast(broadcast);
        
        contentIndexer.index(item);
        
        refresh(esClient.client());

        ListenableActionFuture<SearchResponse> result1 = esClient.client()
            .prepareSearch(EsSchema.CONTENT_INDEX)
            .setQuery(QueryBuilders.nestedQuery("broadcasts", 
                    QueryBuilders.termQuery("channel", 1L)
            )).execute();
        SearchHits hits1 = result1.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(1, hits1.totalHits());

        ListenableActionFuture<SearchResponse> result2 = esClient.client()
            .prepareSearch(EsSchema.CONTENT_INDEX)
            .setQuery(QueryBuilders.nestedQuery("broadcasts",
                QueryBuilders.rangeQuery("transmissionTime")
                    .from(broadcastStart.minusDays(1).toDate())
            )).execute();
        SearchHits hits2 = result2.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(1, hits2.totalHits());
        
        ListenableActionFuture<SearchResponse> result3 = esClient.client()
            .prepareSearch(EsSchema.CONTENT_INDEX)
            .setQuery(QueryBuilders.nestedQuery("broadcasts", 
                QueryBuilders.rangeQuery("transmissionTime")
                    .from(new DateTime(DateTimeZones.UTC).toDate())
            )).execute();
        SearchHits hits3 = result3.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(0, hits3.totalHits());
    }
    
    @Test
    public void testTopicFacets() throws Exception {
        DateTime now = new DateTime(DateTimeZones.UTC);
        Broadcast broadcast1 = new Broadcast(Id.valueOf(1), now, now.plusHours(1));

        Broadcast broadcast2 = new Broadcast(Id.valueOf(1), now.plusHours(2), now.plusHours(3));

        Tag topic1 = new Tag(Id.valueOf(1), 1.0f, Boolean.TRUE, Tag.Relationship.ABOUT);
        Tag topic2 = new Tag(Id.valueOf(2), 1.0f, Boolean.TRUE, Tag.Relationship.ABOUT);

        Item item1 = new Item("uri1", "curie1", Publisher.METABROADCAST);
        item1.addBroadcast(broadcast1);
        item1.setId(Id.valueOf(1));
        item1.addTopicRef(topic1);
        item1.addTopicRef(topic2);
        Item item2 = new Item("uri2", "curie2", Publisher.METABROADCAST);
        item2.addBroadcast(broadcast1);
        item2.setId(Id.valueOf(2));
        item2.addTopicRef(topic1);
        Item item3 = new Item("uri3", "curie3", Publisher.METABROADCAST);
        item3.addBroadcast(broadcast2);
        item3.setId(Id.valueOf(3));
        item3.addTopicRef(topic2);
        
        contentIndexer.index(item1);
        contentIndexer.index(item2);
        contentIndexer.index(item3);
        
        refresh(esClient.client());
        
        ListenableActionFuture<SearchResponse> futureResult = esClient.client()
            .prepareSearch(EsSchema.CONTENT_INDEX)
            .setQuery(QueryBuilders.nestedQuery(EsContent.BROADCASTS,
                QueryBuilders.rangeQuery(EsBroadcast.TRANSMISSION_TIME)
                    .from(now.minusHours(1).toDate())
                    .to(now.plusHours(1).toDate())
            ))
            .addFacet(FacetBuilders.termsFacet("topicFacet")
                .nested(EsContent.TOPICS + "." + EsTopicMapping.TOPIC)
                .field(EsContent.TOPICS + "." + EsTopicMapping.TOPIC_ID))
            .execute();
        
        
        SearchResponse result = futureResult.actionGet(5, TimeUnit.SECONDS);
        Facets facets = result.getFacets();
        List<? extends Entry> terms = facets.facet(TermsFacet.class, "topicFacet").getEntries();

        assertEquals(2, terms.size());
        assertEquals("1", terms.get(0).getTerm().string());
        assertEquals(2, terms.get(0).getCount());
        assertEquals("2", terms.get(1).getTerm().string());
        assertEquals(1, terms.get(1).getCount());
    }

    @Test
    public void testIndexsEpisodeDataOntoSeries() throws Exception {
        DateTime now = DateTime.now();
        DateTime nowPlusOneDay = DateTime.now().plusDays(1);
        Series series = new Series(Id.valueOf(1l), Publisher.METABROADCAST);

        Episode episodeOne = new Episode(Id.valueOf(2l), Publisher.METABROADCAST);
        episodeOne.setSeries(series);

        Encoding encodingOne = new Encoding();
        Location locationOne = new Location();
        Policy policyOne = new Policy();
        policyOne.setAvailabilityStart(now);
        policyOne.setAvailabilityEnd(nowPlusOneDay);
        locationOne.setPolicy(policyOne);
        encodingOne.setAvailableAt(ImmutableSet.of(locationOne));

        episodeOne.setBroadcasts(ImmutableSet.of(new Broadcast(Id.valueOf(100l), now, nowPlusOneDay)));
        episodeOne.setManifestedAs(ImmutableSet.of(encodingOne));

        indexAndRefresh(series, episodeOne);

        GetResponse resp = esClient.client().get(new GetRequest("content", "container", "1")).get();
        Map<String, Object> src = resp.getSource();
        assertThat(((List) src.get(EsContent.BROADCASTS)).size(), is(1));
        assertThat(((List) src.get(EsContent.LOCATIONS)).size(), is(1));

        Episode episodeTwo = new Episode(Id.valueOf(2l), Publisher.METABROADCAST);
        episodeTwo.setSeries(series);

        Encoding encodingTwo = new Encoding();

        Location locationTwo = new Location();
        Location locationThree = new Location();
        Location locationFour = new Location();

        Policy policyTwo = new Policy();
        policyTwo.setAvailabilityStart(now);
        policyTwo.setAvailabilityEnd(nowPlusOneDay);

        Policy policyThree = new Policy();
        policyThree.setAvailabilityStart(now.plusMinutes(20));
        policyThree.setAvailabilityEnd(nowPlusOneDay);

        Policy policyFour = new Policy();
        policyFour.setAvailabilityStart(now.minusMinutes(20));
        policyFour.setAvailabilityEnd(nowPlusOneDay.plusDays(1));


        locationTwo.setPolicy(policyTwo);
        locationThree.setPolicy(policyThree);
        locationFour.setPolicy(policyFour);

        encodingTwo.setAvailableAt(ImmutableSet.of(locationTwo, locationThree, locationFour));

        episodeTwo.setBroadcasts(
                ImmutableSet.of(
                        new Broadcast(Id.valueOf(100l), now.minusDays(1), nowPlusOneDay),
                        new Broadcast(Id.valueOf(100l), now, nowPlusOneDay),
                        new Broadcast(Id.valueOf(100l), now.plusMinutes(20), nowPlusOneDay)
                )
        );

        episodeTwo.setManifestedAs(ImmutableSet.of(encodingTwo));

        indexAndRefresh(episodeTwo);

        GetResponse respTwo = esClient.client().get(new GetRequest("content", "container", "1")).get();
        Map<String, Object> srcTwo = respTwo.getSource();
        assertThat(((List)srcTwo.get(EsContent.BROADCASTS)).size(), is(3));
        assertThat(((List)srcTwo.get(EsContent.LOCATIONS)).size(), is(3));

        indexAndRefresh(series);
        GetResponse respThree = esClient.client().get(new GetRequest("content", "container", "1")).get();
        Map<String, Object> srcThree = respThree.getSource();
        assertThat(((List) srcThree.get(EsContent.BROADCASTS)).size(), is(3));
        assertThat(((List) srcThree.get(EsContent.LOCATIONS)).size(), is(3));
    }

    private void indexAndRefresh(Content... contents) throws IndexException {
        for (Content content : contents) {
            contentIndexer.index(content);
        }
        ElasticSearchHelper.refresh(esClient.client());
    }
}
