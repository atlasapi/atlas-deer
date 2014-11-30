package org.atlasapi.content;

import static org.atlasapi.util.ElasticSearchHelper.refresh;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.EsSchema;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.elasticsearch.action.ListenableActionFuture;
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

import com.metabroadcast.common.time.DateTimeZones;

public final class EsContentIndexingTest {
    
    private static final Node esClient = ElasticSearchHelper.testNode();
    private EsContentIndex contentIndexer;

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
        ElasticSearchHelper.refresh(esClient);
        contentIndexer = new EsContentIndex(esClient, EsSchema.CONTENT_INDEX, 60000);
        contentIndexer.startAsync().awaitRunning(25, TimeUnit.SECONDS);
    }
    
    @After
    public void teardown() throws Exception {
        ElasticSearchHelper.clearIndices(esClient);
    }

    @Test
    public void testScheduleQueries() throws Exception {
        DateTime broadcastStart = new DateTime(1980, 10, 10, 10, 10, 10, 10, DateTimeZones.UTC);
        Broadcast broadcast = new Broadcast(Id.valueOf(1), broadcastStart, broadcastStart.plusHours(1));
        Item item = new Item("uri", "curie", Publisher.METABROADCAST);
        item.setId(Id.valueOf(1));
        item.addBroadcast(broadcast);
        
        contentIndexer.index(item);
        
        refresh(esClient);

        ListenableActionFuture<SearchResponse> result1 = esClient.client()
            .prepareSearch(EsSchema.CONTENT_INDEX)
            .setQuery(QueryBuilders.nestedQuery("broadcasts", 
                    QueryBuilders.fieldQuery("channel", 1L)
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

        TopicRef topic1 = new TopicRef(Id.valueOf(1), 1.0f, Boolean.TRUE, TopicRef.Relationship.ABOUT);
        TopicRef topic2 = new TopicRef(Id.valueOf(2), 1.0f, Boolean.TRUE, TopicRef.Relationship.ABOUT);

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
        
        refresh(esClient);
        
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
        
        
        SearchResponse result = futureResult.actionGet(1, TimeUnit.SECONDS);
        Facets facets = result.getFacets();
        List<? extends Entry> terms = facets.facet(TermsFacet.class, "topicFacet").getEntries();

        assertEquals(2, terms.size());
        assertEquals("1", terms.get(0).getTerm().string());
        assertEquals(2, terms.get(0).getCount());
        assertEquals("2", terms.get(1).getTerm().string());
        assertEquals(1, terms.get(1).getCount());
    }
}
