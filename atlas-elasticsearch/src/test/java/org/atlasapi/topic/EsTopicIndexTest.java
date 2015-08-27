package org.atlasapi.topic;

import static org.atlasapi.topic.EsTopic.ALIASES;
import static org.atlasapi.topic.EsTopic.DESCRIPTION;
import static org.atlasapi.topic.EsTopic.ID;
import static org.atlasapi.topic.EsTopic.SOURCE;
import static org.atlasapi.topic.EsTopic.TITLE;
import static org.atlasapi.util.ElasticSearchHelper.refresh;
import static org.atlasapi.util.EsAlias.VALUE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;

public class EsTopicIndexTest {

    private static final Node esClient = ElasticSearchHelper.testNode();
    private final String indexName = "topics";
    
    private EsTopicIndex index;
    
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
    public void setup() throws Exception {
        index = new EsTopicIndex(esClient.client(), indexName, 60, TimeUnit.SECONDS);
        index.startAsync().awaitRunning();
    }
    
    @After
    public void tearDown() throws Exception {
        ElasticSearchHelper.clearIndices(esClient.client());
    }
    
    @Test
    public void testIndexesAndRetrievesTopic() throws Exception {
        Topic topic = topic(1234, Publisher.DBPEDIA, "title", "description", new Alias("an", "Alias"));
        
        index.index(topic);
        
        refresh(esClient.client());
        
        GetResponse got = esClient.client().get(
            Requests.getRequest(indexName).id("1234")
        ).actionGet(10000);
        
        Map<String, Object> source = got.getSource();
        assertEquals(1234, source.get(ID));
        assertEquals("dbpedia.org", source.get(SOURCE));
        assertEquals("title", source.get(TITLE));
        assertEquals("description", source.get(DESCRIPTION));
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> aliases = (List<Map<String, Object>>) source.get(ALIASES);
        assertEquals("Alias", Iterables.getOnlyElement(aliases).get(VALUE));
    }

    @Test
    public void testIdOnlyQuery() throws Exception {
        Topic topic = topic(1234, Publisher.DBPEDIA, "title", "description", new Alias("an", "Alias"));
        
        index.index(topic);
        refresh(esClient.client());
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(topic.getId()))
        ));
        IndexQueryResult result = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), Selection.ALL));
        assertThat(Iterables.getOnlyElement(result.getIds()),is(topic.getId()));
    }

    @Test
    public void testConjunctiveQuery() throws Exception {
        Topic topic = topic(1234, Publisher.DBPEDIA, "title", "description", new Alias("an", "Alias"));
        
        index.index(topic);
        refresh(esClient.client());
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(topic.getId())),
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("Alias"))
        ));
        IndexQueryResult result = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), Selection.ALL));
        assertThat(Iterables.getOnlyElement(result.getIds()),is(topic.getId()));
    }

    @Test
    public void testPublisherFilter() throws Exception {
        Topic topic = topic(1234, Publisher.METABROADCAST, "title", "description", new Alias("an", "Alias"));
        
        index.index(topic);
        refresh(esClient.client());
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(topic.getId()))
        ));
        IndexQueryResult result = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), Selection.ALL));
        assertThat(Iterables.isEmpty(result.getIds()), is(true));
    }

    @Test
    public void testSelection() throws Exception {
        Topic topic1 = topic(1234, Publisher.DBPEDIA, "title1", "description1", new Alias("an", "Alias"));
        Topic topic2 = topic(1235, Publisher.DBPEDIA, "title2", "description2", new Alias("an", "Alias"));
        
        index.index(topic2);
        index.index(topic1);
        refresh(esClient.client());
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("Alias"))
        ));
        IndexQueryResult result = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            Selection.ALL));
        assertThat(Iterables.size(result.getIds()), is(2));

        result = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            new Selection(0,1)));
        assertThat(Iterables.getOnlyElement(result.getIds()), is(topic1.getId()));

        result = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            new Selection(1,1)));
        assertThat(Iterables.getOnlyElement(result.getIds()), is(topic2.getId()));

        result = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            new Selection(1,5)));
        assertThat(Iterables.getOnlyElement(result.getIds()), is(topic2.getId()));
    }

    private Topic topic(int id, Publisher source, String title, String description, Alias... aliases) {
        Topic topic = new Topic();
        topic.setId(id);
        topic.setPublisher(source);
        topic.addAliases(ImmutableList.copyOf(aliases));
        topic.setTitle(title);
        topic.setDescription(description);
        return topic;
    }
}
