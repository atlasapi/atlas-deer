package org.atlasapi.topic;

import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.EsUnequivalentContentIndex;
import org.atlasapi.content.Item;
import org.atlasapi.content.Tag;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.NoOpSecondaryIndex;

import com.metabroadcast.common.query.Selection;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.atlasapi.util.ElasticSearchHelper.refresh;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsPopularTopicsIndexTest {

    private final Node esClient = ElasticSearchHelper.testNode();
    private final EsUnequivalentContentIndex index = new EsUnequivalentContentIndex(
            esClient.client(),
            EsSchema.CONTENT_INDEX,
            mock(ChannelGroupResolver.class),
            new NoOpSecondaryIndex(),
            1_000
    );

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }

    @Before
    public void setup() {
        index.startAsync().awaitRunning();
    }

    @After
    public void after() throws Exception {
        ElasticSearchHelper.clearIndices(esClient.client());
        esClient.close();
    }

    @Test
    public void testPopularTopics() throws Exception {
        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                new DateTime(),
                new DateTime().plusHours(1)
        );
        Broadcast broadcast2 = new Broadcast(
                Id.valueOf(1),
                new DateTime().plusHours(2),
                new DateTime().plusHours(3)
        );

        Tag topic1 = new Tag(Id.valueOf(1), 1.0f, Boolean.TRUE, Tag.Relationship.ABOUT);
        Tag topic2 = new Tag(Id.valueOf(2), 1.0f, Boolean.TRUE, Tag.Relationship.ABOUT);

        Item item1 = new Item("uri1", "curie1", Publisher.METABROADCAST);
        item1.addBroadcast(broadcast1);
        item1.setId(Id.valueOf(1));
        item1.addTopicRef(topic1);
        Item item2 = new Item("uri2", "curie2", Publisher.METABROADCAST);
        item2.addBroadcast(broadcast1);
        item2.setId(Id.valueOf(2));
        item2.addTopicRef(topic1);
        Item item3 = new Item("uri3", "curie3", Publisher.METABROADCAST);
        item3.addBroadcast(broadcast1);
        item3.setId(Id.valueOf(3));
        item3.addTopicRef(topic1);
        item3.addTopicRef(topic2);
        Item item4 = new Item("uri4", "curie4", Publisher.METABROADCAST);
        item4.addBroadcast(broadcast2);
        item4.setId(Id.valueOf(4));
        item4.addTopicRef(topic2);
        Item item5 = new Item("uri5", "curie5", Publisher.METABROADCAST);
        item5.addBroadcast(broadcast2);
        item5.setId(Id.valueOf(5));
        item5.addTopicRef(topic2);

        index.index(item1);
        index.index(item2);
        index.index(item3);
        index.index(item4);
        index.index(item5);
        refresh(esClient.client());

        TopicResolver resolver = mock(TopicResolver.class);
        when(resolver.resolveIds(argThat(hasItems(Id.valueOf(1), Id.valueOf(2)))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(new Topic(Id.valueOf(
                        1)), new Topic(Id.valueOf(2))))));

        PopularTopicIndex searcher = new EsPopularTopicIndex(esClient.client());

        Interval interval = new Interval(new DateTime().minusHours(1), new DateTime().plusHours(1));

        FluentIterable<Id> topicIds = queryIndex(
                searcher,
                interval,
                Selection.offsetBy(0).withLimit(Integer.MAX_VALUE)
        );

        assertEquals(2, topicIds.size());
        assertEquals(1l, topicIds.get(0).longValue());
        assertEquals(2l, topicIds.get(1).longValue());

        topicIds = queryIndex(searcher, interval, Selection.offsetBy(0).withLimit(1));
        assertEquals(1, topicIds.size());
        assertEquals(1, topicIds.get(0).longValue());

        topicIds = queryIndex(searcher, interval, Selection.offsetBy(1).withLimit(1));
        assertEquals(1, topicIds.size());
        assertEquals(2, topicIds.get(0).longValue());
    }

    private FluentIterable<Id> queryIndex(PopularTopicIndex searcher, Interval interval,
            Selection selection) {
        ListenableFuture<FluentIterable<Id>> futureIds = searcher.popularTopics(
                interval,
                selection
        );
        FluentIterable<Id> topicIds = Futures.getUnchecked(futureIds);
        return topicIds;
    }

}
