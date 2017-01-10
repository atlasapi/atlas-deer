package org.atlasapi.content;

import java.util.Arrays;

import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResults;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.NoOpSecondaryIndex;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.atlasapi.util.ElasticSearchHelper.refresh;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class EsContentTitleSearcherTest {

    private final Node esClient = ElasticSearchHelper.testNode();

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }

    @After
    public void after() throws Exception {
        ElasticSearchHelper.clearIndices(esClient.client());
        esClient.close();
    }

    @Test
    public void testSearch() throws Exception {
        Broadcast broadcast1 = new Broadcast(
                Id.valueOf(1),
                new DateTime(),
                new DateTime().plusHours(1)
        );
        Broadcast broadcast2 = new Broadcast(
                Id.valueOf(1),
                new DateTime().plusHours(3),
                new DateTime().plusHours(4)
        );

        Item item1 = new Item("uri1", "curie1", Publisher.METABROADCAST);
        item1.setTitle("title1");
        item1.setId(Id.valueOf(1));
        item1.addBroadcast(broadcast1);
        item1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        Item item2 = new Item("uri2", "curie2", Publisher.METABROADCAST);
        item2.setTitle("title2");
        item2.setId(Id.valueOf(2));
        item2.addBroadcast(broadcast1);
        item2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        Item item3 = new Item("uri3", "curie3", Publisher.METABROADCAST);
        item3.setTitle("pippo");
        item3.setId(Id.valueOf(3));
        item3.addBroadcast(broadcast2);
        item3.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        Item item4 = new Item("uri4", "curie4", Publisher.METABROADCAST);
        item4.setTitle("title4");
        item4.setId(Id.valueOf(4));
        item4.addBroadcast(broadcast2);
        item4.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        Brand brand1 = new Brand("buri1", "buri1", Publisher.METABROADCAST);
        brand1.setTitle("title");
        brand1.setId(Id.valueOf(5));
        brand1.setItemRefs(Arrays.asList(item1.toRef(), item2.toRef()));
        Brand brand2 = new Brand("buri2", "buri2", Publisher.METABROADCAST);
        brand2.setTitle("b");
        brand2.setId(Id.valueOf(6));
        brand2.setItemRefs(Arrays.asList(item3.toRef()));

        item1.setContainerRef(brand1.toRef());
        item2.setContainerRef(brand1.toRef());
        item3.setContainerRef(brand2.toRef());

        EsUnequivalentContentIndex contentIndex = new EsUnequivalentContentIndex(
                esClient.client(),
                EsSchema.CONTENT_INDEX,
                mock(ChannelGroupResolver.class),
                new NoOpSecondaryIndex(),
                60000
        );
        contentIndex.startAsync().awaitRunning();

        EsContentTitleSearcher contentSearcher = new EsContentTitleSearcher(esClient.client());

        contentIndex.index(brand1);
        contentIndex.index(brand2);
        contentIndex.index(item1);
        contentIndex.index(item2);
        contentIndex.index(item3);
        contentIndex.index(item4);

        refresh(esClient.client());

        SearchQuery query = SearchQuery.builder("title")
                .withSelection(Selection.offsetBy(0))
                .withSpecializations(ImmutableSet.<Specialization>of())
                .withPublishers(ImmutableSet.<Publisher>of())
                .withTitleWeighting(1)
                .withBroadcastWeighting(0)
                .withCatchupWeighting(0)
                .withPriorityChannelWeighting(0)
                .build();

        ListenableFuture<SearchResults> future = contentSearcher.search(query);

        SearchResults results = future.get();
        assertEquals(2, results.getIds().size());
        assertEquals(brand1.getId(), results.getIds().get(0));
        assertEquals(item4.getId(), results.getIds().get(1));
    }
}
