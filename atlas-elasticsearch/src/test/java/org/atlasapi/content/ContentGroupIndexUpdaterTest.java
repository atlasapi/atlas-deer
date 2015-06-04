package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.EsSchema;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ContentGroupIndexUpdaterTest {


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
        index = new EsContentIndex(esClient, EsSchema.CONTENT_INDEX, 60000, contentResolver());
        index.startAsync().awaitRunning();
    }

    @After
    public void teardown() throws Exception {
        ElasticSearchHelper.clearIndices(esClient);
        ElasticSearchHelper.refresh(esClient);
    }

    private ContentResolver contentResolver() {
        ContentResolver resolver = mock(ContentResolver.class);

        SettableFuture<Resolved<Content>> presentFuture = SettableFuture.create();
        presentFuture.set(Resolved.valueOf(ImmutableList.of(item())));

        SettableFuture<Resolved<Content>> emptyFuture = SettableFuture.create();
        emptyFuture.set(Resolved.<Content>empty());

        when(resolver.resolveIds(ImmutableSet.of())).thenReturn(emptyFuture);
        when(resolver.resolveIds(ImmutableSet.of(Id.valueOf("10")))).thenReturn(presentFuture);

        return resolver;
    }

    @Test
    public void testCorrectContentGroupIdsAreSetOnContentIndexEntry() throws Exception {

        Item item = item();
        indexAndRefresh(item);
        GetResponse resp = new GetRequestBuilder(esClient.client(), "content").setId("10").get();
        assertThat(resp.getSource().get(EsContent.TITLE), is(item.getTitle()));


        ContentGroup cg1 = new ContentGroup(
                ContentGroup.Type.ORGANISATION,
                "http://test.com",
                null,
                Publisher.METABROADCAST
        );
        cg1.setId(20l);

        ContentGroup cg2 = new ContentGroup(
                ContentGroup.Type.ORGANISATION,
                "http://test2.com",
                null,
                Publisher.METABROADCAST
        );
        cg2.setId(30l);

        cg1.setContents(
                ImmutableList.of(
                        new ItemRef(item.getId(), item.getSource(), "sortkey", DateTime.now())
                )
        );

        cg2.setContents(
                ImmutableList.of(
                        new ItemRef(item.getId(), item.getSource(), "sortkey", DateTime.now())
                )
        );

        index.index(cg1);
        index.index(cg2);

        ElasticSearchHelper.refresh(esClient);

        GetResponse resp2 = new GetRequestBuilder(esClient.client(), "content")
                .setFields(EsContent.CONTENT_GROUPS)
                .setId("10")
                .execute()
                .get();
        assertThat(resp2.getField(EsContent.CONTENT_GROUPS).getValues().get(0), is(30l));
        assertThat(resp2.getField(EsContent.CONTENT_GROUPS).getValues().get(1), is(20l));

        cg1.setContents(ImmutableList.of());

        index.index(cg1);
        ElasticSearchHelper.refresh(esClient);

        GetResponse resp3 = new GetRequestBuilder(esClient.client(), "content")
                .setFields(EsContent.CONTENT_GROUPS)
                .setId("10")
                .execute()
                .get();

        assertThat(resp3.getField(EsContent.CONTENT_GROUPS).getValues().size(), is(1));
        assertThat(resp3.getField(EsContent.CONTENT_GROUPS).getValues().size(), is(1));
    }

    private Item item() {
        Item item = new Item(Id.valueOf(10l), Publisher.METABROADCAST);
        item.setTitle("TestTitle!");
        return item;
    }

    private void indexAndRefresh(Content... contents) throws IndexException {
        for (Content content : contents) {
            index.index(content);
        }
        ElasticSearchHelper.refresh(esClient);
    }
}