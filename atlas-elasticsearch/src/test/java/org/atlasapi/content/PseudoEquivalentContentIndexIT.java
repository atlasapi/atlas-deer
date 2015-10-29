package org.atlasapi.content;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.CassandraSecondaryIndex;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;

public class PseudoEquivalentContentIndexIT {

    private SecondaryIndex equivIndex = mock(CassandraSecondaryIndex.class);
    private ContentIndex contentIndex;
    private Client esNode = ElasticSearchHelper.testNode().client();

    @Before
    public void setUp() {
        EsUnequivalentContentIndex delegate = new EsUnequivalentContentIndex(
                esNode,
                EsSchema.CONTENT_INDEX,
                new NoOpContentResolver(),
                mock(ChannelGroupResolver.class),
                equivIndex,
                60000
        );
        delegate.startAsync().awaitRunning();
        contentIndex = new PseudoEquivalentContentIndex(delegate);
    }

    @Test
    public void testQuery() throws Exception {
        Item item1 = new Item(Id.valueOf(1l), Publisher.METABROADCAST);
        Item item2 = new Item(Id.valueOf(2l), Publisher.METABROADCAST);
        Item item3 = new Item(Id.valueOf(3l), Publisher.METABROADCAST);

        when(equivIndex.lookup(anyList()))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(1l, 2l)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(2l, 2l)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(3l, 3l)))
                .thenReturn(
                        Futures.immediateFuture(
                            ImmutableMap.of(
                                    1l, 2l,
                                    2l, 2l,
                                    3l, 3l
                            )
                )
        );


        indexAndRefresh(item1, item2, item3);

        ListenableFuture<IndexQueryResult> result = contentIndex.query(
                new AttributeQuerySet(ImmutableList.of()),
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all(),
                Optional.empty()
        );

        ImmutableList<Id> ids = ImmutableList.copyOf(Futures.get(result, IOException.class).getIds());

        assertThat(ids, is(ImmutableList.of(Id.valueOf(2l), Id.valueOf(3l))));
    }

    @Test
    public void testRemovesNonActivelyPublishedContent() throws IndexException, ExecutionException, InterruptedException {
        when(equivIndex.lookup(anyList()))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(10l, 10l)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(20l, 20l)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(3l, 3l)))
                .thenReturn(
                        Futures.immediateFuture(
                                ImmutableMap.of(
                                        1l, 2l,
                                        2l, 2l,
                                        3l, 3l
                                )
                        )
                );

        Item item = new Item(Id.valueOf(10l), Publisher.METABROADCAST);
        indexAndRefresh(item);
        item.setActivelyPublished(false);
        indexAndRefresh(item);
        GetResponse resp = esNode.prepareGet().setIndex("content").setId("10").execute().get();
        assertThat(resp.isExists(), is(false));

        Episode episode = new Episode(Id.valueOf(20l), Publisher.METABROADCAST);
        episode.setContainerRef(new BrandRef(Id.valueOf(30l), Publisher.METABROADCAST));
        indexAndRefresh(episode);
        episode.setActivelyPublished(false);
        indexAndRefresh(episode);
        GetResponse resp2 = esNode.prepareGet().setIndex("content").setId("20").execute().get();
        assertThat(resp2.isExists(), is(false));
    }

    private void indexAndRefresh(Content... contents) throws IndexException {
        for (Content content : contents) {
            contentIndex.index(content);
        }
        ElasticSearchHelper.refresh(esNode);
    }
}