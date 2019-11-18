package org.atlasapi.content;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.CassandraSecondaryIndex;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.SecondaryIndex;

import com.metabroadcast.common.query.Selection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PseudoEquivalentContentIndexIT {

    private SecondaryIndex equivIndex = mock(CassandraSecondaryIndex.class);
    private ContentIndex contentIndex;
    private Client esNode = ElasticSearchHelper.testNode().client();

    @Before
    public void setUp() {
        EsUnequivalentContentIndex delegate = EsUnequivalentContentIndex.create(
                esNode,
                EsSchema.CONTENT_INDEX,
                mock(ChannelGroupResolver.class),
                equivIndex,
                60000
        );
        delegate.startAsync().awaitRunning();
        contentIndex = PseudoEquivalentContentIndex.create(delegate);
    }

    @Test
    public void testQuery() throws Exception {
        Item item1 = new Item(Id.valueOf(1L), Publisher.BBC);
        Item item2 = new Item(Id.valueOf(2L), Publisher.METABROADCAST);
        Item item3 = new Item(Id.valueOf(3L), Publisher.METABROADCAST);

        when(equivIndex.lookup(anyList()))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(1L, 2L)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(2L, 2L)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(3L, 3L)))
                .thenReturn(
                        Futures.immediateFuture(
                                ImmutableMap.of(
                                        1L, 2L,
                                        2L, 2L,
                                        3L, 3L
                                )
                        )
                );

        indexAndRefresh(item1, item2, item3);

        ListenableFuture<IndexQueryResult> result = contentIndex.query(
                AttributeQuerySet.create(ImmutableList.of()),
                ImmutableList.of(Publisher.METABROADCAST),
                Selection.all()
        );

        ImmutableList<Id> ids = ImmutableList.copyOf(Futures.get(result, IOException.class)
                .getIds());

        assertThat(ids, is(ImmutableList.of(Id.valueOf(2L), Id.valueOf(3L))));
    }

    @Ignore
    @Test
    public void testRemovesNonActivelyPublishedContent()
            throws IndexException, ExecutionException, InterruptedException {
        when(equivIndex.lookup(anyList()))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(10L, 10L)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(20L, 20L)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(3L, 3L)))
                .thenReturn(
                        Futures.immediateFuture(
                                ImmutableMap.of(
                                        1L, 2L,
                                        2L, 2L,
                                        3L, 3L
                                )
                        )
                );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        indexAndRefresh(item);
        item.setActivelyPublished(false);
        indexAndRefresh(item);
        GetResponse resp = esNode.prepareGet().setIndex("content").setId("10").execute().get();
        assertThat(resp.isExists(), is(false));

        Episode episode = new Episode(Id.valueOf(20L), Publisher.METABROADCAST);
        episode.setContainerRef(new BrandRef(Id.valueOf(30L), Publisher.METABROADCAST));
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
