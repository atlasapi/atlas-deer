package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PseudoEquivalentEsContentIndexTest {

    private SecondaryIndex equivIndex = mock(SecondaryIndex.class);
    private ContentIndex contentIndex;
    private Client esNode = ElasticSearchHelper.testNode().client();
    @Before
    public void setUp() {
        EsContentIndex delegate = new EsContentIndex(
                esNode,
                EsSchema.CONTENT_INDEX,
                60000,
                new NoOpContentResolver(),
                mock(ChannelGroupResolver.class)
        );
        delegate.startAsync().awaitRunning();
        contentIndex = new PseudoEquivalentEsContentIndex(delegate, equivIndex);
    }

    @Test
    public void testQuery() throws Exception {
        Item item1 = new Item(Id.valueOf(1l), Publisher.METABROADCAST);
        Item item2 = new Item(Id.valueOf(2l), Publisher.METABROADCAST);
        Item item3 = new Item(Id.valueOf(3l), Publisher.METABROADCAST);

        when(equivIndex.lookup(any())).thenReturn(
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

    private void indexAndRefresh(Content... contents) throws IndexException {
        for (Content content : contents) {
            contentIndex.index(content);
        }
        ElasticSearchHelper.refresh(esNode);
    }
}