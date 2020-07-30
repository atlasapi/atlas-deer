package org.atlasapi.elasticsearch.content;

import java.util.List;

import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PseudoEquivalentContentIndexTest {

    private final List<AttributeQuery<?>> query = Lists.newArrayList();

    private @Mock EsUnequivalentContentSearcher esUnequivalentContentSearcher;
    private PseudoEquivalentContentSearcher pseudoEquivalentContentSearcher;

    @Before
    public void setUp() throws Exception {
        pseudoEquivalentContentSearcher = PseudoEquivalentContentSearcher.create(
                esUnequivalentContentSearcher);
    }

    @Test
    public void testQuery() throws Exception {
        DelegateIndexQueryResult delegateResult = DelegateIndexQueryResult.builder(10L)
                .add(Id.valueOf(0L), Id.valueOf(10L), Publisher.METABROADCAST)
                .add(Id.valueOf(1L), Id.valueOf(10L), Publisher.BBC)
                .add(Id.valueOf(2L), Id.valueOf(11L), Publisher.METABROADCAST)
                .build();

        List<Publisher> publishers = Lists.newArrayList(
                Publisher.METABROADCAST,
                Publisher.BBC
        );

        setupMocks(delegateResult, publishers);

        IndexQueryResult queryResult = pseudoEquivalentContentSearcher.query(
                query, publishers, Selection.ALL
        ).get();

        assertThat(queryResult.getTotalCount(), is(10L));

        assertThat(queryResult.getIds().size(), is(2));
        assertThat(queryResult.getIds(), containsInAnyOrder(Id.valueOf(0L), Id.valueOf(2L)));
    }

    @Test
    public void testQueryPagination() throws Exception {
        DelegateIndexQueryResult delegateResult = DelegateIndexQueryResult.builder(100L)
                .add(Id.valueOf(0L), Id.valueOf(10L), Publisher.METABROADCAST)
                .add(Id.valueOf(1L), Id.valueOf(11L), Publisher.METABROADCAST)
                .add(Id.valueOf(2L), Id.valueOf(12L), Publisher.METABROADCAST)
                .add(Id.valueOf(3L), Id.valueOf(13L), Publisher.METABROADCAST)
                .add(Id.valueOf(4L), Id.valueOf(14L), Publisher.METABROADCAST)
                .add(Id.valueOf(5L), Id.valueOf(15L), Publisher.METABROADCAST)
                .build();

        List<Publisher> publishers = Lists.newArrayList(Publisher.METABROADCAST);

        setupMocks(delegateResult, publishers);

        IndexQueryResult queryResult = pseudoEquivalentContentSearcher.query(
                query, publishers, new Selection(2, 2)
        ).get();

        assertThat(queryResult.getIds().size(), is(2));
        assertThat(queryResult.getIds(), containsInAnyOrder(Id.valueOf(2L), Id.valueOf(3L)));
    }

    @Test
    public void testQueryReturnsIdFromHighestPrecedencePublisher() throws Exception {
        Id canonicalIdA = Id.valueOf(10L);
        Id canonicalIdB = Id.valueOf(11L);
        Id canonicalIdC = Id.valueOf(12L);

        DelegateIndexQueryResult delegateResult = DelegateIndexQueryResult.builder(100L)
                .add(Id.valueOf(0L), canonicalIdA, Publisher.METABROADCAST)
                .add(Id.valueOf(1L), canonicalIdB, Publisher.METABROADCAST)
                .add(Id.valueOf(2L), canonicalIdA, Publisher.BBC)
                .add(Id.valueOf(3L), canonicalIdC, Publisher.BBC)
                .add(Id.valueOf(4L), canonicalIdC, Publisher.METABROADCAST)
                .add(Id.valueOf(5L), canonicalIdA, Publisher.PA)
                .build();

        List<Publisher> publishers = Lists.newArrayList(
                Publisher.PA,
                Publisher.BBC,
                Publisher.METABROADCAST
        );

        setupMocks(delegateResult, publishers);

        IndexQueryResult queryResult = pseudoEquivalentContentSearcher.query(
                query, publishers, Selection.ALL
        ).get();

        assertThat(queryResult.getIds().size(), is(3));

        assertThat(queryResult.getIds().get(0), is(Id.valueOf(1L)));
        assertThat(queryResult.getIds().get(1), is(Id.valueOf(3L)));
        assertThat(queryResult.getIds().get(2), is(Id.valueOf(5L)));
    }

    private void setupMocks(DelegateIndexQueryResult queryResult, List<Publisher> publishers) {
        when(esUnequivalentContentSearcher.delegateQuery(
                eq(Lists.newArrayList()),
                eq(publishers),
                any(Selection.class))
        )
                .thenReturn(Futures.immediateFuture(queryResult));
    }
}
