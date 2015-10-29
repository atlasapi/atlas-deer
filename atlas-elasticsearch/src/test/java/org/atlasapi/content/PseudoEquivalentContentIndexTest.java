package org.atlasapi.content;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;

@RunWith(MockitoJUnitRunner.class)
public class PseudoEquivalentContentIndexTest {

    private final AttributeQuerySet query = new AttributeQuerySet(Lists.newArrayList());
    private final List<Publisher> publishers = Lists.newArrayList(Publisher.METABROADCAST);
    private final Optional<IndexQueryParams> indexQueryParams = Optional.<IndexQueryParams>empty();

    private @Mock EsUnequivalentContentIndex esUnequivalentContentIndex;
    private PseudoEquivalentContentIndex pseudoEquivalentContentIndex;

    @Before
    public void setUp() throws Exception {
        pseudoEquivalentContentIndex = new PseudoEquivalentContentIndex(esUnequivalentContentIndex);
    }

    @Test
    public void testQuery() throws Exception {
        ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap = ImmutableMultimap.of(
                Id.valueOf(0L), Id.valueOf(10L),
                Id.valueOf(0L), Id.valueOf(11L),
                Id.valueOf(1L), Id.valueOf(12L)
        );
        mockDelegate(canonicalIdToIdMultiMap, 10L);

        IndexQueryResult queryResult = pseudoEquivalentContentIndex.query(
                query, publishers, Selection.ALL, indexQueryParams
        ).get();

        assertThat(queryResult.getTotalCount(), is(10L));

        assertThat(queryResult.getIds().size(), is(2));
        assertThat(queryResult.getIds(), containsInAnyOrder(Id.valueOf(0L), Id.valueOf(1L)));
    }

    @Test
    public void testQueryPagination() throws Exception {
        ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap = ImmutableMultimap.<Id, Id>builder()
                .put(Id.valueOf(0L), Id.valueOf(10L))
                .put(Id.valueOf(1L), Id.valueOf(11L))
                .put(Id.valueOf(2L), Id.valueOf(13L))
                .put(Id.valueOf(3L), Id.valueOf(14L))
                .put(Id.valueOf(4L), Id.valueOf(15L))
                .put(Id.valueOf(5L), Id.valueOf(16L))
                .build();
        mockDelegate(canonicalIdToIdMultiMap, 100L);

        IndexQueryResult queryResult = pseudoEquivalentContentIndex.query(
                query, publishers, new Selection(2, 2), Optional.<IndexQueryParams>empty()
        ).get();

        assertThat(queryResult.getIds().size(), is(2));
        assertThat(queryResult.getIds(), containsInAnyOrder(Id.valueOf(2L), Id.valueOf(3L)));
    }

    private void mockDelegate(ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap, long resultCount) {
        when(esUnequivalentContentIndex.query(
                eq(new AttributeQuerySet(Lists.newArrayList())),
                eq(publishers),
                any(Selection.class),
                eq(indexQueryParams))
        )
                .thenReturn(Futures.immediateFuture(IndexQueryResult.withIdsAndCanonicalIds(
                        canonicalIdToIdMultiMap, resultCount
                )));
    }
}