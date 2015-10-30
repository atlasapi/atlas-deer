package org.atlasapi.content;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.SecondaryIndex;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;

@RunWith(MockitoJUnitRunner.class)
public class PseudoEquivalentContentIndexTest {

    private final AttributeQuerySet query = new AttributeQuerySet(Lists.newArrayList());
    private final List<Publisher> publishers = Lists.newArrayList(Publisher.METABROADCAST);
    private final Optional<IndexQueryParams> indexQueryParams = Optional.<IndexQueryParams>empty();

    private @Mock EsUnequivalentContentIndex esUnequivalentContentIndex;
    private @Mock SecondaryIndex equividIndex;
    private PseudoEquivalentContentIndex pseudoEquivalentContentIndex;

    @Before
    public void setUp() throws Exception {
        pseudoEquivalentContentIndex = new PseudoEquivalentContentIndex(
                esUnequivalentContentIndex, equividIndex
        );
    }

    @Test
    public void testQuery() throws Exception {
        ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap = ImmutableMultimap.of(
                Id.valueOf(0L), Id.valueOf(10L),
                Id.valueOf(0L), Id.valueOf(11L),
                Id.valueOf(1L), Id.valueOf(12L)
        );
        setupMocks(canonicalIdToIdMultiMap, 10L);

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
        setupMocks(canonicalIdToIdMultiMap, 100L);

        IndexQueryResult queryResult = pseudoEquivalentContentIndex.query(
                query, publishers, new Selection(2, 2), Optional.<IndexQueryParams>empty()
        ).get();

        assertThat(queryResult.getIds().size(), is(2));
        assertThat(queryResult.getIds(), containsInAnyOrder(Id.valueOf(2L), Id.valueOf(3L)));
    }

    @Test
    public void testQueryReturnsResolvedCanonicalIdAndNotTheIndexCanonicalId() throws Exception {
        Id indexCanonicalId = Id.valueOf(0L);
        Id id = Id.valueOf(10L);
        Id canonicalId = Id.valueOf(20L);

        ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap = ImmutableMultimap.<Id, Id>builder()
                .put(indexCanonicalId, id)
                .build();

        mockDelegate(canonicalIdToIdMultiMap, 1L);
        when(equividIndex.lookup(Lists.newArrayList(id.longValue())))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(
                        id.longValue(), canonicalId.longValue()
                )));

        IndexQueryResult queryResult = pseudoEquivalentContentIndex.query(
                query, publishers, Selection.ALL, Optional.<IndexQueryParams>empty()
        ).get();

        assertThat(Iterables.getOnlyElement(queryResult.getIds()), is(canonicalId));
    }

    @Test
    public void testQueryReturnsResolvedCanonicalIdFromFirstIdInSetThatResolves() throws Exception {
        Id indexCanonicalId = Id.valueOf(0L);
        Id idA = Id.valueOf(10L);
        Id idB = Id.valueOf(11L);
        Id canonicalId = Id.valueOf(20L);

        ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap = ImmutableMultimap.<Id, Id>builder()
                .put(indexCanonicalId, idA)
                .put(indexCanonicalId, idB)
                .build();

        mockDelegate(canonicalIdToIdMultiMap, 2L);
        when(equividIndex.lookup(Lists.newArrayList(idA.longValue())))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of()));
        when(equividIndex.lookup(Lists.newArrayList(idB.longValue())))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(
                        idB.longValue(), canonicalId.longValue()
                )));

        IndexQueryResult queryResult = pseudoEquivalentContentIndex.query(
                query, publishers, Selection.ALL, Optional.<IndexQueryParams>empty()
        ).get();

        assertThat(Iterables.getOnlyElement(queryResult.getIds()), is(canonicalId));
    }

    @Test
    public void testQueryReturnsIndexCanonicalIdWhenItFailsToResolveCanonicalId() throws Exception {
        Id indexCanonicalId = Id.valueOf(0L);
        Id id = Id.valueOf(10L);

        ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap = ImmutableMultimap.<Id, Id>builder()
                .put(indexCanonicalId, id)
                .build();

        mockDelegate(canonicalIdToIdMultiMap, 1L);
        when(equividIndex.lookup(Lists.newArrayList(id.longValue())))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of()));

        IndexQueryResult queryResult = pseudoEquivalentContentIndex.query(
                query, publishers, Selection.ALL, Optional.<IndexQueryParams>empty()
        ).get();

        assertThat(Iterables.getOnlyElement(queryResult.getIds()), is(indexCanonicalId));

    }

    private void setupMocks(ImmutableMultimap<Id, Id> canonicalIdToIdMultiMap, long resultCount) {
        mockDelegate(canonicalIdToIdMultiMap, resultCount);

        for (Map.Entry<Id, Id> entry : canonicalIdToIdMultiMap.entries()) {
            when(equividIndex.lookup(Lists.newArrayList(entry.getValue().longValue())))
                    .thenReturn(Futures.immediateFuture(ImmutableMap.of(
                            entry.getValue().longValue(),
                            entry.getKey().longValue()
                    )));
        }
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