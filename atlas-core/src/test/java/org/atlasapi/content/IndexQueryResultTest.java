package org.atlasapi.content;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.entity.Id;
import org.junit.Test;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;

public class IndexQueryResultTest {

    private final Id canonicalIdA = Id.valueOf(0L);
    private final Id canonicalIdB = Id.valueOf(1L);
    private final Id idA = Id.valueOf(10L);
    private final Id idB = Id.valueOf(11L);
    private final Id idC = Id.valueOf(12L);

    @Test
    public void testResultWithSingleId() throws Exception {
        IndexQueryResult result = IndexQueryResult.withSingleId(idA);

        assertThat(result.getTotalCount(), is(1L));

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().first().get(), is(idA));
        assertThat(result.getCanonicalIds().size(), is(0));
    }

    @Test
    public void testResultMultipleIds() throws Exception {
        IndexQueryResult result = IndexQueryResult.withIds(Lists.newArrayList(idA, idB), 10L);

        assertThat(result.getTotalCount(), is(10L));

        assertThat(result.getIds().size(), is(2));
        assertThat(result.getIds().get(0), is(idA));
        assertThat(result.getIds().get(1), is(idB));
    }

    @Test
    public void testResultWithIdsAndCanonicalIdsHasExpectedIds() throws Exception {
        IndexQueryResult result = IndexQueryResult.withIdsAndCanonicalIds(
                ImmutableMultimap.<Id, Id>builder()
                        .put(canonicalIdA, idA)
                        .put(canonicalIdA, idB)
                        .put(canonicalIdB, idC)
                        .build(),
                10L
        );

        assertThat(result.getTotalCount(), is(10L));

        assertThat(result.getIds().size(), is(3));
        assertThat(result.getIds().get(0), is(idA));
        assertThat(result.getIds().get(1), is(idB));
        assertThat(result.getIds().get(2), is(idC));
    }

    @Test
    public void testGetCanonicalIdsOnlyReturnsUniqueIds() throws Exception {
        IndexQueryResult result = IndexQueryResult.withIdsAndCanonicalIds(
                ImmutableMultimap.<Id, Id>builder()
                        .put(canonicalIdA, idA)
                        .put(canonicalIdA, idB)
                        .put(canonicalIdB, idC)
                        .build(),
                10L
        );

        assertThat(result.getCanonicalIds().size(), is(2));
        assertThat(result.getCanonicalIds(), containsInAnyOrder(canonicalIdA, canonicalIdB));
    }

    @Test
    public void testGetIdsByCanonicalIdReturnsExpectedIds() throws Exception {
        IndexQueryResult result = IndexQueryResult.withIdsAndCanonicalIds(
                ImmutableMultimap.<Id, Id>builder()
                        .put(canonicalIdA, idA)
                        .put(canonicalIdA, idB)
                        .put(canonicalIdB, idC)
                        .build(),
                10L
        );

        assertThat(result.getIds(canonicalIdA).size(), is(2));
        assertThat(result.getIds(canonicalIdA).contains(idA), is(true));
        assertThat(result.getIds(canonicalIdA).contains(idB), is(true));
    }
}