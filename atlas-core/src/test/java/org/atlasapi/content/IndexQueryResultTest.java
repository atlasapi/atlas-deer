package org.atlasapi.content;

import org.atlasapi.entity.Id;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IndexQueryResultTest {

    private final Id idA = Id.valueOf(10L);
    private final Id idB = Id.valueOf(11L);

    @Test
    public void testResultWithSingleId() throws Exception {
        IndexQueryResult result = IndexQueryResult.withSingleId(idA);

        assertThat(result.getTotalCount(), is(1L));

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().first().get(), is(idA));
    }

    @Test
    public void testResultMultipleIds() throws Exception {
        IndexQueryResult result = IndexQueryResult.withIds(Lists.newArrayList(idA, idB), 10L);

        assertThat(result.getTotalCount(), is(10L));

        assertThat(result.getIds().size(), is(2));
        assertThat(result.getIds().get(0), is(idA));
        assertThat(result.getIds().get(1), is(idB));
    }
}