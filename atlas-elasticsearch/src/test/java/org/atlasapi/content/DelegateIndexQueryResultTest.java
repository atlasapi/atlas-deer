package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DelegateIndexQueryResultTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Id canonicalIdA = Id.valueOf(0L);
    private final Id canonicalIdB = Id.valueOf(1L);
    private final Id idA = Id.valueOf(10L);
    private final Id idB = Id.valueOf(11L);

    @Test
    public void testResultWithCanonicalIdsAndPublishers() throws Exception {
        DelegateIndexQueryResult result = DelegateIndexQueryResult.builder(4)
                .add(idA, canonicalIdA, Publisher.METABROADCAST)
                .add(idB, canonicalIdB, Publisher.BBC)
                .build();

        assertThat(result.getTotalCount(), is(4L));

        assertThat(result.getIds().size(), is(2));
        assertThat(result.getIds().get(0), is(idA));
        assertThat(result.getIds().get(1), is(idB));

        assertThat(result.getResults().size(), is(2));

        assertThat(result.getResults().get(0).getId(), is(idA));
        assertThat(result.getResults().get(0).getCanonicalId(), is(canonicalIdA));
        assertThat(result.getResults().get(0).getPublisher(), is(Publisher.METABROADCAST));

        assertThat(result.getResults().get(1).getId(), is(idB));
        assertThat(result.getResults().get(1).getCanonicalId(), is(canonicalIdB));
        assertThat(result.getResults().get(1).getPublisher(), is(Publisher.BBC));
    }

    @Test
    public void testResultBuilderRejectsNullCanonicalIds() throws Exception {
        exception.expect(NullPointerException.class);
        DelegateIndexQueryResult.builder(1)
                .add(idA, null, Publisher.METABROADCAST)
                .build();
    }

    @Test
    public void testResultBuilderRejectsNullPublishers() throws Exception {
        exception.expect(NullPointerException.class);
        DelegateIndexQueryResult.builder(1)
                .add(idA, canonicalIdA, null)
                .build();
    }
}