package org.atlasapi.query.v4.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.response.ContentResult;
import com.metabroadcast.sherlock.client.response.ContentSearchQueryResponse;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PseudoEsEquivalentContentSearcherTest {
    private final SearchQuery.Builder query = SearchQuery.builder();

    private @Mock
    SherlockSearcher sherlockSearcher;
    private PseudoEsEquivalentContentSearcher pseudoEsEquivalentContentSearcher;

    @Before
    public void setUp() throws Exception {
        pseudoEsEquivalentContentSearcher = PseudoEsEquivalentContentSearcher.create(
                sherlockSearcher
        );
    }

    @Test
    public void testQuery() throws Exception {
        ContentSearchQueryResponse response = new ContentSearchQueryResponse(
                ImmutableList.of(
                        new ContentResult(0, 1, 10, Publisher.METABROADCAST.key(), null),
                        new ContentResult(1, 1, 10, Publisher.BBC.key(), null),
                        new ContentResult(2, 1, 11, Publisher.METABROADCAST.key(), null)
                ),
                10
        );

        List<Publisher> publishers = Lists.newArrayList(
                Publisher.METABROADCAST,
                Publisher.BBC
        );

        setupMocks(response);

        IndexQueryResult queryResult = pseudoEsEquivalentContentSearcher.searchForContent(
                query, publishers, Selection.ALL
        ).get();

        assertThat(queryResult.getTotalCount(), is(10L));

        assertThat(queryResult.getIds().size(), is(2));
        assertThat(queryResult.getIds(), containsInAnyOrder(Id.valueOf(0L), Id.valueOf(2L)));
    }

    @Test
    public void testQueryPagination() throws Exception {
        ContentSearchQueryResponse response = new ContentSearchQueryResponse(
                ImmutableList.of(
                        new ContentResult(0, 1, 10, Publisher.METABROADCAST.key(), null),
                        new ContentResult(1, 1, 11, Publisher.METABROADCAST.key(), null),
                        new ContentResult(2, 1, 12, Publisher.METABROADCAST.key(), null),
                        new ContentResult(3, 1, 13, Publisher.METABROADCAST.key(), null),
                        new ContentResult(4, 1, 14, Publisher.METABROADCAST.key(), null),
                        new ContentResult(5, 1, 15, Publisher.METABROADCAST.key(), null)
                ),
                100
        );

        List<Publisher> publishers = Lists.newArrayList(Publisher.METABROADCAST);

        setupMocks(response);

        IndexQueryResult queryResult = pseudoEsEquivalentContentSearcher.searchForContent(
                query, publishers, new Selection(2, 2)
        ).get();

        assertThat(queryResult.getIds().size(), is(2));
        assertThat(queryResult.getIds(), containsInAnyOrder(Id.valueOf(2L), Id.valueOf(3L)));
    }

    @Test
    public void testQueryReturnsIdFromHighestPrecedencePublisher() throws Exception {
        long canonicalIdA = 10;
        long canonicalIdB = 11;
        long canonicalIdC = 12;

        ContentSearchQueryResponse response = new ContentSearchQueryResponse(
                ImmutableList.of(
                        new ContentResult(0, 1, canonicalIdA, Publisher.METABROADCAST.key(), null),
                        new ContentResult(1, 1, canonicalIdB, Publisher.METABROADCAST.key(), null),
                        new ContentResult(2, 1, canonicalIdA, Publisher.BBC.key(), null),
                        new ContentResult(3, 1, canonicalIdC, Publisher.BBC.key(), null),
                        new ContentResult(4, 1, canonicalIdC, Publisher.METABROADCAST.key(), null),
                        new ContentResult(5, 1, canonicalIdA, Publisher.PA.key(), null)
                ),
                100
        );

        List<Publisher> publishers = Lists.newArrayList(
                Publisher.PA,
                Publisher.BBC,
                Publisher.METABROADCAST
        );

        setupMocks(response);

        IndexQueryResult queryResult = pseudoEsEquivalentContentSearcher.searchForContent(
                query, publishers, Selection.ALL
        ).get();

        assertThat(queryResult.getIds().size(), is(3));

        assertThat(queryResult.getIds().get(0), is(Id.valueOf(1L)));
        assertThat(queryResult.getIds().get(1), is(Id.valueOf(3L)));
        assertThat(queryResult.getIds().get(2), is(Id.valueOf(5L)));
    }

    private void setupMocks(ContentSearchQueryResponse queryResult) {
        when(sherlockSearcher.searchForContent(any())).thenReturn(Futures.immediateFuture(queryResult));
    }

}