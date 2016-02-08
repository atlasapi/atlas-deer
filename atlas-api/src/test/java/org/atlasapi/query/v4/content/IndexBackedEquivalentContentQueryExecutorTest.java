package org.atlasapi.query.v4.content;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IndexBackedEquivalentContentQueryExecutorTest {

    private @Mock ContentIndex contentIndex;
    private @Mock MergingEquivalentsResolver<Content> equivalentContentResolver;
    private IndexBackedEquivalentContentQueryExecutor qe;

    @Before
    public void setup() {
        qe = new IndexBackedEquivalentContentQueryExecutor(contentIndex, equivalentContentResolver);
    }

    @Test(expected = NotFoundException.class)
    public void testNoContentForSingleQueryThrowsNotFoundException() throws Exception {

        Query<Content> query = Query.singleQuery(
                Id.valueOf(1),
                QueryContext.standard(mock(HttpServletRequest.class))
        );
        QueryContext ctxt = query.getContext();

        when(equivalentContentResolver.resolveIds(ImmutableSet.of(query.getOnlyId()),
                ctxt.getApplicationSources(), ImmutableSet.copyOf(ctxt.getAnnotations().values())
        ))
                .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>empty()));

        qe.execute(query);

    }
}
