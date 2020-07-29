package org.atlasapi.query.v4.content;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentSearcher;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.applications.client.model.internal.Application;

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

    private @Mock ContentSearcher contentIndex;
    private @Mock MergingEquivalentsResolver<Content> equivalentContentResolver;
    private IndexBackedEquivalentContentQueryExecutor qe;

    @Before
    public void setup() {
        qe = IndexBackedEquivalentContentQueryExecutor.create(contentIndex, equivalentContentResolver);
    }

    @Test(expected = NotFoundException.class)
    public void testNoContentForSingleQueryThrowsNotFoundException() throws Exception {

        Query<Content> query = Query.singleQuery(
                Id.valueOf(1),
                QueryContext.create(
                        mock(Application.class),
                        ActiveAnnotations.standard(),
                        mock(HttpServletRequest.class)
                )
        );
        QueryContext ctxt = query.getContext();

        when(equivalentContentResolver.resolveIds(ImmutableSet.of(query.getOnlyId()),
                ctxt.getApplication(),
                ImmutableSet.copyOf(ctxt.getAnnotations().values()),
                ImmutableSet.of()
        ))
                .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>empty()));

        qe.execute(query);

    }
}
