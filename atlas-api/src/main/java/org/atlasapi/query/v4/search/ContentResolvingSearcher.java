package org.atlasapi.query.v4.search;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentResolvingSearcher {

    private final PseudoEsEquivalentContentSearcher searcher;
    private final MergingEquivalentsResolver<Content> contentResolver;
    private final long timeout;

    public ContentResolvingSearcher(
            PseudoEsEquivalentContentSearcher searcher,
            MergingEquivalentsResolver<Content> contentResolver,
            long timeout
    ) {
        this.searcher = checkNotNull(searcher);
        this.contentResolver = checkNotNull(contentResolver);
        this.timeout = timeout;
    }

    public QueryResult<Content> search(
            SearchQuery.Builder searchQuery,
            Selection selection,
            QueryContext queryContext,
            boolean isFuzzyQuery
    ) {
        try {
            AtomicLong totalResults = new AtomicLong();
            return Futures.transform(
                    Futures.transformAsync(
                            searcher.searchForContent(
                                    searchQuery,
                                    queryContext.getApplication()
                                            .getConfiguration()
                                            .getEnabledReadSources(),
                                    selection,
                                    isFuzzyQuery
                            ),
                            input -> {
                                totalResults.set(input.getTotalCount());
                                return resolve(input.getIds(), queryContext);
                            }
                    ),
                    (Function<ResolvedEquivalents<Content>, QueryResult<Content>>)
                    resolved -> QueryResult.listResult(
                            resolved.getFirstElems(),
                            queryContext,
                            totalResults.get()
                    )
            ).get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private ListenableFuture<ResolvedEquivalents<Content>> resolve(
            Iterable<Id> ids,
            QueryContext queryContext
    ) {
        if (Iterables.isEmpty(ids)) {
            return Futures.immediateFuture(ResolvedEquivalents.empty());
        } else {
            return contentResolver.resolveIds(
                    ids,
                    queryContext.getApplication(),
                    queryContext.getAnnotations().all(),
                    queryContext.getOperands()
            );
        }
    }
}
