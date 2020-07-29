package org.atlasapi.query.v5.search;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentResolvingSearcher {

    private final SherlockSearcher searcher;
    private final MergingEquivalentsResolver<Content> contentResolver;
    private final long timeout;

    public ContentResolvingSearcher(
            SherlockSearcher searcher,
            MergingEquivalentsResolver<Content> contentResolver,
            long timeout
    ) {
        this.searcher = checkNotNull(searcher);
        this.contentResolver = checkNotNull(contentResolver);
        this.timeout = timeout;
    }

    public QueryResult<Content> search(SearchQuery searchQuery, QueryContext queryContext) {
        try {
            AtomicLong totalResults = new AtomicLong();
            return Futures.transform(
                    Futures.transformAsync(
                            searcher.searchForContent(searchQuery),
                            input -> {
                                totalResults.set(input.getTotalResults());
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
            List<Long> decodedIds,
            QueryContext queryContext
    ) {
        List<Id> ids = decodedIds.stream()
                .map(Id::valueOf)
                .collect(Collectors.toList());

        if (ids.isEmpty()) {
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
