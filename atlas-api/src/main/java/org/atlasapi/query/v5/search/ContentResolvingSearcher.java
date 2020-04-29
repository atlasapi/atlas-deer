package org.atlasapi.query.v5.search;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.UncheckedQueryExecutionException;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.sherlock.client.search.ContentSearcher;
import com.metabroadcast.sherlock.client.search.SearchQuery;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentResolvingSearcher {

    private final ContentSearcher searcher;
    private final MergingEquivalentsResolver<Content> contentResolver;
    private final NumberToShortStringCodec idCodec;
    private final long timeout;

    public ContentResolvingSearcher(
            ContentSearcher searcher,
            MergingEquivalentsResolver<Content> contentResolver,
            NumberToShortStringCodec idCodec,
            long timeout
    ) {
        this.searcher = checkNotNull(searcher);
        this.contentResolver = checkNotNull(contentResolver);
        this.idCodec = idCodec;
        this.timeout = timeout;
    }

    public QueryResult<Content> search(SearchQuery searchQuery, QueryContext queryContext) {
        try {
            return Futures.transform(
                    Futures.transformAsync(
                            searcher.searchForIds(searchQuery),
                            input -> resolve(input, queryContext)
                    ),
                    (Function<ResolvedEquivalents<Content>, QueryResult<Content>>)
                    resolved -> result(resolved, queryContext)
            ).get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private ListenableFuture<ResolvedEquivalents<Content>> resolve(
            Iterable<String> encodedIds,
            QueryContext queryContext
    ) {
        List<Id> ids = decodeIds(encodedIds);
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

    private List<Id> decodeIds(Iterable<String> input) {
        if (input == null) {
            return ImmutableList.of();
        } else {
            return StreamSupport.stream(input.spliterator(), false)
                    .map(idCodec::decode)
                    .map(Id::valueOf)
                    .collect(Collectors.toList());
        }
    }

    private QueryResult<Content> result(ResolvedEquivalents<Content> resolved, QueryContext queryContext) {
        Iterable<Content> resources = resolved.getFirstElems();
        return QueryResult.listResult(
                resources,
                queryContext,
                resolved.size()
        );
    }
}
