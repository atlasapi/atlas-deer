package org.atlasapi.query.v4.topic;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicIndex;
import org.atlasapi.topic.TopicResolver;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;

public class IndexBackedTopicQueryExecutor implements QueryExecutor<Topic> {

    private final TopicIndex index;
    private final TopicResolver resolver;

    public IndexBackedTopicQueryExecutor(TopicIndex index, TopicResolver resolver) {
        this.index = checkNotNull(index);
        this.resolver = checkNotNull(resolver);
    }

    @Override
    public QueryResult<Topic> execute(Query<Topic> query) throws QueryExecutionException {
        IndexQueryResult result = Futures.get(getResults(query), 1, TimeUnit.MINUTES, QueryExecutionException.class);
        Resolved<Topic> resolved = Futures.get(resolve(result.getIds()), 1, TimeUnit.MINUTES, QueryExecutionException.class);
        /* We do to ensure the content resolved from the store is in the order
            specified by the index */
        Resolved<Topic> orderedResolved = Resolved.valueOf(Ordering.explicit(result.getIds().toList())
                .onResultOf(Topic::getId)
                .immutableSortedCopy(resolved.getResources()));
        return resultFor(orderedResolved, query, result.getTotalCount());
    }

    private QueryResult<Topic> resultFor(Resolved<Topic> resolved, Query<Topic> query, Long totalCount) throws NotFoundException {
        return query.isListQuery() ? listResult(resolved, query, totalCount)
                                   : singleResult(resolved, query);
    }

    private QueryResult<Topic> singleResult(Resolved<Topic> resolved, Query<Topic> query) throws NotFoundException {
        Topic topic = Iterables.getOnlyElement(resolved.getResources(), null);
        if (topic == null) {
            throw new NotFoundException(query.getOnlyId());
        }
        return QueryResult.singleResult(topic, query.getContext());
    }

    private QueryResult<Topic> listResult(Resolved<Topic> resolved, Query<Topic> query, Long totalCount) {
        return QueryResult.listResult(resolved.getResources(), query.getContext(), totalCount);
    }

    private ListenableFuture<IndexQueryResult> getResults(Query<Topic> query)
            throws QueryExecutionException {
        if (query.isListQuery()) {
            return queryIndex(query);
        }
        FluentIterable<Id> ids = FluentIterable.from(ImmutableList.of(query.getOnlyId()));
        IndexQueryResult result = new IndexQueryResult(ids, ImmutableList.of(), 1L);
        return Futures.immediateFuture(result);
    }

    private ListenableFuture<IndexQueryResult> queryIndex(Query<Topic> query)
            throws QueryExecutionException {
        return index.query(
            query.getOperands(), 
            query.getContext().getApplicationSources().getEnabledReadSources(), 
            query.getContext().getSelection().or(Selection.ALL)
        );
    }

    private ListenableFuture<Resolved<Topic>> resolve(FluentIterable<Id> ids) {
        return resolver.resolveIds(ids);
    }

}
