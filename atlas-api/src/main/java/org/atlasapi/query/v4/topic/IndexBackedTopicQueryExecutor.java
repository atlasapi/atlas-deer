package org.atlasapi.query.v4.topic;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

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
import com.google.common.util.concurrent.AsyncFunction;
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
        return resultFor(Futures.get(resolve(getTopicIds(query)),
            1, TimeUnit.MINUTES, QueryExecutionException.class), query);
    }

    private QueryResult<Topic> resultFor(Resolved<Topic> resolved, Query<Topic> query) throws NotFoundException {
        return query.isListQuery() ? listResult(resolved, query)
                                   : singleResult(resolved, query);
    }

    private QueryResult<Topic> singleResult(Resolved<Topic> resolved, Query<Topic> query) throws NotFoundException {
        Topic topic = Iterables.getOnlyElement(resolved.getResources(), null);
        if (topic == null) {
            throw new NotFoundException(query.getOnlyId());
        }
        return QueryResult.singleResult(topic, query.getContext());
    }

    private QueryResult<Topic> listResult(Resolved<Topic> resolved, Query<Topic> query) {
        return QueryResult.listResult(resolved.getResources(), query.getContext(), Long.valueOf(resolved.getResources().size()));
    }

    private ListenableFuture<FluentIterable<Id>> getTopicIds(Query<Topic> query)
            throws QueryExecutionException {
        return query.isListQuery() ? queryIndex(query)
                                   : Futures.immediateFuture(FluentIterable.from(ImmutableList.of(query.getOnlyId())));
    }

    private ListenableFuture<FluentIterable<Id>> queryIndex(Query<Topic> query)
            throws QueryExecutionException {
        return index.query(
            query.getOperands(), 
            query.getContext().getApplicationSources().getEnabledReadSources(), 
            query.getContext().getSelection().or(Selection.ALL)
        );
    }

    private ListenableFuture<Resolved<Topic>> resolve(ListenableFuture<FluentIterable<Id>> ids) {
        return Futures.transform(ids, new AsyncFunction<FluentIterable<Id>, Resolved<Topic>>() {

            @Override
            public ListenableFuture<Resolved<Topic>> apply(FluentIterable<Id> ids)
                    throws Exception {
                return resolver.resolveIds(ids);
            }
        });
    }

}
