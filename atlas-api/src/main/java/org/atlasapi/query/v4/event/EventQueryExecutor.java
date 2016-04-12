package org.atlasapi.query.v4.event;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventQueryExecutor implements QueryExecutor<EventV2> {

    private final EventV2Resolver resolver;

    public EventQueryExecutor(EventV2Resolver resolver) {
        this.resolver = checkNotNull(resolver);
    }

    @Override
    public QueryResult<EventV2> execute(Query<EventV2> query) throws QueryExecutionException {
        try {
            return Futures.get(
                    executeQuery(query),
                    1,
                    TimeUnit.MINUTES,
                    QueryExecutionException.class
            );
        } catch (UncheckedQueryExecutionException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private ListenableFuture<QueryResult<EventV2>> executeQuery(final Query<EventV2> query) {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private ListenableFuture<QueryResult<EventV2>> executeSingleQuery(final Query<EventV2> query) {
        Id id = query.getOnlyId();
        return Futures.transform(
                resolve(id),
                (Resolved<EventV2> input) -> {
                    List<EventV2> eventList = input.getResources().toList();
                    if (eventList.isEmpty()) {
                        throw new UncheckedQueryExecutionException(new NotFoundException(id));
                    }
                    EventV2 resource = eventList.get(0);
                    return QueryResult.singleResult(resource, query.getContext());
                }
        );

    }

    private ListenableFuture<QueryResult<EventV2>> executeListQuery(final Query<EventV2> query) {
        throw new UnsupportedOperationException();
    }

    private ListenableFuture<Resolved<EventV2>> resolve(Id id) {
        return resolver.resolveIds(ImmutableSet.of(id));
    }
}
