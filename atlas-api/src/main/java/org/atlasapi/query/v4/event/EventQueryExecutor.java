package org.atlasapi.query.v4.event;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.exceptions.UncheckedQueryExecutionException;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventQueryExecutor implements QueryExecutor<Event> {

    private final EventResolver resolver;

    public EventQueryExecutor(EventResolver resolver) {
        this.resolver = checkNotNull(resolver);
    }

    @Override
    public QueryResult<Event> execute(Query<Event> query) throws QueryExecutionException {
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

    private ListenableFuture<QueryResult<Event>> executeQuery(final Query<Event> query) {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private ListenableFuture<QueryResult<Event>> executeSingleQuery(final Query<Event> query) {
        Id id = query.getOnlyId();
        return Futures.transform(
                resolve(id),
                (Resolved<Event> input) -> {
                    List<Event> eventList = input.getResources().toList();
                    if (eventList.isEmpty()) {
                        throw new UncheckedQueryExecutionException(new NotFoundException(id));
                    }
                    Event resource = eventList.get(0);
                    return QueryResult.singleResult(resource, query.getContext());
                }
        );

    }

    private ListenableFuture<QueryResult<Event>> executeListQuery(final Query<Event> query) {
        throw new UnsupportedOperationException();
    }

    private ListenableFuture<Resolved<Event>> resolve(Id id) {
        return resolver.resolveIds(ImmutableSet.of(id));
    }
}
