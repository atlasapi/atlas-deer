package org.atlasapi.neo4j.service.query;

import java.util.stream.StreamSupport;

import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.entity.Id;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryExecutor {

    private final Session session;

    private QueryExecutor(Session session) {
        this.session = checkNotNull(session);
    }

    public static QueryExecutor create(Session session) {
        return new QueryExecutor(session);
    }

    public ListenableFuture<IndexQueryResult> execute(GraphQuery query) {
        Result result = session.query(
                query.getQuery().getQuery(),
                query.getQuery().getParameters()
        );

        ImmutableSet<Id> ids = StreamSupport.stream(result.spliterator(), false)
                .map(resultMap -> resultMap.get("id").toString())
                .map(Id::valueOf)
                .collect(ImmutableCollectors.toSet());

        IndexQueryResult queryResult = IndexQueryResult.withIds(ids, ids.size());

        return Futures.immediateFuture(queryResult);
    }
}
