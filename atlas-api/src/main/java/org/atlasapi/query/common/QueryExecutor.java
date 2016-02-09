package org.atlasapi.query.common;

import javax.annotation.Nonnull;

public interface QueryExecutor<T> {

    @Nonnull
    QueryResult<T> execute(@Nonnull Query<T> query) throws QueryExecutionException;

}
