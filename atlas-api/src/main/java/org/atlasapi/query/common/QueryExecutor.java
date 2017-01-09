package org.atlasapi.query.common;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.exceptions.QueryExecutionException;

public interface QueryExecutor<T> {

    @Nonnull
    QueryResult<T> execute(@Nonnull Query<T> query) throws QueryExecutionException;

}
