package org.atlasapi.query.common;

public interface ContextualQueryExecutor<CONTEXT, RESOURCE> {

    ContextualQueryResult<CONTEXT, RESOURCE> execute(ContextualQuery<CONTEXT, RESOURCE> query)
            throws QueryExecutionException;

}
