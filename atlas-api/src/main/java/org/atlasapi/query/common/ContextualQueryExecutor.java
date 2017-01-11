package org.atlasapi.query.common;

import org.atlasapi.query.common.exceptions.QueryExecutionException;

public interface ContextualQueryExecutor<CONTEXT, RESOURCE> {

    ContextualQueryResult<CONTEXT, RESOURCE> execute(ContextualQuery<CONTEXT, RESOURCE> query)
            throws QueryExecutionException;

}
