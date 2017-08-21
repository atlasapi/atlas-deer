package org.atlasapi.query.common;

import org.atlasapi.query.common.exceptions.QueryExecutionException;

public interface ContextualQueryExecutor<CONTEXT, RESOURCE, RETURN_RESOURCE> {

    ContextualQueryResult<CONTEXT, RETURN_RESOURCE> execute(ContextualQuery<CONTEXT, RESOURCE, RETURN_RESOURCE> query)
            throws QueryExecutionException;

}
