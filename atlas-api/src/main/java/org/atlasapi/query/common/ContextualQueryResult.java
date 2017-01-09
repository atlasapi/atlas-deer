package org.atlasapi.query.common;

import org.atlasapi.query.common.QueryResult.ListQueryResult;
import org.atlasapi.query.common.QueryResult.SingleQueryResult;
import org.atlasapi.query.common.context.QueryContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContextualQueryResult<CONTEXT, RESOURCE> {

    public static final <C, R> ContextualQueryResult<C, R> valueOf(
            SingleQueryResult<C> contextResult,
            ListQueryResult<R> resourceResult,
            QueryContext queryContext) {
        return new ContextualQueryResult<C, R>(contextResult, resourceResult, queryContext);
    }

    private final SingleQueryResult<CONTEXT> contextResult;
    private final ListQueryResult<RESOURCE> resourceResult;
    private final QueryContext queryContext;

    public ContextualQueryResult(SingleQueryResult<CONTEXT> contextResult,
            ListQueryResult<RESOURCE> resourceResult, QueryContext queryContext) {
        this.contextResult = checkNotNull(contextResult);
        this.resourceResult = checkNotNull(resourceResult);
        this.queryContext = checkNotNull(queryContext);
    }

    public SingleQueryResult<CONTEXT> getContextResult() {
        return this.contextResult;
    }

    public ListQueryResult<RESOURCE> getResourceResult() {
        return this.resourceResult;
    }

    public QueryContext getContext() {
        return this.queryContext;
    }

}
