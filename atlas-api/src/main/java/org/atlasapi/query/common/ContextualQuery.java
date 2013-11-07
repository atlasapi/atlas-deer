package org.atlasapi.query.common;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.query.common.Query.ListQuery;
import org.atlasapi.query.common.Query.SingleQuery;

public class ContextualQuery<CONTEXT, RESOURCE> {
    
    public static final <C, R> ContextualQuery<C, R> valueOf(SingleQuery<C> contextQuery, ListQuery<R> resourceQuery, QueryContext context) {
        return new ContextualQuery<C, R>(contextQuery, resourceQuery, context);
    }
    
    private final SingleQuery<CONTEXT> contextQuery;
    private final ListQuery<RESOURCE> resourceQuery;
    private final QueryContext context;
    
    public ContextualQuery(SingleQuery<CONTEXT> contextQuery, ListQuery<RESOURCE> resourceQuery, QueryContext context) {
        this.contextQuery = checkNotNull(contextQuery);
        this.resourceQuery = checkNotNull(resourceQuery);
        this.context = checkNotNull(context);
    }
    
    public SingleQuery<CONTEXT> getContextQuery() {
        return this.contextQuery;
    }
    
    public ListQuery<RESOURCE> getResourceQuery() {
        return this.resourceQuery;
    }
    
    public QueryContext getContext() {
        return this.context;
    }

}
