package org.atlasapi.query.common;

import com.google.common.collect.FluentIterable;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class QueryResult<T> {

    public static final <T> SingleQueryResult<T> singleResult(T resource, QueryContext context) {
        return new SingleQueryResult<T>(resource, context);
    }

    public static final <T> ListQueryResult<T> listResult(Iterable<T> resource,
            QueryContext context, Long resultCount) {
        return new ListQueryResult<T>(resource, context, resultCount);
    }

    public static final <T> ListQueryResult<T> listResult(Iterable<T> resource,
            QueryContext context, Integer resultCount) {
        return new ListQueryResult<T>(resource, context, Long.valueOf(resultCount));
    }

    private final QueryContext context;

    protected QueryResult(QueryContext context) {
        this.context = checkNotNull(context);
    }

    public QueryContext getContext() {
        return context;
    }

    public abstract boolean isListResult();

    public abstract long getTotalResults();

    public abstract FluentIterable<T> getResources();

    public abstract T getOnlyResource();

    public static final class SingleQueryResult<T> extends QueryResult<T> {

        private final T resource;

        public SingleQueryResult(T resource, QueryContext context) {
            super(context);
            this.resource = checkNotNull(resource);
        }

        @Override
        public boolean isListResult() {
            return false;
        }

        @Override
        public long getTotalResults() {
            return 1;
        }

        @Override
        public FluentIterable<T> getResources() {
            throw new IllegalStateException(
                    "QueryResult.getResources() cannot be called on single result");
        }

        public T getOnlyResource() {
            return resource;
        }

    }

    public static final class ListQueryResult<T> extends QueryResult<T> {

        private final FluentIterable<T> resources;
        private final Long resultCount;

        public ListQueryResult(Iterable<T> resources, QueryContext context, Long resultCount) {
            super(context);
            this.resultCount = checkNotNull(resultCount);
            this.resources = FluentIterable.from(resources);
        }

        @Override
        public boolean isListResult() {
            return true;
        }

        public FluentIterable<T> getResources() {
            return resources;
        }

        public T getOnlyResource() {
            throw new IllegalStateException(
                    "QueryResult.getOnlyResource() cannot be called on list result");
        }

        @Override
        public long getTotalResults() {
            return resultCount;
        }
    }

}
