package org.atlasapi.output.useraware;

import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;

import com.google.common.collect.FluentIterable;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class UserAccountsAwareQueryResult<T> {

    public static final <T> SingleQueryResult<T> singleResult(T resource,
            UserAccountsAwareQueryContext context) {
        return new SingleQueryResult<T>(resource, context);
    }

    public static final <T> ListQueryResult<T> listResult(Iterable<T> resource,
            UserAccountsAwareQueryContext context) {
        return new ListQueryResult<T>(resource, context);
    }

    private final UserAccountsAwareQueryContext context;

    protected UserAccountsAwareQueryResult(UserAccountsAwareQueryContext context) {
        this.context = checkNotNull(context);
    }

    public UserAccountsAwareQueryContext getContext() {
        return context;
    }

    public abstract boolean isListResult();

    public abstract FluentIterable<T> getResources();

    public abstract T getOnlyResource();

    public static final class SingleQueryResult<T> extends UserAccountsAwareQueryResult<T> {

        private final T resource;

        public SingleQueryResult(T resource, UserAccountsAwareQueryContext context) {
            super(context);
            this.resource = checkNotNull(resource);
        }

        @Override
        public boolean isListResult() {
            return false;
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

    public static final class ListQueryResult<T> extends UserAccountsAwareQueryResult<T> {

        private final FluentIterable<T> resources;

        public ListQueryResult(Iterable<T> resources, UserAccountsAwareQueryContext context) {
            super(context);
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
                    "QueryResult.getOnlyResource() cannot be called on single result");
        }

    }

}
