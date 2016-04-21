package org.atlasapi.query.common.useraware;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class UserAccountsAwareQuery<T> {

    public static final <T> SingleQuery<T> singleQuery(Id id, UserAccountsAwareQueryContext context) {
        return new SingleQuery<T>(id, context);
    }

    public static final <T> ListQuery<T> listQuery(AttributeQuerySet operands,
            UserAccountsAwareQueryContext context) {
        return new ListQuery<T>(operands, context);
    }

    private final UserAccountsAwareQueryContext context;

    protected UserAccountsAwareQuery(UserAccountsAwareQueryContext context) {
        this.context = checkNotNull(context);
    }

    public UserAccountsAwareQueryContext getContext() {
        return context;
    }

    public abstract boolean isListQuery();

    public abstract AttributeQuerySet getOperands();

    public abstract Id getOnlyId();

    public static final class SingleQuery<T> extends UserAccountsAwareQuery<T> {

        private final Id id;

        public SingleQuery(Id id, UserAccountsAwareQueryContext context) {
            super(context);
            this.id = checkNotNull(id);
        }

        public Id getOnlyId() {
            return id;
        }

        @Override
        public boolean isListQuery() {
            return false;
        }

        @Override
        public AttributeQuerySet getOperands() {
            throw new IllegalStateException(
                    "Query.getOperands() cannot be called on a single query");
        }

    }

    public static final class ListQuery<T> extends UserAccountsAwareQuery<T> {

        private final AttributeQuerySet operands;

        public ListQuery(AttributeQuerySet operands, UserAccountsAwareQueryContext context) {
            super(context);
            this.operands = checkNotNull(operands);
        }

        public AttributeQuerySet getOperands() {
            return this.operands;
        }

        @Override
        public boolean isListQuery() {
            return true;
        }

        @Override
        public Id getOnlyId() {
            throw new IllegalStateException("Query.getOnlyId() cannot be called on a list query");
        }

    }

}
