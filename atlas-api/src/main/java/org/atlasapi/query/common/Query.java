package org.atlasapi.query.common;

import java.util.Set;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;
import org.atlasapi.query.common.context.QueryContext;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Query<T> {

    public static final <T> SingleQuery<T> singleQuery(Id id, QueryContext context) {
        return new SingleQuery<>(id, context);
    }

    public static final <T> SingleQuery<T> singleQuery(
            Id id,
            QueryContext context,
            Set<AttributeQuery<?>> queryAttributes
    ) {
        return new SingleQuery<>(id, context, queryAttributes);
    }

    public static final <T> ListQuery<T> listQuery(
            Set<AttributeQuery<?>> operands,
            QueryContext context
    ) {
        return new ListQuery<>(operands, context);
    }

    private final QueryContext context;

    protected Query(QueryContext context) {
        this.context = checkNotNull(context);
    }

    public QueryContext getContext() {
        return context;
    }

    public abstract boolean isListQuery();

    public abstract Iterable<AttributeQuery<?>> getOperands();

    public abstract Id getOnlyId();

    public static final class SingleQuery<T> extends Query<T> {

        private final Id id;
        private final Set<AttributeQuery<?>> operands;

        public SingleQuery(Id id, QueryContext context) {
            super(context);
            this.id = checkNotNull(id);
            this.operands = ImmutableSet.of();
        }

        public SingleQuery(Id id, QueryContext context, Set<AttributeQuery<?>> operands) {
            super(context);
            this.id = checkNotNull(id);
            this.operands = checkNotNull(operands);
        }

        public Id getOnlyId() {
            return id;
        }

        @Override
        public boolean isListQuery() {
            return false;
        }

        @Override
        public Set<AttributeQuery<?>> getOperands() {
            return operands;
        }

    }

    public static final class ListQuery<T> extends Query<T> {

        private final Set<AttributeQuery<?>> operands;

        public ListQuery(Set<AttributeQuery<?>> operands, QueryContext context) {
            super(context);
            this.operands = checkNotNull(operands);
        }

        public Set<AttributeQuery<?>> getOperands() {
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
