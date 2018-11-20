package org.atlasapi.query.common;

import java.util.Objects;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.entity.Id;
import org.atlasapi.query.common.context.QueryContext;

import com.google.common.collect.ImmutableSet;
import org.w3c.dom.Attr;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Query<T> {

    public static final <T> SingleQuery<T> singleQuery(Id id, QueryContext context) {
        return new SingleQuery<T>(id, context);
    }

    public static final <T> SingleQuery<T> singleQuery(Id id, QueryContext context, AttributeQuerySet queryAttributes) {
        return new SingleQuery<T>(id, context, queryAttributes);
    }

    public static final <T> ListQuery<T> listQuery(AttributeQuerySet operands,
            QueryContext context) {
        return new ListQuery<T>(operands, context);
    }

    private final QueryContext context;

    protected Query(QueryContext context) {
        this.context = checkNotNull(context);
    }

    public QueryContext getContext() {
        return context;
    }

    public abstract boolean isListQuery();

    public abstract AttributeQuerySet getOperands();

    public abstract Id getOnlyId();

    public static final class SingleQuery<T> extends Query<T> {

        private final Id id;
        private final AttributeQuerySet operands;

        public SingleQuery(Id id, QueryContext context) {
            super(context);
            this.id = checkNotNull(id);
            this.operands = null;
        }

        public SingleQuery(Id id, QueryContext context, AttributeQuerySet operands) {
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
        public AttributeQuerySet getOperands() {
            if (operands.isEmpty()) {
                return AttributeQuerySet.create(ImmutableSet.of());
            }

            return operands;
        }

    }

    public static final class ListQuery<T> extends Query<T> {

        private final AttributeQuerySet operands;

        public ListQuery(AttributeQuerySet operands, QueryContext context) {
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
