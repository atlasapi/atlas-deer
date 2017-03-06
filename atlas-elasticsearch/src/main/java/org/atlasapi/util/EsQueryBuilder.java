package org.atlasapi.util;

import java.util.Date;
import java.util.List;
import java.util.Stack;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.BooleanAttributeQuery;
import org.atlasapi.criteria.DateTimeAttributeQuery;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.FloatAttributeQuery;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.IntegerAttributeQuery;
import org.atlasapi.criteria.MatchesNothing;
import org.atlasapi.criteria.QueryNode;
import org.atlasapi.criteria.QueryNode.IntermediateNode;
import org.atlasapi.criteria.QueryNode.TerminalNode;
import org.atlasapi.criteria.QueryNodeVisitor;
import org.atlasapi.criteria.QueryVisitor;
import org.atlasapi.criteria.SortAttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperatorVisitor;
import org.atlasapi.criteria.operator.DateTimeOperatorVisitor;
import org.atlasapi.criteria.operator.EqualsOperatorVisitor;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.criteria.operator.Operators.After;
import org.atlasapi.criteria.operator.Operators.Before;
import org.atlasapi.criteria.operator.Operators.Beginning;
import org.atlasapi.criteria.operator.Operators.Equals;
import org.atlasapi.criteria.operator.Operators.GreaterThan;
import org.atlasapi.criteria.operator.Operators.LessThan;
import org.atlasapi.criteria.operator.StringOperatorVisitor;
import org.atlasapi.entity.Id;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.joda.time.DateTime;

public class EsQueryBuilder {

    private static final Joiner PATH_JOINER = Joiner.on(".");
    private static final String NON_LETTER_PREFIX = "#";
    private static final String NON_LETTER_PREFIX_REGEX = "[^a-zA-Z]+.*";

    public QueryBuilder buildQuery(AttributeQuerySet operands) {
        return operands.accept(new QueryNodeVisitor<QueryBuilder>() {

            Stack<String> traversed = new Stack<String>();

            @Override
            public QueryBuilder visit(IntermediateNode node) {
                BoolQueryBuilder conjuncts = QueryBuilders.boolQuery();
                traversed.addAll(node.pathSegments());
                for (QueryNode desc : node.getDescendants()) {
                    conjuncts.must(desc.accept(this));
                }
                for (int i = 0; i < node.pathSegments().size(); i++) {
                    traversed.pop();
                }
                return node.pathSegments().isEmpty() ? conjuncts
                                                     : nest(node, conjuncts);
            }

            private NestedQueryBuilder nest(IntermediateNode node, BoolQueryBuilder conjuncts) {
                return QueryBuilders.nestedQuery(nestPath(node), conjuncts);
            }

            private String nestPath(IntermediateNode node) {
                if (traversed.isEmpty()) {
                    return PATH_JOINER.join(node.pathSegments());
                }
                StringBuilder builder = new StringBuilder();
                builder = PATH_JOINER.appendTo(builder, traversed).append('.');
                return PATH_JOINER.appendTo(builder, node.pathSegments()).toString();
            }

            @Override
            public QueryBuilder visit(TerminalNode node) {
                AttributeQuery<?> query = node.getQuery();
                QueryBuilder builder = queryForTerminal(node);
                if (node.pathSegments().size() > 1) {
                    builder = QueryBuilders.nestedQuery(
                            query.getAttribute().getPathPrefix(),
                            builder
                    );
                }
                return builder;
            }
        });
    }

    private QueryBuilder queryForTerminal(TerminalNode node) {
        return node.getQuery().accept(new QueryVisitor<QueryBuilder>() {

            @Override
            public QueryBuilder visit(final IntegerAttributeQuery query) {
                final String name = query.getAttribute().javaAttributeName();
                final List<Integer> values = query.getValue();
                return query.accept(new EsComparableOperatorVisitor<Integer>(name, values));
            }

            @Override
            public QueryBuilder visit(StringAttributeQuery query) {
                final List<String> values = query.getValue();
                final String name = query.getAttribute().javaAttributeName();
                return query.accept(new EsStringOperatorVisitor(query.getAttribute()
                        .javaAttributeName(), values));
            }

            @Override
            public QueryBuilder visit(BooleanAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Boolean> value = query.getValue().subList(0, 1);
                return query.accept(new EsEqualsOperatorVisitor<Boolean>(name, value));
            }

            @Override
            public QueryBuilder visit(EnumAttributeQuery<?> query) {
                final String name = query.getAttributeName();
                /* Specialization is current indexed as uppercase and so we have this bodge
                    to ensure matches, remove once reindexing has been completed with lowercase values */
                if ("specialization".equalsIgnoreCase(name)) {
                    return query.accept(
                            new EsEqualsOperatorVisitor<>(
                                    name, Lists.transform(query.getValue(), v -> v.name())
                            )
                    );
                }
                final List<String> values = Lists.transform(
                        query.getValue(),
                        Functions.toStringFunction()
                );
                return query.accept(new EsEqualsOperatorVisitor<String>(name, values));
            }

            @Override
            public QueryBuilder visit(DateTimeAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Date> values = toDates(query.getValue());
                return query.accept(new EsComparableOperatorVisitor<Date>(name, values));
            }

            private List<Date> toDates(List<DateTime> value) {
                return Lists.transform(value, new Function<DateTime, Date>() {

                    @Override
                    public Date apply(DateTime input) {
                        return input.toDate();
                    }
                });
            }

            @Override
            public QueryBuilder visit(MatchesNothing noOp) {
                throw new IllegalArgumentException();
            }

            @Override
            public QueryBuilder visit(IdAttributeQuery query) {
                final String name = query.getAttribute().javaAttributeName();
                final List<Long> value = Lists.transform(query.getValue(), Id.toLongValue());
                return query.accept(new EsComparableOperatorVisitor<Long>(name, value));

            }

            @Override
            public QueryBuilder visit(FloatAttributeQuery query) {
                final String name = query.getAttribute().javaAttributeName();
                final List<Float> value = query.getValue();
                return query.accept(new EsComparableOperatorVisitor<Float>(name, value));
            }

            @Override
            public QueryBuilder visit(SortAttributeQuery query) {
                return null;
            }
        });
    }

    private static class EsEqualsOperatorVisitor<T>
            implements EqualsOperatorVisitor<QueryBuilder> {

        protected List<T> value;
        protected String name;

        public EsEqualsOperatorVisitor(String name, List<T> value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public QueryBuilder visit(Equals equals) {
            Object[] values = value.toArray(new Object[value.size()]);
            return QueryBuilders.termsQuery(name, values);
        }

    }

    private static class EsStringOperatorVisitor extends EsEqualsOperatorVisitor<String>
            implements StringOperatorVisitor<QueryBuilder> {

        public EsStringOperatorVisitor(String name, List<String> value) {
            super(name, value);
        }

        @Override
        public QueryBuilder visit(Beginning beginning) {
            String prefix = value.get(0);
            if (NON_LETTER_PREFIX.equals(prefix)) {
                return QueryBuilders.regexpQuery(name, NON_LETTER_PREFIX_REGEX);
            }
            return QueryBuilders.prefixQuery(name, prefix);
        }

        @Override
        public QueryBuilder visit(Operators.Ascending ascending) {
            return null;
        }

        @Override
        public QueryBuilder visit(Operators.Descending ascending) {
            return null;
        }
    }

    private static class EsComparableOperatorVisitor<T extends Comparable<T>>
            extends EsEqualsOperatorVisitor<T>
            implements ComparableOperatorVisitor<QueryBuilder>,
            DateTimeOperatorVisitor<QueryBuilder> {

        public EsComparableOperatorVisitor(String name, List<T> value) {
            super(name, value);
        }

        private RangeQueryBuilder rangeLessThan(String name, List<T> value) {
            return QueryBuilders.rangeQuery(name)
                    .lt(Ordering.natural().max(value));
        }

        private RangeQueryBuilder rangeMoreThan(String name, List<T> value) {
            return QueryBuilders.rangeQuery(name)
                    .gt(Ordering.natural().min(value));
        }

        @Override
        public QueryBuilder visit(LessThan lessThan) {
            return rangeLessThan(name, value);
        }

        @Override
        public QueryBuilder visit(GreaterThan greaterThan) {
            return rangeMoreThan(name, value);
        }

        @Override
        public QueryBuilder visit(Operators.Ascending ascending) {
            return null;
        }

        @Override
        public QueryBuilder visit(Operators.Descending ascending) {
            return null;
        }

        @Override
        public QueryBuilder visit(Before before) {
            return rangeLessThan(name, value);
        }

        @Override
        public QueryBuilder visit(After after) {
            return rangeMoreThan(name, value);
        }

    }
}
