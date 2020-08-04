package org.atlasapi.elasticsearch.util;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.BooleanAttributeQuery;
import org.atlasapi.criteria.DateTimeAttributeQuery;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.FloatAttributeQuery;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.IntegerAttributeQuery;
import org.atlasapi.criteria.MatchesNothing;
import org.atlasapi.criteria.QueryVisitor;
import org.atlasapi.criteria.SortAttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.legacy.LegacyTranslation;
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

import com.metabroadcast.sherlock.client.helpers.OccurrenceClause;
import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.Parameter;
import com.metabroadcast.sherlock.client.parameter.PrefixParameter;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.RegexParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermsParameter;
import com.metabroadcast.sherlock.common.type.BooleanMapping;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import com.metabroadcast.sherlock.common.type.FloatMapping;
import com.metabroadcast.sherlock.common.type.InstantMapping;
import com.metabroadcast.sherlock.common.type.IntegerMapping;
import com.metabroadcast.sherlock.common.type.KeywordMapping;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.joda.time.DateTime;

import static org.atlasapi.criteria.attribute.Attributes.ALIASES_NAMESPACE;
import static org.atlasapi.criteria.attribute.Attributes.ALIASES_VALUE;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_GROUP;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_TITLE_PREFIX;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_TYPE;
import static org.atlasapi.criteria.attribute.Attributes.GENRE;
import static org.atlasapi.criteria.attribute.Attributes.LOCATIONS_ALIASES_NAMESPACE;
import static org.atlasapi.criteria.attribute.Attributes.LOCATIONS_ALIASES_VALUE;
import static org.atlasapi.criteria.attribute.Attributes.SOURCE;
import static org.atlasapi.criteria.attribute.Attributes.SPECIALIZATION;
import static org.atlasapi.criteria.attribute.Attributes.TAG_RELATIONSHIP;
import static org.atlasapi.criteria.attribute.Attributes.TAG_SUPERVISED;
import static org.atlasapi.criteria.attribute.Attributes.TAG_WEIGHTING;

public class EsQueryBuilder {

    /**
     * These are the attributes that are supported by {@link EsQueryBuilder}
     */
    public static final ImmutableSet<Attribute<?>> SUPPORTED_ATTRIBUTES =
            ImmutableSet.<Attribute<?>>builder()
                    .add(CONTENT_TYPE)
                    .add(SOURCE)
                    .add(ALIASES_NAMESPACE)
                    .add(ALIASES_VALUE)
                    .add(LOCATIONS_ALIASES_NAMESPACE)
                    .add(LOCATIONS_ALIASES_VALUE)
                    .add(TAG_RELATIONSHIP)
                    .add(TAG_SUPERVISED)
                    .add(TAG_WEIGHTING)
                    .add(CONTENT_TITLE_PREFIX)
                    .add(GENRE)
                    .add(CONTENT_GROUP)
                    .add(SPECIALIZATION)
                    .build();

    private static final String NON_LETTER_PREFIX = "#";
    private static final String NON_LETTER_PREFIX_REGEX = "[^a-zA-Z]+.*";

    private final Function<String, LegacyTranslation> fieldTranslator;

    private EsQueryBuilder(Function<String, LegacyTranslation> fieldTranslator) {
        this.fieldTranslator = fieldTranslator;
    }

    public static EsQueryBuilder create(Function<String, LegacyTranslation> fieldTranslator) {
        return new EsQueryBuilder(fieldTranslator);
    }

    public BoolParameter buildQuery(Iterable<AttributeQuery<?>> operands) {

        SingleClauseBoolParameter.Builder mustBuilder =
                SingleClauseBoolParameter.builder(OccurrenceClause.MUST);

        for (AttributeQuery<?> operand : operands) {
            Optional<Parameter> parameter = queryForTerminal(operand);
            parameter.ifPresent(mustBuilder::addParameter);
        }

        return mustBuilder.build();
    }

    private Optional<Parameter> queryForTerminal(AttributeQuery<?> attributeQuery) {
        return attributeQuery.accept(new QueryVisitor<Optional<Parameter>>() {

            @Override
            public Optional<Parameter> visit(final IntegerAttributeQuery query) {
                final String name = query.getAttribute().javaAttributeName();
                final LegacyTranslation translation = getTranslationForAttributeName(name);
                if (translation.shouldSilentlyIgnore()) {
                    return Optional.empty();
                } else {
                    final IntegerMapping mapping = (IntegerMapping) translation.getMapping();
                    final List<Integer> values = query.getValue();
                    return Optional.of(query.accept(new EsComparableOperatorVisitor<>(mapping, values)));
                }
            }

            @Override
            public Optional<Parameter> visit(StringAttributeQuery query) {
                final String name = query.getAttribute().javaAttributeName();
                final LegacyTranslation translation = getTranslationForAttributeName(name);
                if (translation.shouldSilentlyIgnore()) {
                    return Optional.empty();
                } else {
                    final ChildTypeMapping<String> mapping =
                            (ChildTypeMapping<String>) translation.getMapping();
                    final List<String> values = query.getValue();
                    return Optional.of(query.accept(new EsStringOperatorVisitor(mapping, values)));
                }
            }

            @Override
            public Optional<Parameter> visit(BooleanAttributeQuery query) {
                final String name = query.getAttributeName();
                final LegacyTranslation translation = getTranslationForAttributeName(name);
                if (translation.shouldSilentlyIgnore()) {
                    return Optional.empty();
                } else {
                    final BooleanMapping mapping = (BooleanMapping) translation.getMapping();
                    final List<Boolean> value = query.getValue().subList(0, 1);
                    return Optional.of(query.accept(new EsEqualsOperatorVisitor<>(mapping, value)));
                }
            }

            @Override
            public Optional<Parameter> visit(EnumAttributeQuery<?> query) {
                final String name = query.getAttributeName();
                final LegacyTranslation translation = getTranslationForAttributeName(name);
                if (translation.shouldSilentlyIgnore()) {
                    return Optional.empty();
                } else {
                    KeywordMapping<String> mapping = (KeywordMapping<String>) translation.getMapping();
                    final List<String> values = Lists.transform(
                            query.getValue(),
                            Functions.toStringFunction()
                    );
                    return Optional.of(query.accept(new EsEqualsOperatorVisitor<>(mapping, values)));
                }
            }

            @Override
            public Optional<Parameter> visit(DateTimeAttributeQuery query) {
                final String name = query.getAttributeName();
                final LegacyTranslation translation = getTranslationForAttributeName(name);
                if (translation.shouldSilentlyIgnore()) {
                    return Optional.empty();
                } else {
                    final InstantMapping mapping = (InstantMapping) translation.getMapping();
                    final List<Instant> values = toInstants(query.getValue());
                    return Optional.of(query.accept(new EsComparableOperatorVisitor<>(mapping, values)));
                }
            }

            private List<Instant> toInstants(List<DateTime> value) {
                return Lists.transform(value, input -> input.toDate().toInstant());
            }

            @Override
            public Optional<Parameter> visit(MatchesNothing noOp) {
                throw new IllegalArgumentException();
            }

            @Override
            public Optional<Parameter> visit(IdAttributeQuery query) {
                final String name = query.getAttribute().javaAttributeName();
                final LegacyTranslation translation = getTranslationForAttributeName(name);
                if (translation.shouldSilentlyIgnore()) {
                    return Optional.empty();
                } else {
                    final KeywordMapping<Long> mapping = (KeywordMapping<Long>) translation.getMapping();
                    final List<Long> values = Lists.transform(query.getValue(), Id.toLongValue());
                    return Optional.of(query.accept(new EsEqualsOperatorVisitor<>(mapping, values)));
                }
            }

            @Override
            public Optional<Parameter> visit(FloatAttributeQuery query) {
                final String name = query.getAttribute().javaAttributeName();
                final LegacyTranslation translation = getTranslationForAttributeName(name);
                if (translation.shouldSilentlyIgnore()) {
                    return Optional.empty();
                } else {
                    final FloatMapping mapping = (FloatMapping) translation.getMapping();
                    final List<Float> values = query.getValue();
                    return Optional.of(query.accept(new EsComparableOperatorVisitor<>(mapping, values)));
                }
            }

            @Override
            public Optional<Parameter> visit(SortAttributeQuery query) {
                return Optional.empty();
            }

            private LegacyTranslation getTranslationForAttributeName(String name) {
                if (com.google.common.base.Strings.isNullOrEmpty(name)) {
                    throw new IllegalArgumentException("Tried to add a query for a null or empty field name.");
                }
                final LegacyTranslation translation = fieldTranslator.apply(name);
                if (translation.shouldThrowException()) {
                    throw new IllegalArgumentException(name + " is not a known field.");
                }
                return translation;
            }
        });
    }

    private static class EsEqualsOperatorVisitor<T, M extends ChildTypeMapping<T>>
            implements EqualsOperatorVisitor<Parameter> {

        protected List<T> values;
        protected M mapping;

        public EsEqualsOperatorVisitor(M mapping, List<T> values) {
            this.mapping = mapping;
            this.values = values;
        }

        @Override
        public TermsParameter<T> visit(Equals equals) {
            return TermsParameter.of(mapping, values);
        }

    }

    private static class EsStringOperatorVisitor extends EsEqualsOperatorVisitor<String, ChildTypeMapping<String>>
            implements StringOperatorVisitor<Parameter> {

        public EsStringOperatorVisitor(ChildTypeMapping<String> name, List<String> values) {
            super(name, values);
        }

        @Override
        public SingleValueParameter<String> visit(Beginning beginning) {
            String prefix = values.get(0);
            if (NON_LETTER_PREFIX.equals(prefix)) {
                return RegexParameter.of(mapping, NON_LETTER_PREFIX_REGEX);
            }
            return PrefixParameter.of(mapping, prefix);
        }

        @Override
        public SingleValueParameter<String> visit(Operators.Ascending ascending) {
            return null;
        }

        @Override
        public SingleValueParameter<String> visit(Operators.Descending ascending) {
            return null;
        }
    }

    private static class EsComparableOperatorVisitor<T extends Comparable<T>>
            extends EsEqualsOperatorVisitor<T, RangeTypeMapping<T>>
            implements ComparableOperatorVisitor<Parameter>,
            DateTimeOperatorVisitor<Parameter> {

        public EsComparableOperatorVisitor(RangeTypeMapping<T> mapping, List<T> values) {
            super(mapping, values);
        }

        private RangeParameter<T> rangeLessThan(RangeTypeMapping<T> mapping, List<T> values) {
            return RangeParameter.to(mapping, Ordering.natural().max(values));
        }

        private RangeParameter<T> rangeMoreThan(RangeTypeMapping<T> mapping, List<T> values) {
            return RangeParameter.from(mapping, Ordering.natural().min(values));
        }

        @Override
        public RangeParameter<T> visit(LessThan lessThan) {
            return rangeLessThan(mapping, values);
        }

        @Override
        public RangeParameter<T> visit(GreaterThan greaterThan) {
            return rangeMoreThan(mapping, values);
        }

        @Override
        public RangeParameter<T> visit(Operators.Ascending ascending) {
            return null;
        }

        @Override
        public RangeParameter<T> visit(Operators.Descending ascending) {
            return null;
        }

        @Override
        public RangeParameter<T> visit(Before before) {
            return rangeLessThan(mapping, values);
        }

        @Override
        public RangeParameter<T> visit(After after) {
            return rangeMoreThan(mapping, values);
        }

    }
}
