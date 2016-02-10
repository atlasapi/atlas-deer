package org.atlasapi.query.common;

import org.atlasapi.content.QueryParseException;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.criteria.operator.Operators;

import static com.google.common.base.Preconditions.checkNotNull;

public final class QueryAtomParser<I, O> {

    public static final <I, O> QueryAtomParser<I, O> valueOf(Attribute<O> attribute,
            AttributeCoercer<I, O> coercer) {
        return new QueryAtomParser<I, O>(attribute, coercer);
    }

    private final Attribute<O> attribute;
    private final AttributeCoercer<I, O> coercer;

    public QueryAtomParser(Attribute<O> attribute, AttributeCoercer<I, O> coercer) {
        this.attribute = checkNotNull(attribute);
        this.coercer = checkNotNull(coercer);
    }

    public Attribute<O> getAttribute() {
        return attribute;
    }

    public AttributeQuery<O> parse(String key, Iterable<I> rawValues)
            throws QueryParseException {
        return attribute.createQuery(operator(key), parse(rawValues));
    }

    private Operator operator(String key) throws InvalidOperatorException {
        if (key.equals(attribute.externalName())) {
            return Operators.EQUALS;
        }
        // +1 for . operator separator
        String operatorName = key.substring(attribute.externalName().length() + 1);
        Operator operator = Operators.lookup(operatorName);
        if (operator != null) {
            return operator;
        }
        throw new InvalidOperatorException(
                String.format("unknown operator '%s'", operatorName));
    }

    private Iterable<O> parse(Iterable<I> rawValues)
            throws InvalidAttributeValueException {
        return coercer.apply(rawValues);
    }

}
