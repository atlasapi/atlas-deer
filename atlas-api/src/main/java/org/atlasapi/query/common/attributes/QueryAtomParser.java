package org.atlasapi.query.common.attributes;

import org.atlasapi.content.QueryParseException;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;
import org.atlasapi.query.common.exceptions.InvalidOperatorException;

import static com.google.common.base.Preconditions.checkNotNull;

public final class QueryAtomParser<O> {

    private final Attribute<O> attribute;
    private final AttributeCoercer<O> coercer;

    private QueryAtomParser(Attribute<O> attribute, AttributeCoercer<O> coercer) {
        this.attribute = checkNotNull(attribute);
        this.coercer = checkNotNull(coercer);
    }

    public static <O> QueryAtomParser<O> create(
            Attribute<O> attribute,
            AttributeCoercer<O> coercer
    ) {
        return new QueryAtomParser<>(attribute, coercer);
    }

    public Attribute<O> getAttribute() {
        return attribute;
    }

    public AttributeQuery<O> parse(String key, Iterable<String> rawValues)
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

    private Iterable<O> parse(Iterable<String> rawValues)
            throws InvalidAttributeValueException {
        return coercer.apply(rawValues);
    }
}
