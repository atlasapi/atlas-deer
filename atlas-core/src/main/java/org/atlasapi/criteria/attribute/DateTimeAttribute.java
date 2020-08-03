package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.DateTimeAttributeQuery;
import org.atlasapi.criteria.operator.DateTimeOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

import org.joda.time.DateTime;

public class DateTimeAttribute extends Attribute<DateTime> {

    private DateTimeAttribute(
            String name,
            Class<? extends Identified> target
    ) {
        super(name, target);
    }

    public static DateTimeAttribute create(String name, Class<? extends Identified> target) {
        return new DateTimeAttribute(name, target);
    }

    @Override
    public String toString() {
        return "DateTime valued attribute: " + name;
    }

    @Override
    public AttributeQuery<DateTime> createQuery(Operator op, Iterable<DateTime> values) {
        if (!(op instanceof DateTimeOperator)) {
            throw new IllegalArgumentException();
        }
        return new DateTimeAttributeQuery(this, (DateTimeOperator) op, values);
    }

    @Override
    public Class<DateTime> requiresOperandOfType() {
        return DateTime.class;
    }
}
