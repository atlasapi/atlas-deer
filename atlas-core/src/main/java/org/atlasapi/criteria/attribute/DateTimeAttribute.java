package org.atlasapi.criteria.attribute;

import javax.annotation.Nullable;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.DateTimeAttributeQuery;
import org.atlasapi.criteria.operator.DateTimeOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

import com.metabroadcast.sherlock.common.type.InstantMapping;

import org.joda.time.DateTime;

public class DateTimeAttribute extends Attribute<DateTime> {

    private DateTimeAttribute(
            String name,
            @Nullable InstantMapping directMapping,
            Class<? extends Identified> target
    ) {
        super(name, directMapping, target);
    }

    public static DateTimeAttribute create(
            String name,
            InstantMapping directMapping,
            Class<? extends Identified> target
    ) {
        return new DateTimeAttribute(name, directMapping, target);
    }

    public static DateTimeAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new DateTimeAttribute(name, null, target);
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
