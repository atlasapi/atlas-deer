package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.BooleanAttributeQuery;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

public class BooleanAttribute extends Attribute<Boolean> {

    private BooleanAttribute(
            String name,
            Class<? extends Identified> target
    ) {
        super(name, target);
    }

    private BooleanAttribute(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        super(name, javaAttributeName, target);
    }

    public static BooleanAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new BooleanAttribute(name, target);
    }

    public static BooleanAttribute create(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        return new BooleanAttribute(name, javaAttributeName, target);
    }

    @Override
    public String toString() {
        return "Boolean valued attribute: " + name;
    }

    @Override
    public AttributeQuery<Boolean> createQuery(Operator op, Iterable<Boolean> values) {
        if (!(op instanceof EqualsOperator)) {
            throw new IllegalArgumentException();
        }
        return new BooleanAttributeQuery(this, (EqualsOperator) op, values);
    }

    @Override
    public Class<Boolean> requiresOperandOfType() {
        return Boolean.class;
    }
}
