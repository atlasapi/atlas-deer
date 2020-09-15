package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.IntegerAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

public class IntegerAttribute extends Attribute<Integer> {

    private IntegerAttribute(
            String name,
            Class<? extends Identified> target,
            boolean isCollection
    ) {
        super(name, target, isCollection);
    }

    public static IntegerAttribute list(
            String name,
            Class<? extends Identified> target
    ) {
        return new IntegerAttribute(name, target, true);
    }

    @Override
    public String toString() {
        return "Integer valued attribute: " + name;
    }

    @Override
    public AttributeQuery<Integer> createQuery(Operator op, Iterable<Integer> values) {
        if (!(op instanceof ComparableOperator)) {
            throw new IllegalArgumentException();
        }
        return new IntegerAttributeQuery(this, (ComparableOperator) op, values);
    }

    @Override
    public Class<Integer> requiresOperandOfType() {
        return Integer.class;
    }
}
