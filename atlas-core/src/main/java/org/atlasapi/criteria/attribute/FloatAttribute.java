package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.FloatAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

public class FloatAttribute extends Attribute<Float> {

    private FloatAttribute(
            String name,
            Class<? extends Identified> target
    ) {
        super(name, target);
    }

    private FloatAttribute(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        super(name, javaAttributeName, target);
    }

    public static FloatAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, target);
    }

    public static FloatAttribute create(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, javaAttributeName, target);
    }

    @Override
    public String toString() {
        return "Float valued attribute: " + name;
    }

    @Override
    public AttributeQuery<Float> createQuery(Operator op, Iterable<Float> values) {
        if (!(op instanceof ComparableOperator)) {
            throw new IllegalArgumentException();
        }
        return new FloatAttributeQuery(this, (ComparableOperator) op, values);
    }

    @Override
    public Class<Float> requiresOperandOfType() {
        return Float.class;
    }
}
