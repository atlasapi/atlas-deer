package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.FloatAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

public class FloatAttribute extends Attribute<Float> {

    private FloatAttribute(
            String name,
            Class<? extends Identified> target,
            boolean isCollectionOfValues
    ) {
        super(name, target, isCollectionOfValues);
    }

    private FloatAttribute(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target,
            boolean isCollectionOfValues
    ) {
        super(name, javaAttributeName, target, isCollectionOfValues);
    }

    public static FloatAttribute single(
            String name,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, target, false);
    }

    public static FloatAttribute single(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, javaAttributeName, target, false);
    }

    public static FloatAttribute list(
            String name,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, target, true);
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
