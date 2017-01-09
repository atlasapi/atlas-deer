package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.criteria.operator.StringOperator;
import org.atlasapi.entity.Identified;

public class StringAttribute extends Attribute<String> {

    private StringAttribute(
            String name, Class<? extends Identified> target,
            boolean isCollectionOfValues
    ) {
        super(name, target, isCollectionOfValues);
    }

    private StringAttribute(
            String name, String javaAttributeName,
            Class<? extends Identified> target,
            boolean isCollectionOfValues
    ) {
        super(name, javaAttributeName, target, isCollectionOfValues);
    }

    public static StringAttribute single(
            String name,
            Class<? extends Identified> target
    ) {
        return new StringAttribute(name, target, false);
    }

    public static StringAttribute single(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        return new StringAttribute(name, javaAttributeName, target, false);
    }

    public static StringAttribute list(
            String name,
            Class<? extends Identified> target
    ) {
        return new StringAttribute(name, target, true);
    }

    public static StringAttribute list(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        return new StringAttribute(name, javaAttributeName, target, true);
    }

    @Override
    public String toString() {
        return "String valued attribute: " + name;
    }

    @Override
    public Class<String> requiresOperandOfType() {
        return String.class;
    }

    @Override
    public AttributeQuery<String> createQuery(Operator op, Iterable<String> values) {
        if (!(op instanceof StringOperator)) {
            throw new IllegalArgumentException();
        }
        return new StringAttributeQuery(this, (StringOperator) op, values);
    }
}
