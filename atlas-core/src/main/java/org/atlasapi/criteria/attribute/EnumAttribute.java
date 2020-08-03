package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

public class EnumAttribute<T extends Enum<T>> extends Attribute<T> {

    private final Class<T> type;

    private EnumAttribute(
            String name,
            Class<T> type,
            Class<? extends Identified> target
    ) {
        super(name, target);
        this.type = type;
    }

    public static <T extends Enum<T>> EnumAttribute<T> create(
            String name,
            Class<T> type,
            Class<? extends Identified> target
    ) {
        return new EnumAttribute<T>(name, type, target);
    }

    @Override
    public String toString() {
        return "Enum valued attribute: " + name;
    }

    @Override
    public Class<T> requiresOperandOfType() {
        return type;
    }

    @Override
    public AttributeQuery<T> createQuery(Operator op, Iterable<T> values) {
        if (!(op instanceof EqualsOperator)) {
            throw new IllegalArgumentException();
        }
        return new EnumAttributeQuery<T>(this, (EqualsOperator) op, values);
    }
}
