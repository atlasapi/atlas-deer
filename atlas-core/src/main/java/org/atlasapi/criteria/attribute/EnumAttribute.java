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
            Class<? extends Identified> target,
            boolean isCollection
    ) {
        super(name, target, isCollection);
        this.type = type;
    }

    public static <T extends Enum<T>> EnumAttribute<T> single(
            String name,
            Class<T> type,
            Class<? extends Identified> target
    ) {
        return new EnumAttribute<T>(name, type, target, false);
    }

    public static <T extends Enum<T>> EnumAttribute<T> list(
            String name,
            Class<T> type,
            Class<? extends Identified> target
    ) {
        return new EnumAttribute<T>(name, type, target, true);
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
