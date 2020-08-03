package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.criteria.operator.StringOperator;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;

public class IdAttribute extends Attribute<Id> {

    private IdAttribute(
            String name,
            Class<? extends Identified> target
    ) {
        super(name, target);
    }

    private IdAttribute(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        super(name, javaAttributeName, target);
    }

    public static IdAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new IdAttribute(name, target);
    }

    public static IdAttribute create(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        return new IdAttribute(name, javaAttributeName, target);
    }

    @Override
    public String toString() {
        return "id attribute: " + name;
    }

    @Override
    public Class<Id> requiresOperandOfType() {
        return Id.class;
    }

    @Override
    public AttributeQuery<Id> createQuery(Operator op, Iterable<Id> values) {
        if (!(op instanceof StringOperator)) {
            throw new IllegalArgumentException();
        }
        return new IdAttributeQuery(this, (EqualsOperator) op, values);
    }
}
