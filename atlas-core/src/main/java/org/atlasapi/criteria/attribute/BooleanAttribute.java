package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.BooleanAttributeQuery;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

import com.metabroadcast.sherlock.common.type.BooleanMapping;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class BooleanAttribute extends Attribute<Boolean> {

    private BooleanAttribute(
            String name,
            BooleanMapping mapping,
            Class<? extends Identified> target
    ) {
        super(name, mapping, target);
    }

    public static BooleanAttribute create(
            String name,
            BooleanMapping mapping,
            Class<? extends Identified> target
    ) {
        return new BooleanAttribute(name, mapping, target);
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
