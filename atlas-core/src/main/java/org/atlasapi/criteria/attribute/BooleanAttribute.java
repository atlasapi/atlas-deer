package org.atlasapi.criteria.attribute;

import javax.annotation.Nullable;

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
            @Nullable BooleanMapping directMapping,
            Class<? extends Identified> target
    ) {
        super(name, directMapping, target);
    }

    public static BooleanAttribute create(
            String name,
            BooleanMapping directMapping,
            Class<? extends Identified> target
    ) {
        return new BooleanAttribute(name, directMapping, target);
    }

    public static BooleanAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new BooleanAttribute(name, null, target);
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
