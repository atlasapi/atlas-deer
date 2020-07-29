package org.atlasapi.criteria.attribute;

import javax.annotation.Nullable;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.IntegerAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;

public class IntegerAttribute extends Attribute<Integer> {

    private IntegerAttribute(
            String name,
            @Nullable RangeTypeMapping<Integer> directMapping,
            Class<? extends Identified> target
    ) {
        super(name, directMapping, target);
    }

    public static IntegerAttribute create(
            String name,
            RangeTypeMapping<Integer> directMapping,
            Class<? extends Identified> target
    ) {
        return new IntegerAttribute(name, directMapping, target);
    }

    public static IntegerAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new IntegerAttribute(name, null, target);
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
