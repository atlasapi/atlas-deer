package org.atlasapi.criteria.attribute;

import javax.annotation.Nullable;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.FloatAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

import com.metabroadcast.sherlock.common.type.FloatMapping;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class FloatAttribute extends Attribute<Float> {

    private FloatAttribute(
            String name,
            @Nullable FloatMapping directMapping,
            Class<? extends Identified> target
    ) {
        super(name, directMapping, target);
    }

    public static FloatAttribute create(
            String name,
            FloatMapping directMapping,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, directMapping, target);
    }

    public static FloatAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, null, target);
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
