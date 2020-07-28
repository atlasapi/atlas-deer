package org.atlasapi.criteria.attribute;

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
            FloatMapping mapping,
            Class<? extends Identified> target
    ) {
        super(name, mapping, target);
    }

    private static FloatAttribute create(
            String name,
            FloatMapping mapping,
            Class<? extends Identified> target
    ) {
        return new FloatAttribute(name, mapping, target);
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
