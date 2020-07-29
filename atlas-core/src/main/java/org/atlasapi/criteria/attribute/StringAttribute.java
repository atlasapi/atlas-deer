package org.atlasapi.criteria.attribute;

import javax.annotation.Nullable;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.criteria.operator.StringOperator;
import org.atlasapi.entity.Identified;

import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class StringAttribute extends Attribute<String> {

    private StringAttribute(
            String name,
            @Nullable ChildTypeMapping<String> directMapping,
            Class<? extends Identified> target
    ) {
        super(name, directMapping, target);
    }

    public static StringAttribute create(
            String name,
            ChildTypeMapping<String> directMapping,
            Class<? extends Identified> target
    ) {
        return new StringAttribute(name, directMapping, target);
    }

    public static StringAttribute create(
            String name,
            Class<? extends Identified> target
    ) {
        return new StringAttribute(name, null, target);
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
