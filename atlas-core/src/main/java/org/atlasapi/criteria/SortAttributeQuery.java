package org.atlasapi.criteria;

import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.Operator;

public class SortAttributeQuery extends AttributeQuery<String> {

    public SortAttributeQuery(Attribute<String> attribute,
            Operator op, Iterable<String> values) {
        super(attribute, op, values);
    }

    @Override
    public <V> V accept(QueryVisitor<V> v) {
        return v.visit(this);
    }
}
