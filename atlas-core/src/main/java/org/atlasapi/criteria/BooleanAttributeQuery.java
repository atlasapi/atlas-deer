/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.criteria;

import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.EqualsOperatorVisitor;

public class BooleanAttributeQuery extends AttributeQuery<Boolean> {

    private final EqualsOperator op;

    public BooleanAttributeQuery(Attribute<Boolean> attribute, EqualsOperator op,
            Iterable<Boolean> values) {
        super(attribute, op, values);
        this.op = op;
    }

    public <V> V accept(QueryVisitor<V> v) {
        return v.visit(this);
    }

    public <V> V accept(EqualsOperatorVisitor<V> v) {
        return op.accept(v);
    }

    public boolean isUnconditionallyTrue() {
        return getValue().contains(Boolean.TRUE)
                && getValue().contains(Boolean.FALSE);
    }
}
