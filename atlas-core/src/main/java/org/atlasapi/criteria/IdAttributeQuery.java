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
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.ComparableOperatorVisitor;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.EqualsOperatorVisitor;
import org.atlasapi.criteria.operator.StringOperator;
import org.atlasapi.criteria.operator.StringOperatorVisitor;
import org.atlasapi.entity.Id;

public class IdAttributeQuery extends AttributeQuery<Id> {

    private final EqualsOperator op;

    public IdAttributeQuery(Attribute<Id> attribute, EqualsOperator op, Iterable<Id> values) {
        super(attribute, op, values);
        this.op = op;
    }

    public <V> V accept(QueryVisitor<V> visitor) {
        return visitor.visit(this);
    }

    public <V> V accept(EqualsOperatorVisitor<V> v) {
        return op.accept(v);
    }

    public EqualsOperator getOperator() {
        return op;
    }

}
