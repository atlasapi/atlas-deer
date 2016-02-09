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

import java.util.List;

import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.Operator;

import com.google.common.collect.ImmutableList;

public abstract class AttributeQuery<T> extends AtomicQuery {

    private Attribute<T> attribute;
    private List<T> values;
    private final Operator op;

    public AttributeQuery(Attribute<T> attribute, Operator op, Iterable<T> values) {
        this.op = op;
        for (Object value : values) {
            Class<?> lhs = attribute.requiresOperandOfType();
            Class<?> rhs = value.getClass();
            if (!op.canBeAppliedTo(lhs, rhs)) {
                throw new IllegalArgumentException("Wrong types for operator, lhs "
                        + lhs.getSimpleName()
                        + " found "
                        + rhs.getSimpleName());
            }
        }
        this.attribute = attribute;
        this.values = ImmutableList.copyOf(values);
    }

    public Attribute<T> getAttribute() {
        return attribute;
    }

    public void setAttribute(Attribute<T> attribute) {
        this.attribute = attribute;
    }

    public String getAttributeName() {
        return attribute.externalName();
    }

    public List<T> getValue() {
        return values;
    }

    public void setValue(List<T> value) {
        this.values = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof AttributeQuery<?>) {
            AttributeQuery<?> other = (AttributeQuery<?>) obj;
            return attribute.equals(other.attribute)
                    && op.equals(other.op)
                    && values.equals(other.values);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return attribute.hashCode() ^ op.hashCode() ^ values.hashCode();
    }

    @Override
    public String toString() {
        return "(" + getAttributeName() + " " + op.toString() + " " + values + ")";
    }

    public Operator getOperator() {
        return op;
    }
}
