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

package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

public class EnumValuedAttribute<T extends Enum<T>> extends Attribute<T> {

    public static final <T extends Enum<T>> EnumValuedAttribute<T> valueOf(String name,
            Class<T> type, Class<? extends Identified> target,
            boolean isCollection) {
        return new EnumValuedAttribute<T>(name, type, target, isCollection);
    }

    private final Class<T> type;

    EnumValuedAttribute(String name, Class<T> type, Class<? extends Identified> target) {
        this(name, type, target, false);
    }

    public EnumValuedAttribute(String name, Class<T> type, Class<? extends Identified> target,
            boolean isCollection) {
        super(name, target, isCollection);
        this.type = type;
    }

    @Override
    public String toString() {
        return "Enum valued attribute: " + name;
    }

    @Override
    public Class<T> requiresOperandOfType() {
        return type;
    }

    public T toEnumValue(String value) {
        return Enum.valueOf(type, value);
    }

    @Override
    public AttributeQuery<T> createQuery(Operator op, Iterable<T> values) {
        if (!(op instanceof EqualsOperator)) {
            throw new IllegalArgumentException();
        }
        return new EnumAttributeQuery<T>(this, (EqualsOperator) op, values);
    }
}
