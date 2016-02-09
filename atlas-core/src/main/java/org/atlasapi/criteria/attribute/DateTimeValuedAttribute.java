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
import org.atlasapi.criteria.DateTimeAttributeQuery;
import org.atlasapi.criteria.operator.DateTimeOperator;
import org.atlasapi.criteria.operator.Operator;
import org.atlasapi.entity.Identified;

import org.joda.time.DateTime;

public class DateTimeValuedAttribute extends Attribute<DateTime> {

    public DateTimeValuedAttribute(String name, Class<? extends Identified> target) {
        super(name, target);
    }

    @Override
    public String toString() {
        return "DateTime valued attribute: " + name;
    }

    @Override
    public AttributeQuery<DateTime> createQuery(Operator op, Iterable<DateTime> values) {
        if (!(op instanceof DateTimeOperator)) {
            throw new IllegalArgumentException();
        }
        return new DateTimeAttributeQuery(this, (DateTimeOperator) op, values);
    }

    @Override
    public Class<DateTime> requiresOperandOfType() {
        return DateTime.class;
    }
}