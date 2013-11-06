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

import org.atlasapi.content.Identified;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.IntegerAttributeQuery;
import org.atlasapi.criteria.operator.ComparableOperator;
import org.atlasapi.criteria.operator.Operator;

public class IntegerValuedAttribute extends Attribute<Integer> {

	IntegerValuedAttribute(String name, Class<? extends Identified> target) {
		super(name, target);
	}
	
	public IntegerValuedAttribute(String name, Class<? extends Identified> target, boolean isCollection) {
		super(name, target, isCollection);
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