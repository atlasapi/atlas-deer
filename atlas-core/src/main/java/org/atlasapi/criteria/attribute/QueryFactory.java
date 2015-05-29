package org.atlasapi.criteria.attribute;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.operator.Operator;

public interface QueryFactory<T> {

	AttributeQuery<T> createQuery(Operator op, Iterable<T> values);

	Class<T> requiresOperandOfType();

}
