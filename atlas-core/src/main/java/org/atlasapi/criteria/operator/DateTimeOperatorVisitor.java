package org.atlasapi.criteria.operator;

import org.atlasapi.criteria.operator.Operators.After;
import org.atlasapi.criteria.operator.Operators.Ascending;
import org.atlasapi.criteria.operator.Operators.Before;
import org.atlasapi.criteria.operator.Operators.Descending;

public interface DateTimeOperatorVisitor<V> extends EqualsOperatorVisitor<V> {

    V visit(Before before);

    V visit(After after);

    V visit(Ascending asc);

    V visit(Descending desc);

}
