package org.atlasapi.criteria;

public interface QueryVisitor<T> {

    T visit(IntegerAttributeQuery query);

    T visit(StringAttributeQuery query);

    T visit(BooleanAttributeQuery query);

    T visit(EnumAttributeQuery<?> query);

    T visit(DateTimeAttributeQuery dateTimeAttributeQuery);

    T visit(MatchesNothing noOp);

    T visit(IdAttributeQuery query);

    T visit(FloatAttributeQuery query);

    T visit(SortAttributeQuery query);

}
