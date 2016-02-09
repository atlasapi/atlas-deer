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
