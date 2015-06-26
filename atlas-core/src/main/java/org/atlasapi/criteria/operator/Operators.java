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

package org.atlasapi.criteria.operator;

import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.collect.Maps;

public class Operators {

    public static final Equals EQUALS = new Equals();
    public static final Beginning BEGINNING = new Beginning();
    public static final Ascending ASCENDING = new Ascending();
    public static final Descending DESCENDING = new Descending();

    public static final ComparableOperator GREATER_THAN = new GreaterThan();
    public static final ComparableOperator LESS_THAN = new LessThan();

    public static final Before BEFORE = new Before();
    public static final After AFTER = new After();

    private static final Map<String, Operator> lookupTable = createLookup();

    public static Operator lookup(String name) {
        return lookupTable.get(name);
    }

    private static Map<String, Operator> createLookup() {
        Map<String, Operator> table = Maps.newHashMap();
        table.put(EQUALS.name(), EQUALS);
        table.put(BEGINNING.name(), BEGINNING);
        table.put(LESS_THAN.name(), LESS_THAN);
        table.put(GREATER_THAN.name(), GREATER_THAN);
        table.put(BEFORE.name(), BEFORE);
        table.put(AFTER.name(), AFTER);
        return table;
    }

    private static class BaseOperator {

        private final String name;

        public BaseOperator(String name) {
            this.name = name;
        }

        public final String name() {
            return name;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof BaseOperator) {
                return name.equals(((BaseOperator) obj).name);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class Equals extends BaseOperator implements
            StringOperator, ComparableOperator, EqualsOperator, DateTimeOperator {

        private Equals() {
            super("equals");
        }

        public <V> V accept(StringOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public <V> V accept(ComparableOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public <V> V accept(EqualsOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return lhs.isAssignableFrom(rhs);
        }

        @Override
        public <V> V accept(DateTimeOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

    }

    public static class Beginning extends BaseOperator implements StringOperator {

        private Beginning() {
            super("beginning");
        }

        public <V> V accept(StringOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return String.class.equals(lhs) && String.class.equals(rhs);
        }
    }

    public static class Before extends BaseOperator implements DateTimeOperator {

        private Before() {
            super("before");
        }

        public <V> V accept(DateTimeOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return DateTime.class.equals(lhs) && DateTime.class.equals(rhs);
        }
    }

    public static class After extends BaseOperator implements DateTimeOperator {

        private After() {
            super("after");
        }

        public <V> V accept(DateTimeOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return DateTime.class.equals(lhs) && DateTime.class.equals(rhs);
        }
    }

    public static class GreaterThan extends BaseOperator implements ComparableOperator {

        private GreaterThan() {
            super("greaterThan");
        }

        public <V> V accept(ComparableOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return Number.class.isAssignableFrom(lhs) && Number.class.isAssignableFrom(rhs);
        }
    }

    public static class LessThan extends BaseOperator implements ComparableOperator {

        private LessThan() {
            super("lessThan");
        }

        public <V> V accept(ComparableOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return Number.class.isAssignableFrom(lhs) && Number.class.isAssignableFrom(rhs);
        }
    }

    public static class Ascending extends BaseOperator implements StringOperator, ComparableOperator, DateTimeOperator {

        private Ascending() {
            super("asc");
        }

        @Override public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return true;
        }

        @Override public <V> V accept(DateTimeOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        @Override public <V> V accept(StringOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        @Override public <V> V accept(ComparableOperatorVisitor<V> v) {
            return v.visit(this);
        }
    }

    public static class Descending extends BaseOperator implements StringOperator, ComparableOperator, DateTimeOperator {

        private Descending() {
            super("desc");
        }

        @Override public boolean canBeAppliedTo(Class<?> lhs, Class<?> rhs) {
            return true;
        }

        @Override public <V> V accept(StringOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        @Override public <V> V accept(DateTimeOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }

        @Override public <V> V accept(ComparableOperatorVisitor<V> visitor) {
            return visitor.visit(this);
        }
    }
}
