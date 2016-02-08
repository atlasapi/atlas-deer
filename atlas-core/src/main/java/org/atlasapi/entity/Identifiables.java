package org.atlasapi.entity;

import java.util.Collection;

import com.metabroadcast.common.base.MorePredicates;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Ordering;

public final class Identifiables {

    private Identifiables() {
    }

    public static final Function<Identifiable, Id> toId() {
        return ToIdFunction.INSTANCE;
    }

    private enum ToIdFunction implements Function<Identifiable, Id> {

        INSTANCE;

        @Override
        public Id apply(Identifiable input) {
            return input.getId();
        }

        @Override
        public String toString() {
            return "toId";
        }

    }

    public static Predicate<Identifiable> idFilter(Collection<Id> ids) {
        return MorePredicates.transformingPredicate(toId(), Predicates.in(ids));
    }

    private static final Ordering<Identifiable> ORDER_BY_ID = Ordering.natural().onResultOf(toId());

    public static final Ordering<Identifiable> orderById() {
        return ORDER_BY_ID;
    }

}
