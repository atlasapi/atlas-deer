package org.atlasapi.entity;

import java.util.Collection;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.metabroadcast.common.base.MorePredicates;

public final class Sourceds {

    private Sourceds() { }

    public static final Function<Sourced, Publisher> toPublisher() {
        return ToSourceFunction.INSTANCE;
    }

    private enum ToSourceFunction implements Function<Sourced, Publisher> {

        INSTANCE;

        @Override
        public Publisher apply(Sourced input) {
            return input.getSource();
        }

        @Override
        public String toString() {
            return "toPublisher";
        }

    }
    
    public static <S extends Sourced> Predicate<S> sourceFilter(Collection<Publisher> sources) {
        return MorePredicates.transformingPredicate(toPublisher(), Predicates.in(sources));
    }
    
}
