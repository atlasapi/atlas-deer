package org.atlasapi.output;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.metabroadcast.applications.client.model.internal.Application;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Described;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Sourced;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.equivalence.Equivalable;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import static com.google.common.base.Preconditions.checkNotNull;

public class StrategyBackedEquivalentsMerger<E extends Equivalable<E>>
        implements ApplicationEquivalentsMerger<E> {

    private final EquivalentsMergeStrategy<E> strategy;

    public StrategyBackedEquivalentsMerger(EquivalentsMergeStrategy<E> strategy) {
        this.strategy = checkNotNull(strategy);
    }

    @Override
    public <T extends E> List<T> merge(final Optional<Id> id, Iterable<T> equivalents,
            Application application, Set<Annotation> activeAnnotations) {
        if (!application.getConfiguration().isPrecedenceEnabled()) {
            return ImmutableList.copyOf(equivalents);
        }
        Ordering<Sourced> publisherOrdering = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourced::getSource);
        Ordering<T> idOrdering = Ordering.natural().onResultOf(Equivalable::getId);
        Ordering<T> equivsOrdering = publisherOrdering.compound(idOrdering);

        List<T> sortedEquivalents = equivsOrdering.sortedCopy(equivalents);

        if (sortedEquivalents.isEmpty()) {
            return ImmutableList.of();
        }
        if (trivialMerge(sortedEquivalents)) {
            return ImmutableList.of(
                    strategy.merge(
                            sortedEquivalents,
                            application,
                            activeAnnotations
                    )
            );
        }
        T chosen = sortedEquivalents.get(0);

        if (id.isPresent()) {
            //TODO: find by index, then get it from the index and remove it, and then move it to the start
            Optional<T> requested = Iterables.tryFind(equivalents, idIs(id.get()));

            if (requested.isPresent()
                    && chosen.getSource().equals(requested.get().getSource())) {
                //TODO: here, mess with sortedEquivalents order so that requested becomes first
                chosen = requested.get();
                sortedEquivalents.remove(requested.get());
                sortedEquivalents.add(0, requested.get());
            }
        }

        return ImmutableList.of(strategy.merge(sortedEquivalents, application, activeAnnotations));
    }

    private boolean trivialMerge(List<?> sortedEquivalents) {
        return sortedEquivalents.size() == 1;
    }

    private Predicate<Identifiable> idIs(final Id id) {
        return input -> input.getId().equals(id);
    }

}
