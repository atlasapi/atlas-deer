package org.atlasapi.output;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.metabroadcast.applications.client.model.internal.Application;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
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
        Ordering<Sourced> equivsOrdering = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourced::getSource);

        ImmutableList<T> sortedEquivalents = equivsOrdering.immutableSortedCopy(equivalents);

        if (sortedEquivalents.isEmpty()) {
            return ImmutableList.of();
        }
        if (trivialMerge(sortedEquivalents)) {
            return ImmutableList.of(
                    strategy.merge(
                            Iterables.getFirst(sortedEquivalents, null),
                            ImmutableList.of(),
                            application,
                            activeAnnotations
                    )
            );
        }
        T chosen = sortedEquivalents.get(0);

        if (id.isPresent()) {
            Optional<T> requested = Iterables.tryFind(equivalents, idIs(id.get()));

            if (requested.isPresent()
                    && chosen.getSource().equals(requested.get().getSource())) {
                chosen = requested.get();
            }
        }

        return ImmutableList.of(strategy.merge(chosen,
                new HashSet<>(sortedEquivalents), application, activeAnnotations));
    }

    private boolean trivialMerge(ImmutableList<?> sortedEquivalents) {
        return sortedEquivalents.size() == 1;
    }

    private Predicate<Identifiable> idIs(final Id id) {
        return input -> input.getId().equals(id);
    }

}
