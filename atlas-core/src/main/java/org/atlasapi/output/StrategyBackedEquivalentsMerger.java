package org.atlasapi.output;

import java.util.List;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.equivalence.Equivalable;

import com.metabroadcast.applications.client.model.internal.Application;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import static com.google.common.base.Preconditions.checkNotNull;

public class StrategyBackedEquivalentsMerger<E extends Equivalable<E>>
        implements ApplicationEquivalentsMerger<E> {

    private final EquivalentsMergeStrategy<E> strategy;
    @VisibleForTesting public static final Ordering<Equivalable> ID_ORDERING =
            Ordering.natural().onResultOf(Equivalable::getId);

    public StrategyBackedEquivalentsMerger(EquivalentsMergeStrategy<E> strategy) {
        this.strategy = checkNotNull(strategy);
    }

    @Override
    public <T extends E> List<T> merge(final Optional<Id> id, Iterable<T> equivalents,
            Application application, Set<Annotation> activeAnnotations) {
        if (!application.getConfiguration().isPrecedenceEnabled()) {
            return ImmutableList.copyOf(equivalents);
        }
        // order content by source precedence, then by id (lowest/oldest first)
        Ordering<Sourced> publisherOrdering = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourced::getSource);
        Ordering<T> equivsOrdering = publisherOrdering.compound(ID_ORDERING);
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

        // If the query asks for a specific ID and that ID is from the highest precedence, then that
        // ID becomes the highest precedence piece of content. Relevant only if there are multiple
        // IDs from the highest precedence source.
        // Furthermore, at the time of this rework it is unclear if anything relies on this logic,
        // but nothing built in the last 3 years relies on it. It is the view of the current team
        // this logic is wrong and should be removed as it causes the result of the merge to be
        // different for different IDs of the equiv set.
        if (id.isPresent()) {
            Optional<T> requested = Iterables.tryFind(equivalents, idIs(id.get()));

            if (requested.isPresent()
                    && chosen.getSource().equals(requested.get().getSource())) {
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
