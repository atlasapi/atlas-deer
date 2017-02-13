package org.atlasapi.output;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.equivalence.Equivalable;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class StrategyBackedEquivalentsMerger<E extends Equivalable<E>>
        implements ApplicationEquivalentsMerger<E> {

    private final EquivalentsMergeStrategy<E> strategy;

    public StrategyBackedEquivalentsMerger(EquivalentsMergeStrategy<E> strategy) {
        this.strategy = checkNotNull(strategy);
    }

    @Override
    public <T extends E> List<T> merge(
            final Optional<Id> id,
            Iterable<T> equivalents,
            Application application
    ) {
        if (!application.getConfiguration().isPrecedenceEnabled()) {
            return ImmutableList.copyOf(equivalents);
        }

        Ordering<Sourced> equivsOrdering = applicationEquivalentsOrdering(application, equivalents);

        ImmutableList<T> sortedEquivalents = equivsOrdering.immutableSortedCopy(equivalents);

        if (sortedEquivalents.isEmpty()) {
            return ImmutableList.of();
        }
        if (trivialMerge(sortedEquivalents)) {
            return ImmutableList.of(
                    strategy.merge(
                            Iterables.getFirst(sortedEquivalents, null),
                            ImmutableList.of(),
                            application
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

        Iterable<T> notChosen = Iterables.filter(
                sortedEquivalents,
                Predicates.not(idIs(chosen.getId()))
        );
        return ImmutableList.of(strategy.merge(chosen, notChosen, application));
    }

    private boolean trivialMerge(ImmutableList<?> sortedEquivalents) {
        return sortedEquivalents.size() == 1;
    }

    private <T extends E> Ordering<Sourced> applicationEquivalentsOrdering(
            Application application,
            Iterable<T> equivalents
    ) {

        List<Publisher> applicationPublishers = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .sortedCopy(application.getConfiguration().getEnabledReadSources());

        List<Publisher> equivPublishers = StreamSupport.stream(equivalents.spliterator(), false)
                .map(Sourced::getSource)
                .filter(publisher -> !applicationPublishers.contains(publisher))
                .collect(Collectors.toList());

        return Ordering.explicit(
                ImmutableList.<Publisher>builder()
                        .addAll(applicationPublishers)
                        .addAll(equivPublishers)
                        .build()
        ).onResultOf(Sourceds.toPublisher());
    }

    private Predicate<Identifiable> idIs(final Id id) {
        return input -> input.getId().equals(id);
    }

}
