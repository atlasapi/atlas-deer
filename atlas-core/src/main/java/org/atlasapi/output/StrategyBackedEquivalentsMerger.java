package org.atlasapi.output;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.application.ApplicationSources;
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

public class StrategyBackedEquivalentsMerger<E extends Equivalable<E>>
        implements ApplicationEquivalentsMerger<E> {

    private final EquivalentsMergeStrategy<E> strategy;

    public StrategyBackedEquivalentsMerger(EquivalentsMergeStrategy<E> strategy) {
        this.strategy = checkNotNull(strategy);
    }

    @Override
    public <T extends E> List<T> merge(final Optional<Id> id, Iterable<T> equivalents, ApplicationSources sources) {
        if (!sources.isPrecedenceEnabled()) {
            return ImmutableList.copyOf(equivalents);
        }
        Ordering<Sourced> equivsOrdering = applicationEquivalentsOrdering(sources);
        ImmutableList<T> sortedEquivalents = equivsOrdering.immutableSortedCopy(equivalents);
        if (trivialMerge(sortedEquivalents)) {
            return sortedEquivalents;
        }
        T chosen = sortedEquivalents.get(0);
        
        if (id.isPresent()) {
            Optional<T> requested = Iterables.tryFind(equivalents, idIs(id.get()));
            
            if (requested.isPresent()
                    && chosen.getSource().equals(requested.get().getSource())) {
                chosen = requested.get();
            }
        }
        
        Iterable<T> notChosen = Iterables.filter(sortedEquivalents, Predicates.not(idIs(chosen.getId())));
        return ImmutableList.of(strategy.merge(chosen, notChosen, sources));
    }
    

    private boolean trivialMerge(ImmutableList<?> sortedEquivalents) {
        return sortedEquivalents.isEmpty() || sortedEquivalents.size() == 1;
    }

    private Ordering<Sourced> applicationEquivalentsOrdering(ApplicationSources sources) {
        return sources.publisherPrecedenceOrdering().onResultOf(Sourceds.toPublisher());
    }
    
    private Predicate<Identifiable> idIs(final Id id) {
        return new Predicate<Identifiable>() {

            @Override
            public boolean apply(Identifiable input) {
                return input.getId().equals(id);
            }
        };
    }

}
