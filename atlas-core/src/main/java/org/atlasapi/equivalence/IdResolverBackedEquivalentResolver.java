package org.atlasapi.equivalence;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.Sourced;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;

public class IdResolverBackedEquivalentResolver<E extends Equivalent<E>>
        implements EquivalentsResolver<E> {

    public static final <E extends Equivalent<E>> EquivalentsResolver<E> 
            valueOf(EquivalenceRecordStore store, IdResolver<E> resolver) {
        return new IdResolverBackedEquivalentResolver<E>(store, resolver);
    }
    
    private final EquivalenceRecordStore store;
    private final IdResolver<E> resolver;

    public IdResolverBackedEquivalentResolver(EquivalenceRecordStore store,
            IdResolver<E> resolver) {
        this.store = checkNotNull(store);
        this.resolver = checkNotNull(resolver);
    }

    @Override
    public ListenableFuture<ResolvedEquivalents<E>> resolveIds(Iterable<Id> ids, Set<Publisher> selectedSources,
            Set<Annotation> activeAnnotations) {
        final ImmutableSet<Id> uniqueIds = ImmutableSet.copyOf(ids);
        final Predicate<Sourced> sourceFilter = Sourceds.sourceFilter(selectedSources);

        final OptionalMap<Id, EquivalenceRecord> records = store.resolveRecords(uniqueIds);
        
        Set<Id> idsToResolve = idsToResolve(uniqueIds, records, sourceFilter);

        return Futures.transform(resolver.resolveIds(idsToResolve),
            new Function<Resolved<E>, ResolvedEquivalents<E>>() {
                @Override
                public ResolvedEquivalents<E> apply(Resolved<E> input) {
                    return buildEquivalents(uniqueIds, sourceFilter, records, input.toMap()).build();
                }
        });
    }

    private ResolvedEquivalents.Builder<E> buildEquivalents(ImmutableSet<Id> uniqueIds,
            Predicate<Sourced> sourceFilter, OptionalMap<Id, EquivalenceRecord> records,
            OptionalMap<Id, E> resolvedResources) {
        
        Function<Identifiable, Optional<E>> resolveRef =
                Functions.compose(Functions.forMap(resolvedResources), Identifiables.toId());

        ResolvedEquivalents.Builder<E> equivalents = ResolvedEquivalents.builder();
        for (Id id : uniqueIds) {
            Iterable<E> equivalent = null;
            Optional<EquivalenceRecord> resolvedRecord = records.get(id);
            if (resolvedRecord.isPresent()) {
                List<EquivalenceRef> equivs = sortRequestedFirst(id, resolvedRecord.get().getEquivalents());
                equivalent = Optional.presentInstances(Iterables.transform(equivs, resolveRef));
            } else {
                Optional<E> resolvedResource = resolvedResources.get(id);
                if (resolvedResource.isPresent()) {
                    E resource = resolvedResource.get();
                    equivalent = ImmutableList.of(resource);
                }
            }
            if (equivalent != null) {
                equivalents.putEquivalents(id, Iterables.filter(equivalent, sourceFilter));
            }
        }
        return equivalents;
    }

    private List<EquivalenceRef> sortRequestedFirst(final Id id, ImmutableSet<EquivalenceRef> equivalents) {
        return Ordering.from(new Comparator<Id>() {
            @Override
            public int compare(Id o1, Id o2) {
                //id before other, order of others undefined.
                return o1.equals(id) ? -1 : 0;
            }
        }).onResultOf(Identifiables.toId()).immutableSortedCopy(equivalents);
    }

    private Set<Id> idsToResolve(Set<Id> uniqueIds, OptionalMap<Id, EquivalenceRecord> records,
            Predicate<Sourced> sourceFilter) {
        Set<Id> idsToResolve = Sets.newHashSet();
        for (Id id : uniqueIds) {
            Optional<EquivalenceRecord> resolvedRecord = records.get(id);
            if (resolvedRecord.isPresent()) {
                EquivalenceRecord record = resolvedRecord.get();
                if (sourceFilter.apply(record)) {
                    idsToResolve.addAll(selectedIds(record, sourceFilter));
                }
            } else {
                /*
                 * didn't get an equivalence record for this id but it may still
                 * exist without equivalence having been run for it. Add it in
                 * case it really exists. We'll need to make sure it's from a
                 * selected source later on though.
                 */
                idsToResolve.add(id);
            }
        }
        return idsToResolve;
    }

    private Collection<Id> selectedIds(EquivalenceRecord record, Predicate<Sourced> sourceFilter) {
        ImmutableSet<EquivalenceRef> equivalents = record.getEquivalents();
        Set<EquivalenceRef> selectedRefs = Sets.filter(equivalents, sourceFilter);
        return Collections2.transform(selectedRefs, Identifiables.toId());
    }

}
