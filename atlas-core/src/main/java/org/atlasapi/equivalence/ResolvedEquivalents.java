package org.atlasapi.equivalence;

import com.google.common.base.Predicates;
import com.google.common.collect.ForwardingSetMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Represents a group of resolved sets of equivalents.
 *
 * @param <E>
 */
public class ResolvedEquivalents<E extends Equivalable<E>> extends ForwardingSetMultimap<Id, E> {

    public static <E extends Equivalable<E>> Builder<E> builder() {
        return new Builder<E>();
    }

    public static class Builder<E extends Equivalable<E>> {

        private ImmutableSetMultimap.Builder<Id, E> entries = ImmutableSetMultimap.builder();

        public Builder<E> putEquivalents(Id key, Iterable<? extends E> equivalentSet) {
            this.entries.putAll(key, setEquivalentToFields(equivalentSet));
            return this;
        }

        public ResolvedEquivalents<E> build() {
            return new ResolvedEquivalents<>(entries.build());
        }

        private Iterable<E> setEquivalentToFields(Iterable<? extends E> equivalents) {

            Map<Id, EquivalenceRef> refMap = StreamSupport.stream(equivalents.spliterator(), true)
                    .collect(Collectors.toMap(Identifiable::getId, EquivalenceRef::valueOf));

            Set<EquivalenceRef> allRefs = ImmutableSet.copyOf(refMap.values());

            ImmutableSet.Builder<E> equivContents = ImmutableSet.builder();
            for (E equivalent : equivalents) {
                EquivalenceRef ref = refMap.get(equivalent.getId());
                Set<EquivalenceRef> equivs = Sets.filter(
                        Sets.union(equivalent.getEquivalentTo(), allRefs),
                        Predicates.not(Predicates.equalTo(ref))
                );
                equivContents.add(equivalent.copyWithEquivalentTo(equivs));
            }
            return equivContents.build();
        }

    }

    private SetMultimap<Id, E> entries;

    private ResolvedEquivalents(SetMultimap<Id, E> entries) {
        this.entries = ImmutableSetMultimap.copyOf(entries);
    }

    @Override
    protected SetMultimap<Id, E> delegate() {
        return entries;
    }

    @Override
    public ImmutableSet<E> get(@Nullable Id key) {
        return (ImmutableSet<E>) super.get(key);
    }

    public final Iterable<E> getFirstElems() {
        return asMap().values()
                .stream()
                .map(input -> input.iterator().next())
                .collect(Collectors.toList());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final ResolvedEquivalents<?> EMPTY_INSTANCE
            = new ResolvedEquivalents(ImmutableSetMultimap.<Id, Object>of());

    @SuppressWarnings("unchecked")
    public static <E extends Equivalable<E>> ResolvedEquivalents<E> empty() {
        return (ResolvedEquivalents<E>) EMPTY_INSTANCE;
    }
}
