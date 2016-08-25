package org.atlasapi.equivalence;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a set of equivalent resources and how they link together through an adjacency
 * list
 */
public final class EquivalenceGraph implements Identifiable {

    private final ImmutableMap<Id, Adjacents> adjacencyList;
    private final DateTime updated;
    private final Id id;

    public EquivalenceGraph(Map<Id, Adjacents> adjacencyList, DateTime updated) {
        this.adjacencyList = ImmutableMap.copyOf(adjacencyList);
        this.updated = checkNotNull(updated);
        this.id = Ordering.natural().min(adjacencyList.keySet());
    }

    public static EquivalenceGraph valueOf(Set<Adjacents> set) {
        return new EquivalenceGraph(
                Maps.uniqueIndex(
                        set,
                        Identifiables.toId()
                ),
                new DateTime(DateTimeZones.UTC)
        );
    }

    public static EquivalenceGraph valueOf(ResourceRef subj) {
        return new EquivalenceGraph(
                ImmutableMap.of(
                        subj.getId(), Adjacents.valueOf(subj)
                ),
                new DateTime(DateTimeZones.UTC)
        );
    }

    @Override
    public Id getId() {
        return id;
    }

    public ImmutableSet<Id> getEquivalenceSet() {
        return adjacencyList.keySet();
    }

    public DateTime getUpdated() {
        return updated;
    }

    public Adjacents getAdjacents(Id id) {
        return adjacencyList.get(id);
    }

    public Adjacents getAdjacents(Identifiable identifiable) {
        return adjacencyList.get(identifiable.getId());
    }

    public Map<Id, Adjacents> getAdjacencyList() {
        return adjacencyList;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalenceGraph) {
            EquivalenceGraph other = (EquivalenceGraph) that;
            return adjacencyList.equals(other.adjacencyList)
                    && updated.equals(other.updated);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return updated.hashCode();
    }

    @Override
    public String toString() {
        return adjacencyList.toString();
    }

    /**
     * An entry in an equivalence adjacency list. A subject resource reference and sets of
     * references:
     * <ul>
     *     <li><i>efferent</i> - resources asserted as equivalent to the subject.</li>
     *     <li><i>afferent</i> - resources which asserted the subject as equivalent.</li>
     * </ul>
     */
    public static final class Adjacents implements Identifiable, Sourced {

        private final ResourceRef subject;
        private final DateTime created;
        private final ImmutableMap<Id, ResourceRef> efferent;
        private final ImmutableMap<Id, ResourceRef> afferent;

        public Adjacents(
                ResourceRef subject,
                DateTime created,
                Set<ResourceRef> efferent,
                Set<ResourceRef> afferent
        ) {
            this.subject = checkNotNull(subject);
            this.created = checkNotNull(created);

            Map<Id, ResourceRef> efferentMap = new HashMap<>();
            for (ResourceRef resourceRef : efferent) {
                efferentMap.put(resourceRef.getId(), resourceRef);
            }
            this.efferent = ImmutableMap.copyOf(efferentMap);

            Map<Id, ResourceRef> afferentMap = new HashMap<>();
            for (ResourceRef resourceRef : afferent) {
                afferentMap.put(resourceRef.getId(), resourceRef);
            }
            this.afferent = ImmutableMap.copyOf(afferentMap);

            checkArgument(this.efferent.containsKey(subject.getId()));
            checkArgument(this.afferent.containsKey(subject.getId()));
        }

        public static Adjacents valueOf(ResourceRef subject) {
            return new Adjacents(
                    subject,
                    new DateTime(DateTimeZones.UTC),
                    ImmutableSet.of(subject),
                    ImmutableSet.of(subject)
            );
        }

        @Override
        public Id getId() {
            return subject.getId();
        }

        @Override
        public Publisher getSource() {
            return subject.getSource();
        }

        public ResourceRef getRef() {
            return subject;
        }

        public DateTime getCreated() {
            return created;
        }

        public SetView<ResourceRef> getAdjacent() {
            return Sets.union(
                    ImmutableSet.copyOf(efferent.values()),
                    ImmutableSet.copyOf(afferent.values())
            );
        }

        public ImmutableSet<ResourceRef> getEfferent() {
            return ImmutableSet.copyOf(efferent.values());
        }

        public ImmutableSet<ResourceRef> getAfferent() {
            return ImmutableSet.copyOf(afferent.values());
        }

        public boolean hasEfferentAdjacent(ResourceRef ref) {
            return efferent.containsKey(ref.getId());
        }

        public boolean hasAfferentAdjacent(ResourceRef ref) {
            return afferent.containsKey(ref.getId());
        }

        public Adjacents copyWithEfferent(ResourceRef ref) {
            HashMap<Id, ResourceRef> map = new HashMap<>();
            map.putAll(efferent);
            map.put(ref.getId(), ref);

            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(map.values()),
                    ImmutableSet.copyOf(afferent.values())
            );
        }

        public Adjacents copyWithEfferents(Iterable<ResourceRef> refs) {
            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(refs),
                    ImmutableSet.copyOf(afferent.values())
            );
        }

        public Adjacents copyWithAfferent(ResourceRef ref) {
            HashMap<Id, ResourceRef> map = new HashMap<>();
            map.putAll(afferent);
            map.put(ref.getId(), ref);

            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(efferent.values()),
                    ImmutableSet.copyOf(map.values())
            );
        }

        public Adjacents copyWithoutAfferent(ResourceRef ref) {
            ImmutableSet<ResourceRef> collected = afferent.values()
                    .stream()
                    .filter(value -> !value.getId().equals(ref.getId()))
                    .collect(MoreCollectors.toImmutableSet());

            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(efferent.values()),
                    collected
            );
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that instanceof Adjacents) {
                Adjacents other = (Adjacents) that;
                return subject.equals(other.subject)
                        && afferent.equals(other.afferent)
                        && efferent.equals(other.efferent);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return subject.hashCode();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            appendRef(builder, subject);
            Iterator<ResourceRef> adjs = efferent.values().iterator();
            builder.append(" -> [");
            if (adjs.hasNext()) {
                appendRef(builder, adjs.next());
                while (adjs.hasNext()) {
                    builder.append(", ");
                    appendRef(builder, adjs.next());
                }
            }
            return builder.append(']').toString();
        }

        private StringBuilder appendRef(StringBuilder builder, ResourceRef ref) {
            return builder.append(ref.getId()).append('/').append(ref.getSource().key());
        }
    }
}
