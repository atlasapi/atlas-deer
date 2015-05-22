package org.atlasapi.equivalence;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.metabroadcast.common.collect.MoreSets;
import com.metabroadcast.common.time.DateTimeZones;

/**
 * <p>
 * Represents a set of equivalent resources and how they link together through
 * an adjacency list.
 * </p>
 */
public final class EquivalenceGraph implements Identifiable {
    
    /**
     * <p>
     * An entry in an equivalence adjacency list. A subject resource reference
     * and sets of references:
     * <ul>
     * <li><i>efferent</i> - resources asserted as equivalent to the subject.</li>
     * <li><i>afferent</i> - resources which asserted the subject as equivalent.</li>
     * </p>
     */
    public static final class Adjacents implements Identifiable, Sourced {
        
        public static final Adjacents valueOf(ResourceRef subject) {
            ImmutableSet<ResourceRef> adjacents = ImmutableSet.of(subject);
            DateTime now = new DateTime(DateTimeZones.UTC);
            return new Adjacents(subject, now, adjacents, adjacents);
        }
        
        private final ResourceRef subject;
        private final DateTime created;
        private final ImmutableSet<ResourceRef> efferent;
        private final ImmutableSet<ResourceRef> afferent;
        
        public Adjacents(ResourceRef subject, DateTime created, Set<ResourceRef> efferent, Set<ResourceRef> afferent) {
            this.subject = checkNotNull(subject);
            this.created = checkNotNull(created);
            checkArgument(efferent.contains(subject));
            checkArgument(afferent.contains(subject));
            this.efferent = ImmutableSet.copyOf(Identifiables.orderById().immutableSortedCopy(efferent));
            this.afferent = ImmutableSet.copyOf(Identifiables.orderById().immutableSortedCopy(afferent));
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
            return Sets.union(efferent, afferent);
        }
        
        public SetView<ResourceRef> getStrongAdjacent() {
            return Sets.intersection(efferent, afferent);
        }
        
        public ImmutableSet<ResourceRef> getEfferent() {
            return efferent;
        }

        public ImmutableSet<ResourceRef> getAfferent() {
            return afferent;
        }

        public boolean hasEfferentAdjacent(ResourceRef ref) {
            return efferent.contains(ref);
        }

        public boolean hasAfferentAdjacent(ResourceRef ref) {
            return afferent.contains(ref);
        }
        
        public Adjacents copyWithEfferent(ResourceRef ref) {
            return new Adjacents(subject, created, MoreSets.add(efferent, ref), afferent);
        }
        
        public Adjacents copyWithEfferents(Iterable<ResourceRef> refs) {
            return new Adjacents(subject, created, ImmutableSet.copyOf(refs), afferent);
        }

        public Adjacents copyWithAfferent(ResourceRef ref) {
            return new Adjacents(subject, created, efferent, MoreSets.add(afferent, ref));
        }
        
        public Adjacents copyWithoutAfferent(ResourceRef ref) {
            return new Adjacents(subject, created, efferent, 
                    Sets.filter(afferent, Predicates.not(Predicates.equalTo(ref))));
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
            Iterator<ResourceRef> adjs = efferent.iterator();
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

    public static EquivalenceGraph valueOf(Set<Adjacents> set) {
        DateTime now = new DateTime(DateTimeZones.UTC);
        return new EquivalenceGraph(Maps.uniqueIndex(set, Identifiables.toId()), now);
    }

    public static EquivalenceGraph valueOf(ResourceRef subj) {
        DateTime now = new DateTime(DateTimeZones.UTC);
        Adjacents adjacents = Adjacents.valueOf(subj);
        return new EquivalenceGraph(ImmutableMap.of(subj.getId(), adjacents), now);
    }

    private final ImmutableMap<Id, Adjacents> adjacencyList;
    private final DateTime updated;
    private final Id id;
    
    public EquivalenceGraph(Map<Id, Adjacents> adjacencyList, DateTime updated) {
        this.adjacencyList = ImmutableMap.copyOf(adjacencyList);
        this.updated = checkNotNull(updated);
        this.id = Ordering.natural().min(adjacencyList.keySet());
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
    
    public Adjacents getAdjacents(Identifiable idable) {
        return adjacencyList.get(idable.getId());
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
}
