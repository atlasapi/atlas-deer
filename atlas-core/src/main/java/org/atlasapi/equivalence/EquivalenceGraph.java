package org.atlasapi.equivalence;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EquivalenceGraph that = (EquivalenceGraph) o;
        return Objects.equals(adjacencyList, that.adjacencyList) &&
                Objects.equals(updated, that.updated) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(adjacencyList, updated, id);
    }

    @Override
    public String toString() {
        return adjacencyList.toString();
    }

    /**
     * An entry in an equivalence adjacency list. A subject resource reference and sets of
     * references:
     * <ul>
     *     <li><i>outgoingEdges</i> - resources asserted as equivalent to the subject.</li>
     *     <li><i>incomingEdges</i> - resources which asserted the subject as equivalent.</li>
     * </ul>
     */
    public static final class Adjacents implements Identifiable, Sourced {

        private final ResourceRef subject;
        private final DateTime created;
        private final ImmutableMap<Id, ResourceRef> outgoingEdges;
        private final ImmutableMap<Id, ResourceRef> incomingEdges;

        @JsonCreator
        public Adjacents(
                @JsonProperty("subject") ResourceRef subject,
                @JsonProperty("created") DateTime created,
                @JsonProperty("outgoingEdges") Set<ResourceRef> outgoingEdges,
                @JsonProperty("incomingEdges") Set<ResourceRef> incomingEdges
        ) {
            this.subject = checkNotNull(subject);
            this.created = checkNotNull(created);

            Map<Id, ResourceRef> outgoingEdgesMap = new HashMap<>();
            for (ResourceRef resourceRef : outgoingEdges) {
                outgoingEdgesMap.put(resourceRef.getId(), resourceRef);
            }
            this.outgoingEdges = ImmutableMap.copyOf(outgoingEdgesMap);

            Map<Id, ResourceRef> incomingEdgesMap = new HashMap<>();
            for (ResourceRef resourceRef : incomingEdges) {
                incomingEdgesMap.put(resourceRef.getId(), resourceRef);
            }
            this.incomingEdges = ImmutableMap.copyOf(incomingEdgesMap);

            checkArgument(this.outgoingEdges.containsKey(subject.getId()));
            checkArgument(this.incomingEdges.containsKey(subject.getId()));
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

        @JsonProperty("subject")
        public ResourceRef getRef() {
            return subject;
        }

        @JsonProperty("created")
        public DateTime getCreated() {
            return created;
        }

        public SetView<ResourceRef> getAdjacent() {
            return Sets.union(
                    ImmutableSet.copyOf(outgoingEdges.values()),
                    ImmutableSet.copyOf(incomingEdges.values())
            );
        }

        @JsonProperty("outgoingEdges")
        public ImmutableSet<ResourceRef> getOutgoingEdges() {
            return ImmutableSet.copyOf(outgoingEdges.values());
        }

        @JsonProperty("incomingEdges")
        public ImmutableSet<ResourceRef> getIncomingEdges() {
            return ImmutableSet.copyOf(incomingEdges.values());
        }

        public boolean hasOutgoingAdjacent(ResourceRef ref) {
            return outgoingEdges.containsKey(ref.getId());
        }

        public boolean hasIncomingAdjacent(ResourceRef ref) {
            return incomingEdges.containsKey(ref.getId());
        }

        public Adjacents copyWithOutgoing(ResourceRef ref) {
            HashMap<Id, ResourceRef> map = new HashMap<>();
            map.putAll(outgoingEdges);
            map.put(ref.getId(), ref);

            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(map.values()),
                    ImmutableSet.copyOf(incomingEdges.values())
            );
        }

        public Adjacents copyWithOutgoing(Iterable<ResourceRef> refs) {
            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(refs),
                    ImmutableSet.copyOf(incomingEdges.values())
            );
        }

        public Adjacents copyWithIncoming(ResourceRef ref) {
            HashMap<Id, ResourceRef> map = new HashMap<>();
            map.putAll(incomingEdges);
            map.put(ref.getId(), ref);

            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(outgoingEdges.values()),
                    ImmutableSet.copyOf(map.values())
            );
        }

        public Adjacents copyWithoutIncoming(ResourceRef ref) {
            ImmutableSet<ResourceRef> collected = incomingEdges.values()
                    .stream()
                    .filter(value -> !value.getId().equals(ref.getId()))
                    .collect(MoreCollectors.toImmutableSet());

            return new Adjacents(
                    subject,
                    created,
                    ImmutableSet.copyOf(outgoingEdges.values()),
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
                        && incomingEdges.equals(other.incomingEdges)
                        && outgoingEdges.equals(other.outgoingEdges);
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
            Iterator<ResourceRef> adjs = outgoingEdges.values().iterator();
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
