package org.atlasapi.equivalence;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p> Represents the result of processing an equivalence assertion. One graph, containing the
 * subject of the assertion will be updated, it will have either have had other graphs split out,
 * other graphs merged in, or both.</p>
 * <p>
 * <p>Graphs split out are <em>created</em>. Graphs merged in are <em>deleted</em>.</p>
 * <p>
 * <p>If an assertion only changes internal edges in a graph, i.e. the actual membership of the
 * graph is unchanged, then both created and updated are empty.</p>
 * <p>
 * <p>e.g. assuming a, b, c, d are separate to begin with:</p> <ol> <li>a -> b, c : a is updated, b,
 * c are deleted.</li> <li>a -> c, d : a is updated, b is created, d is deleted.</li> <li>a -> ∅ : a
 * is updated, c, d are created.</li> </ol>
 */
/*
 * N.B. the terms updated/created/deleted are used because they are cruddy.
 */
public class EquivalenceGraphUpdate {

    /**
     * Returns a {@link Builder} for an {@link EquivalenceGraphUpdate} based on the provided updated
     * graph.
     *
     * @param updated - the graph updated in this update.
     * @return a new {@link Builder} based on the updated graph.
     */
    public static Builder builder(EquivalenceGraph updated) {
        return new Builder(updated);
    }

    public static class Builder {

        private EquivalenceGraph updated;
        private ImmutableSet<EquivalenceGraph> created = ImmutableSet.of();
        private ImmutableSet<Id> deleted = ImmutableSet.of();

        public Builder(EquivalenceGraph updated) {
            this.updated = checkNotNull(updated);
        }

        public Builder withCreated(Iterable<EquivalenceGraph> created) {
            this.created = ImmutableSet.copyOf(created);
            return this;
        }

        public Builder withDeleted(Iterable<Id> deleted) {
            this.deleted = ImmutableSet.copyOf(deleted);
            return this;
        }

        public EquivalenceGraphUpdate build() {
            return new EquivalenceGraphUpdate(updated, created, deleted);
        }

    }

    private final EquivalenceGraph updated;
    private final ImmutableSet<EquivalenceGraph> created;
    private final ImmutableSet<Id> deleted;

    public EquivalenceGraphUpdate(EquivalenceGraph updated,
            Iterable<EquivalenceGraph> created,
            Iterable<Id> deleted) {
        this.updated = checkNotNull(updated);
        this.created = ImmutableSet.copyOf(created);
        this.deleted = ImmutableSet.copyOf(deleted);
    }

    /**
     * Returns the graph updated in this update, containing the subject of the assertion that caused
     * this update.
     */
    public EquivalenceGraph getUpdated() {
        return updated;
    }

    /**
     * Returns the graphs created in this update because they've been split out of the updated
     * graph
     */
    public ImmutableSet<EquivalenceGraph> getCreated() {
        return created;
    }

    /**
     * Returns all the graphs resulting from this update.
     */
    public ImmutableSet<EquivalenceGraph> getAllGraphs() {
        return ImmutableSet.<EquivalenceGraph>builder()
                .add(updated)
                .addAll(created)
                .build();
    }

    /**
     * Returns the graphs deleted in this update because they've been merged into the updated graph
     */
    public ImmutableSet<Id> getDeleted() {
        return deleted;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalenceGraphUpdate) {
            EquivalenceGraphUpdate other = (EquivalenceGraphUpdate) that;
            return updated.equals(other.updated)
                    && created.equals(other.created)
                    && deleted.equals(other.deleted);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return updated.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("updated", updated.getId())
                .add("created", Iterables.transform(created, Identifiables.toId()))
                .add("deleted", deleted)
                .toString();
    }

}
