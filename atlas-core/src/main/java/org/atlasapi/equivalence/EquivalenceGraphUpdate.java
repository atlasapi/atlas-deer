package org.atlasapi.equivalence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.entity.Id;

import javax.annotation.Nullable;
import java.util.Objects;

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
 * <p>
 * N.B. the terms updated/created/deleted are used because they are cruddy.
 * <p>
 * This update also contains the equivalence assertion that triggered the graph recomputation.
 */
public class EquivalenceGraphUpdate {

    private final EquivalenceGraph updated;
    private final ImmutableSet<EquivalenceGraph> created;
    private final ImmutableSet<Id> deleted;
    private final EquivalenceAssertion assertion;

    // Visible for Jackson
    @SuppressWarnings("WeakerAccess")
    @JsonCreator
    EquivalenceGraphUpdate(
            @JsonProperty("updated") EquivalenceGraph updated,
            @JsonProperty("created") Iterable<EquivalenceGraph> created,
            @JsonProperty("deleted") Iterable<Id> deleted,
            @Nullable @JsonProperty("assertion") EquivalenceAssertion assertion
    ) {
        this.updated = checkNotNull(updated);
        this.created = ImmutableSet.copyOf(created);
        this.deleted = ImmutableSet.copyOf(deleted);
        this.assertion = assertion;
    }

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

    public Builder copy() {
        return new Builder(this);
    }

    /**
     * Returns the graph updated in this update, containing the subject of the assertion that caused
     * this update.
     */
    @JsonProperty("updated")
    public EquivalenceGraph getUpdated() {
        return updated;
    }

    /**
     * Returns the graphs created in this update because they've been split out of the updated
     * graph
     */
    @JsonProperty("created")
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
    @JsonProperty("deleted")
    public ImmutableSet<Id> getDeleted() {
        return deleted;
    }

    /**
     * This value may be null because older messages do not contain this data.
     *
     * @return the equivalence assertion that triggered the graph recomputation.
     */
    @Nullable
    @JsonProperty("assertion")
    public EquivalenceAssertion getAssertion() {
        return assertion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EquivalenceGraphUpdate that = (EquivalenceGraphUpdate) o;
        return Objects.equals(updated, that.updated) &&
                Objects.equals(created, that.created) &&
                Objects.equals(deleted, that.deleted) &&
                Objects.equals(assertion, that.assertion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updated, created, deleted, assertion);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("updated", updated)
                .add("created", created)
                .add("deleted", deleted)
                .add("assertion", assertion)
                .toString();
    }

    public static class Builder {

        private EquivalenceGraph updated;
        private ImmutableSet<EquivalenceGraph> created = ImmutableSet.of();
        private ImmutableSet<Id> deleted = ImmutableSet.of();
        private EquivalenceAssertion assertion;

        private Builder(EquivalenceGraph updated) {
            this.updated = checkNotNull(updated);
        }

        private Builder(EquivalenceGraphUpdate update) {
            this.updated = update.getUpdated();
            this.created = update.getCreated();
            this.deleted = update.getDeleted();
            this.assertion = update.getAssertion();
        }

        public Builder withCreated(Iterable<EquivalenceGraph> created) {
            this.created = ImmutableSet.copyOf(created);
            return this;
        }

        public Builder withDeleted(Iterable<Id> deleted) {
            this.deleted = ImmutableSet.copyOf(deleted);
            return this;
        }

        public Builder withAssertion(EquivalenceAssertion equivalenceAssertion) {
            this.assertion = equivalenceAssertion;
            return this;
        }

        public EquivalenceGraphUpdate build() {
            return new EquivalenceGraphUpdate(
                    updated, created, deleted, assertion
            );
        }
    }
}
