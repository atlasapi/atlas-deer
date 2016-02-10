package org.atlasapi.equivalence;

import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.Sourced;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A set of equivalent resources and their graph, defining the links between them.
 *
 * @param <T> the equivalent resource type, must be {@link Identifiable} and {@link Sourced}
 */
public final class Equivalent<T extends Identifiable & Sourced> {

    private final EquivalenceGraph graph;
    private final ImmutableSet<T> resources;

    public Equivalent(EquivalenceGraph graph, Iterable<T> resources) {
        this.graph = checkNotNull(graph);
        this.resources = ImmutableSet.copyOf(resources);
    }

    public EquivalenceGraph getGraph() {
        return graph;
    }

    public ImmutableSet<T> getResources() {
        return resources;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("resources", Iterables.transform(resources, Identifiables.toId()))
                .toString();
    }
}
