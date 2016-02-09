package org.atlasapi.entity.util;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public final class Resolved<R extends Identifiable> {

    private static final Resolved<Identifiable> EMPTY = valueOf(ImmutableList.<Identifiable>of());

    @SuppressWarnings("unchecked")
    public static final <R extends Identifiable> Resolved<R> empty() {
        return (Resolved<R>) EMPTY;
    }

    public static final <R extends Identifiable> Resolved<R> valueOf(Iterable<R> resources) {
        return new Resolved<R>(resources);
    }

    private final FluentIterable<R> resources;
    private OptionalMap<Id, R> toMap;

    public Resolved(Iterable<R> resources) {
        this.resources = FluentIterable.from(resources);
    }

    public FluentIterable<R> getResources() {
        return resources;
    }

    public OptionalMap<Id, R> toMap() {
        if (toMap == null) {
            toMap = ImmutableOptionalMap.fromMap(
                    resources.uniqueIndex(Identifiables.toId()));
        }
        return toMap;
    }

    @Override
    public String toString() {
        return resources.toString();
    }

}
