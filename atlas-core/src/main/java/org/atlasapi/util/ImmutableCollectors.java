package org.atlasapi.util;

import java.util.stream.Collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ImmutableCollectors {

    public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>> toList() {
        return Collector.of(
                ImmutableList.Builder::new,
                ImmutableList.Builder::add,
                (b1, b2) -> b1.addAll(b2.build()),
                ImmutableList.Builder::build
        );
    }

    public static <T> Collector<T, ImmutableSet.Builder<T>, ImmutableSet<T>> toSet() {
        return Collector.of(
                ImmutableSet.Builder::new,
                (builder, e) -> builder.add(e),
                (b1, b2) -> b1.addAll(b2.build()),
                (builder) -> builder.build()
        );
    }
}
