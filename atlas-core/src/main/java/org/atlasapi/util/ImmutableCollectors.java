package org.atlasapi.util;

import java.util.function.Function;
import java.util.stream.Collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
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
                ImmutableSet.Builder::add,
                (b1, b2) -> b1.addAll(b2.build()),
                ImmutableSet.Builder::build
        );
    }

    public static <T, K, V> Collector<T, ImmutableMap.Builder<K, V>, ImmutableMap<K, V>> toMap(
            Function<T, K> keyFunction, Function<T, V> valueFunction) {
        return Collector.of(
                ImmutableMap.Builder::new,
                (builder, t) -> builder.put(
                        keyFunction.apply(t), valueFunction.apply(t)
                ),
                (b1, b2) -> b1.putAll(b2.build()),
                ImmutableMap.Builder::build
        );
    }

    public static <T, K, V> Collector<T, ImmutableMultimap.Builder<K, V>, ImmutableMultimap<K, V>> toMultiMap(
            Function<T, K> keyFunction, Function<T, V> valueFunction) {
        return Collector.of(
                ImmutableMultimap.Builder::new,
                (builder, t) -> builder.put(
                        keyFunction.apply(t), valueFunction.apply(t)
                ),
                (b1, b2) -> b1.putAll(b2.build()),
                ImmutableMultimap.Builder::build
        );
    }
}
