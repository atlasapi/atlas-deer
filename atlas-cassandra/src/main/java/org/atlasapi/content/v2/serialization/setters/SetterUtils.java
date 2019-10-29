package org.atlasapi.content.v2.serialization.setters;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class SetterUtils {

    // This is because in the current version of guava used at the time of writing there is no ImmutableMap
    // collector that can deal with duplicate keys.
    public static <T, K, V> Collector<T, ?, ImmutableMap<K, V>> toImmutableMapAllowDuplicates(
            Function<? super T, ? extends K> keyFunction,
            Function<? super T, ? extends V> valueFunction
    ) {
        return Collector.of(
                (Supplier<HashMap<K, V>>) HashMap::new,
                (map, t) -> map.put(keyFunction.apply(t), valueFunction.apply(t)),
                (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                },
                ImmutableMap::copyOf
        );
    }
}
