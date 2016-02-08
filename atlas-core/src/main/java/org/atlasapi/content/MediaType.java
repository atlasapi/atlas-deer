package org.atlasapi.content;

import javax.annotation.Nullable;

import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public enum MediaType {

    AUDIO,
    VIDEO;

    @FieldName("key")
    public String toKey() {
        return this.name().toLowerCase();
    }

    @Override
    public String toString() {
        return toKey();
    }

    private static final ImmutableSet<MediaType> ALL = ImmutableSet.copyOf(values());

    public static final ImmutableSet<MediaType> all() {
        return ALL;
    }

    private static final ImmutableMap<String, Optional<MediaType>> KEY_MAP =
            ImmutableMap.copyOf(Maps.transformValues(Maps.uniqueIndex(
                    all(),
                    new Function<MediaType, String>() {

                        @Override
                        @Nullable
                        public String apply(@Nullable MediaType input) {
                            return input.toKey();
                        }
                    }
            ), new Function<MediaType, Optional<MediaType>>() {

                @Override
                public Optional<MediaType> apply(MediaType o) {
                    return Optional.fromNullable(o);
                }
            }));

    public static Optional<MediaType> fromKey(String key) {
        Optional<MediaType> possibleMediaType = KEY_MAP.get(key);
        return possibleMediaType != null ? possibleMediaType
                                         : Optional.<MediaType>absent();
    }

}
