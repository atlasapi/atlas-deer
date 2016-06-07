package org.atlasapi.channel;

import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public enum ChannelType {

    CHANNEL,
    MASTERBRAND;

    public String toKey() {
        return this.name().toLowerCase();
    }

    private static final ImmutableMap<String, ChannelType> KEY_MAP = ImmutableMap
            .copyOf(Maps.uniqueIndex(
                    ImmutableSet.copyOf(values()),
                    new Function<ChannelType, String>() {
                        @Override
                        @Nullable
                        public String apply(@Nullable ChannelType input) {
                            return input.toKey();
                        }
                    }
            ));

    public static Optional<ChannelType> fromKey(String type) {
        return Optional.ofNullable(KEY_MAP.get(type));
    }
}
