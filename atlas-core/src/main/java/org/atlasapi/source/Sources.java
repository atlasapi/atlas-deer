package org.atlasapi.source;

import org.atlasapi.application.v3.SourceRestriction;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class Sources {

    private Sources() {
    }

    private static final ImmutableSet<Publisher> ALL
            = ImmutableSet.copyOf(Publisher.values());

    public static final ImmutableSet<Publisher> all() {
        return ALL;
    }

    private static final OptionalMap<String, Publisher> KEY_LOOKUP =
            ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), toKey()));

    public static final Function<Publisher, String> toKey() {
        return ToKeyFunction.INSTANCE;
    }

    private enum ToKeyFunction implements Function<Publisher, String> {
        INSTANCE;

        @Override
        public String apply(Publisher from) {
            return from.key();
        }
    }

    public static final Optional<Publisher> fromPossibleKey(String key) {
        return fromKey().apply(key);
    }

    public static final Function<String, Optional<Publisher>> fromKey() {
        return FromKeyFunction.INSTANCE;
    }

    private enum FromKeyFunction implements Function<String, Optional<Publisher>> {
        INSTANCE;

        @Override
        public Optional<Publisher> apply(String input) {
            return KEY_LOOKUP.get(input);
        }

    }

    public static final boolean isAdminOnlySource(Publisher source) {
        return source.restriction().equals(SourceRestriction.ADMIN_ONLY);
    }

}
