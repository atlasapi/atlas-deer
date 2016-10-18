package org.atlasapi.annotation;

import java.util.Map;

import com.metabroadcast.common.collect.ImmutableOptionalMap;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public enum Annotation {
    //The order of these entries defines the order of output fields
    LICENSE,
    ID_SUMMARY,
    ID,
    EXTENDED_ID,
    DESCRIPTION,
    EXTENDED_DESCRIPTION,
    BRAND_REFERENCE,
    BRAND_SUMMARY,
    SERIES_REFERENCE,
    SERIES_SUMMARY,
    SUB_ITEMS,
    CLIPS,
    PEOPLE,
    PEOPLE_DETAIL,
    TAGS,
    CONTENT_GROUPS,
    SEGMENT_EVENTS,
    RELATED_LINKS,
    KEY_PHRASES,
    BROADCASTS,
    LOCATIONS,
    FIRST_BROADCASTS,
    NEXT_BROADCASTS,
    AVAILABLE_LOCATIONS,
    UPCOMING_BROADCASTS,
    CURRENT_AND_FUTURE_BROADCASTS,
    FILTERING_RESOURCE,
    CHANNEL,
    CHANNEL_GROUP,
    REGIONS,
    PLATFORM,
    CHANNEL_GROUPS,
    HISTORY,
    PARENT,
    PRODUCTS,
    RECENTLY_BROADCAST,
    CHANNELS,
    CHANNEL_GROUPS_SUMMARY,
    GENERIC_CHANNEL_GROUPS_SUMMARY,
    CONTENT_CHANNEL_SUMMARY,
    PUBLISHER,
    SERIES,
    CONTENT_SUMMARY,
    CONTENT_DETAIL,
    CHANNEL_SUMMARY,
    AUDIT,
    IMAGES,
    META_MODEL,
    META_ENDPOINT,
    VARIATIONS,
    UPCOMING_CONTENT_DETAIL,
    AVAILABLE_CONTENT_DETAIL,
    AVAILABLE_CONTENT,
    SUB_ITEM_SUMMARIES,
    EVENT,
    EVENT_DETAILS,
    ADVERTISED_CHANNELS,
    SUPPRESS_EPISODE_NUMBERS,
    NON_MERGED,
    REVIEWS,
    RATINGS,
    PRIORITY_REASONS
    ;

    private static final ImmutableSet<Annotation> ALL = ImmutableSet.copyOf(values());

    private static final Map<String, Optional<Annotation>> lookup
            = ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), Annotation::toKey));

    public String toKey() {
        return name().toLowerCase();
    }

    public static Function<Annotation, String> toKeyFunction() {
        return Annotation::toKey;
    }

    public static ImmutableSet<Annotation> all() {
        return ALL;
    }

    public static Map<String, Optional<Annotation>> lookup() {
        return lookup;
    }

    public static Optional<Annotation> fromKey(String key) {
        return lookup.get(key);
    }

    public static ImmutableSet<Annotation> standard() {
        return ImmutableSet.of(
                ID_SUMMARY, LICENSE, META_MODEL, META_ENDPOINT
        );
    }
}
