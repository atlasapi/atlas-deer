package org.atlasapi.annotation;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.collect.ImmutableOptionalMap;

import java.util.Map;

public enum Annotation {
    //The order of these entries defines the order of output fields
    LICENSE,
    ID_SUMMARY,
    ID,
    REP_ID,
    IS_PUBLISHED,
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
    ALL_AGGREGATED_BROADCASTS,
    AGGREGATED_BROADCASTS,
    BROADCASTS, //all broadcasts from equiv set with same source with merge on same channel/tx start
    LOCATIONS,
    FIRST_BROADCASTS,
    NEXT_BROADCASTS,
    AVAILABLE_LOCATIONS,
    UPCOMING_BROADCASTS,
    CURRENT_AND_FUTURE_BROADCASTS,
    ALL_MERGED_BROADCASTS,  //all broadcasts from equiv set with merge on same channel/tx start
    ALL_BROADCASTS,         //all broadcasts from equiv set without merge on same channel/tx start
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
    @Deprecated CHANNEL_GROUPS_SUMMARY,             // causes performance problems
    @Deprecated GENERIC_CHANNEL_GROUPS_SUMMARY,     // only to be used by existing users of this annotation
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
    AWARDS,
    PRIORITY_REASONS,
    MODIFIED_DATES,
    CHANNEL_GROUP_INFO,
    CHANNEL_IDS,
    FUTURE_CHANNELS,
    CUSTOM_FIELDS,
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
