package org.atlasapi.annotation;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.collect.ImmutableOptionalMap;

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
    TOPICS,
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
    SUB_ITEM_SUMMARIES
    ;
    
    public String toKey() {
        return name().toLowerCase();
    }
    
    private static final Function<Annotation, String> TO_KEY = new Function<Annotation, String>() {
        @Override
        public String apply(Annotation input) {
            return input.toKey();
        }
    };
    
    public static final Function<Annotation, String> toKeyFunction() {
        return TO_KEY;
    }
    
    private static final ImmutableSet<Annotation> ALL = ImmutableSet.copyOf(values());
    
    public static final ImmutableSet<Annotation> all() {
        return ALL;
    }
    
    private static final Map<String,Optional<Annotation>> lookup
        = ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), TO_KEY));
    
    public static final Map<String, Optional<Annotation>> lookup() {
        return lookup;
    }
    
    public static final Optional<Annotation> fromKey(String key) {
        return lookup.get(key);
    }
    
    public static final ImmutableSet<Annotation> standard() {
        return defaultAnnotations;
    }
    
    // TODO revise standard annotations for the meta API
    private static final ImmutableSet<Annotation> defaultAnnotations = ImmutableSet.of(
        ID_SUMMARY, LICENSE, META_MODEL, META_ENDPOINT
    );
    
}
