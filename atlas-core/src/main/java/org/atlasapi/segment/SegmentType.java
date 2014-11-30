package org.atlasapi.segment;

import java.util.Map;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;

public enum SegmentType {

    MUSIC("music"),
    SPEECH("speech"),
    VIDEO("video");
    
    private final String display;

    private SegmentType(String display) {
        this.display = display;
    }
    
    @Override
    public String toString() {
        return display;
    }
    
    private static Map<String,SegmentType> lookup = Maps.uniqueIndex(ImmutableSet.copyOf(SegmentType.values()), Functions.toStringFunction());
    
    public static Maybe<SegmentType> fromString(String type) {
        return Maybe.fromPossibleNullValue(lookup.get(type));
    }
}
