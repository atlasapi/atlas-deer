package org.atlasapi.segment;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;

public class SegmentRef {
    
    public static final Function<SegmentRef, String> TO_ID = new Function<SegmentRef, String>(){
        @Override
        public String apply(SegmentRef input) {
            return input.identifier();
        }
    };
    
    private final String identifier;

    public SegmentRef(String identifier) {
        this.identifier = checkNotNull(identifier);
    }

    @FieldName("identifier")
    public String identifier() {
        return identifier;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof SegmentRef) {
            SegmentRef other = (SegmentRef) that;
            return other.identifier.equals(this.identifier);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return identifier.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("SegRef %s", identifier);
    }
}