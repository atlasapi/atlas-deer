package org.atlasapi.segment;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;

public class SegmentRef {
    
    public static final Function<SegmentRef, Long> TO_ID = new Function<SegmentRef, Long>(){
        @Override
        public Long apply(SegmentRef input) {
            return input.identifier();
        }
    };
    
    private final Long identifier;

    public SegmentRef(Long identifier) {
        this.identifier = checkNotNull(identifier);
    }

    public Long identifier() {
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