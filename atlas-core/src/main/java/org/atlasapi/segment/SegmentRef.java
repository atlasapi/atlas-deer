package org.atlasapi.segment;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import org.atlasapi.entity.Id;

public class SegmentRef {
    
    public static final Function<SegmentRef, Id> TO_ID = new Function<SegmentRef, Id>(){
        @Override
        public Id apply(SegmentRef input) {
            return input.getId();
        }
    };
    
    private final Id id;

    public SegmentRef(Id id) {
        this.id = checkNotNull(id);
    }

    public Id getId() {
        return id;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof SegmentRef) {
            SegmentRef other = (SegmentRef) that;
            return other.id.equals(this.id);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("SegRef %s", id);
    }
}