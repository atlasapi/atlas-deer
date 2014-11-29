package org.atlasapi.segment;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;

public class SegmentRef extends ResourceRef {
    
    public static final Function<SegmentRef, Id> TO_ID = new Function<SegmentRef, Id>(){
        @Override
        public Id apply(SegmentRef input) {
            return input.getId();
        }
    };
    
    public SegmentRef(Id id, Publisher source) {
        super(id, source);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.SEGMENT;
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
}