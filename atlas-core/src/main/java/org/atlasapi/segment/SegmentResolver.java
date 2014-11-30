package org.atlasapi.segment;

import org.atlasapi.entity.Id;

import com.google.common.base.Optional;

public interface SegmentResolver {

    Iterable<Segment> resolveSegments(Iterable<Id> id);

}
