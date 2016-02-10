package org.atlasapi.segment;

import org.atlasapi.entity.Id;

public interface SegmentResolver {

    Iterable<Segment> resolveSegments(Iterable<Id> id);

}
