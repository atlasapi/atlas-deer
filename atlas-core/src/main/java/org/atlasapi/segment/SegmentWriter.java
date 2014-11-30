package org.atlasapi.segment;

import org.atlasapi.entity.util.WriteResult;

public interface SegmentWriter {

    WriteResult<Segment, Segment> writeSegment(Segment segment);
}
