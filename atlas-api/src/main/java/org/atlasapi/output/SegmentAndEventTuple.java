package org.atlasapi.output;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;

public class SegmentAndEventTuple {

    private final Segment segment;
    private final SegmentEvent segmentEvent;

    public SegmentAndEventTuple(Segment segment, SegmentEvent segmentEvent) {
        checkArgument(segment.getId().equals(segmentEvent.getSegmentRef().getId()),
                "Segment Event must reference Segment");
        this.segment = checkNotNull(segment);
        this.segmentEvent = checkNotNull(segmentEvent);
    }

    public Segment getSegment() {
        return segment;
    }

    public SegmentEvent getSegmentEvent() {
        return segmentEvent;
    }
}
