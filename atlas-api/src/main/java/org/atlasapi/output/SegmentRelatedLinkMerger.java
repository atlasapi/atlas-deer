package org.atlasapi.output;

import org.atlasapi.content.RelatedLink;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

public interface SegmentRelatedLinkMerger {
    ImmutableList<RelatedLink> getLinks(Segment selectedSegment, SegmentEvent selectedSegmentEvent,
                                        Multimap<Segment, SegmentEvent> segmentMap);
}
