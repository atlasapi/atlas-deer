package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.media.entity.Publisher;

import org.joda.time.Duration;

public class SegmentEventSerialization {

    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();
    private final DescriptionSerialization description = new DescriptionSerialization();
    private final SegmentRefSerialization segmentRef = new SegmentRefSerialization();

    public SegmentEvent serialize(org.atlasapi.segment.SegmentEvent segmentEvent) {
        if (segmentEvent == null) {
            return null;
        }
        SegmentEvent internal = new SegmentEvent();
        identifiedSetter.serialize(internal, segmentEvent);

        internal.setPosition(segmentEvent.getPosition());
        Duration offset = segmentEvent.getOffset();
        if (offset != null) {
            internal.setOffset(offset.getMillis());
        }
        internal.setIsChapter(segmentEvent.getIsChapter());
        internal.setDescription(description.serialize(segmentEvent.getDescription()));
        internal.setSegmentRef(segmentRef.serialize(segmentEvent.getSegmentRef()));
        internal.setVersionId(segmentEvent.getVersionId());
        Publisher source = segmentEvent.getSource();
        if (source != null) {
            internal.setPublisher(source.key());
        }

        return internal;
    }

    public org.atlasapi.segment.SegmentEvent deserialize(SegmentEvent internal) {
        org.atlasapi.segment.SegmentEvent segment = new org.atlasapi.segment.SegmentEvent();

        identifiedSetter.deserialize(segment, internal);

        segment.setPosition(internal.getPosition());
        Long offset = internal.getOffset();
        if (offset != null) {
            segment.setOffset(new Duration(offset));
        }
        segment.setIsChapter(internal.getIsChapter());

        segment.setDescription(description.deserialize(internal.getDescription()));

        segment.setSegment(segmentRef.deserialize(internal.getSegmentRef()));

        segment.setVersionId(internal.getVersionId());

        String publisher = internal.getPublisher();
        if (publisher != null) {
            segment.setPublisher(Publisher.fromKey(publisher).requireValue());
        }

        return segment;
    }

}