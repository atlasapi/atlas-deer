package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.SegmentEvent.Builder;
import org.joda.time.Duration;

public class SegmentEventSerializer {

    public ContentProtos.SegmentEvent.Builder serialize(SegmentEvent event) {
        Builder builder = ContentProtos.SegmentEvent.newBuilder();
        if (event.getCanonicalUri() != null) {
            builder.setUri(event.getCanonicalUri());
        }
        if (event.getIsChapter() != null) {
            builder.setChapter(event.getIsChapter());
        }
        if (event.getOffset() != null) {
            builder.setOffset(event.getOffset().getMillis());
        }
        if (event.getPosition() != null) {
            builder.setPosition(event.getPosition());
        }
        if (event.getSegment() != null) {
            builder.setSegment(event.getId().toString());
        }
        Description desc = event.getDescription();
        if (desc != null) {
            if (desc.getTitle() != null) { builder.setTitle(desc.getTitle()); }
            if (desc.getSynopsis() != null) { builder.setDescription(desc.getSynopsis()); }
            if (desc.getImage() != null) { builder.setImage(desc.getImage()); }
            if (desc.getThumbnail() != null) { builder.setThumbnail(desc.getThumbnail()); }
        }
        return builder;
    }

    public SegmentEvent deserialize(ContentProtos.SegmentEvent msg) {
        SegmentEvent event = new SegmentEvent();
        event.setCanonicalUri(msg.hasUri() ? msg.getUri() : null);
        event.setIsChapter(msg.hasChapter() ? msg.getChapter() : null);
        if (msg.hasOffset()) {
            event.setOffset(Duration.millis(msg.getOffset()));
        }
        event.setPosition(msg.hasPosition() ? msg.getPosition() : null);
        if (msg.hasSegment()) {
            event.setSegment(new SegmentRef(Id.valueOf(msg.getSegment())));
        }
        Description desc = new Description(
            msg.hasTitle() ? msg.getTitle() : null,
            msg.hasDescription() ? msg.getDescription() : null,
            msg.hasImage() ? msg.getImage() : null,
            msg.hasThumbnail() ? msg.getThumbnail() : null
            );
        event.setDescription(desc);
        return event;
    }

}
