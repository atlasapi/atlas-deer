package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
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
        if (event.getSegmentRef() != null) {
            builder.setSegmentRef(serialize(event.getSegmentRef()));
        }
        if (event.getTitle() != null) {
            builder.setTitle(event.getTitle());
        }
        if (event.getDescription() != null) {
            builder.setDescription(event.getDescription());
        }
        if (event.getImage() != null) {
            builder.setImage(event.getImage());
        }
        if (event.getThumbnail() != null) { builder.setThumbnail(event.getThumbnail()); }
        return builder;
    }

    private ContentProtos.SegmentRef serialize(SegmentRef ref) {
        ContentProtos.SegmentRef.Builder builder = ContentProtos.SegmentRef.newBuilder();
        if (ref.getPublisher() != null) {
            builder.setSource(ref.getPublisher().key());
        }
        builder.setSegmentRef(ref.getId().longValue());
        return builder.build();
    }

    public SegmentEvent deserialize(ContentProtos.SegmentEvent msg) {
        SegmentEvent event = new SegmentEvent();
        event.setCanonicalUri(msg.hasUri() ? msg.getUri() : null);
        event.setIsChapter(msg.hasChapter() ? msg.getChapter() : null);
        if (msg.hasOffset()) {
            event.setOffset(Duration.millis(msg.getOffset()));
        }
        event.setPosition(msg.hasPosition() ? msg.getPosition() : null);
        if (msg.hasSegmentRef()) {
            event.setSegment(deserialize(msg.getSegmentRef()));
        }
        event.setTitle(msg.hasTitle() ? msg.getTitle() : null);
        event.setDescription(msg.hasDescription() ? msg.getDescription() : null);
        event.setImage(msg.hasImage() ? msg.getImage() : null);
        event.setThumbnail(msg.hasThumbnail() ? msg.getThumbnail() : null);

        return event;
    }

    private SegmentRef deserialize(ContentProtos.SegmentRef msg) {
        return new SegmentRef(Id.valueOf(msg.getSegmentRef()), Publisher.fromKey(msg.getSource()).requireValue());
    }
}
