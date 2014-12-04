package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.content.RelatedLink;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.SegmentAndEventTuple;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;

import com.google.common.primitives.Ints;

public class SegmentEventWriter implements EntityListWriter<SegmentAndEventTuple> {

    private final EntityListWriter<RelatedLink> relatedLinkWriter =
            new RelatedLinkWriter();

    @Nonnull
    @Override
    public String fieldName(SegmentAndEventTuple entity) {
        return "segment_event";
    }

    @Nonnull
    @Override
    public String listName() {
        return "segment_events";
    }

    @Override
    public void write(@Nonnull SegmentAndEventTuple entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        if (entity == null) {
            return;
        }
        final Segment segment = entity.getSegment();
        SegmentEvent segmentEvent = entity.getSegmentEvent();
        writer.writeField("position", segmentEvent.getPosition());
        writer.writeField("offset", Ints.saturatedCast(segmentEvent.getOffset().getStandardSeconds()));
        writer.writeField("is_chapter", segmentEvent.getIsChapter());
        writer.writeObject(segmentWriter(), segment, ctxt);
    }

    private EntityWriter<Segment> segmentWriter() {
        return new EntityWriter<Segment>() {
            @Override
            public void write(@Nonnull Segment entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
                writer.writeField("title", entity.getTitle());
                writer.writeField("description", entity.getDescription());
                writer.writeField("duration", Ints.saturatedCast(entity.getDuration().getStandardSeconds()));
                writer.writeField("segment_type", entity.getType().name().toLowerCase());
                writer.writeList(relatedLinkWriter, entity.getRelatedLinks(), ctxt);
            }

            @Nonnull
            @Override
            public String fieldName(Segment entity) {
                return "segment";
            }
        };
    }
}