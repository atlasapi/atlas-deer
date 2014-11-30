package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.segment.SegmentEvent;

public class SegmentEventWriter implements EntityListWriter<SegmentEvent> {

    @Override
    public void write(@Nonnull SegmentEvent entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {

    }

    @Nonnull
    @Override
    public String fieldName(SegmentEvent entity) {
        return "segment_event";
    }

    @Nonnull
    @Override
    public String listName() {
        return "segment_events";
    }
}