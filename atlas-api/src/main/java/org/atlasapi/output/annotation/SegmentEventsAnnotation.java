package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.SegmentAndEventTuple;
import org.atlasapi.output.SegmentRelatedLinkMergingFetcher;
import org.atlasapi.output.writers.SegmentEventWriter;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

public class SegmentEventsAnnotation extends OutputAnnotation<Content> {

    private final SegmentRelatedLinkMergingFetcher linkMergingFetcher;
    private final EntityListWriter<SegmentAndEventTuple> segmentWriter =
            new SegmentEventWriter();

    public SegmentEventsAnnotation(SegmentRelatedLinkMergingFetcher linkMergingFetcher) {
        this.linkMergingFetcher = checkNotNull(linkMergingFetcher);
    }

    @Override
    public void write(Content entity, FieldWriter format, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            if (!((Item) entity).getSegmentEvents().isEmpty()) {
                writeSegmentEvents(format, (Item) entity, ctxt);
            } else {
                format.writeList(segmentWriter, ImmutableList.<SegmentAndEventTuple>of(), ctxt);
            }
        }
    }

    private void writeSegmentEvents(FieldWriter writer, Item item, OutputContext ctxt)
            throws IOException {
        writer.writeList(
                segmentWriter,
                linkMergingFetcher.mergeSegmentLinks(item.getSegmentEvents()),
                ctxt
        );
    }
}