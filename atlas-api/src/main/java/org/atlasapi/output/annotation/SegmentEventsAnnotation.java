package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.SegmentAndEventTuple;
import org.atlasapi.output.SegmentRelatedLinkMergingFetcher;
import org.atlasapi.output.writers.SegmentEventWriter;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

public class SegmentEventsAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private final SegmentRelatedLinkMergingFetcher linkMergingFetcher;
    private final EntityListWriter<SegmentAndEventTuple> segmentWriter =
            new SegmentEventWriter();

    public SegmentEventsAnnotation(SegmentRelatedLinkMergingFetcher linkMergingFetcher) {
        this.linkMergingFetcher = checkNotNull(linkMergingFetcher);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter format, OutputContext ctxt) throws IOException {
        if (entity.getContent() instanceof Item) {
            if (!((Item) entity.getContent()).getSegmentEvents().isEmpty()) {
                writeSegmentEvents(format, (Item) entity.getContent(), ctxt);
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