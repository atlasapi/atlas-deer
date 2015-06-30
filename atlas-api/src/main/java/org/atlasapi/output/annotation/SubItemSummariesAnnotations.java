package org.atlasapi.output.annotation;

import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SubItemSummaryListWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SubItemSummariesAnnotations extends OutputAnnotation<Content> {

    private final SubItemSummaryListWriter subItemSummaryListWriter;

    public SubItemSummariesAnnotations(SubItemSummaryListWriter subItemSummaryListWriter) {
        this.subItemSummaryListWriter = checkNotNull(subItemSummaryListWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if(entity instanceof Container) {
            writer.writeList(subItemSummaryListWriter, ((Container)entity).getItemSummaries(), ctxt);
        }
    }
}
