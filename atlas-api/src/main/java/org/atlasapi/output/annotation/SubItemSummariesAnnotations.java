package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ItemSummary;
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
            Container container = (Container) entity;
            ImmutableList<ItemSummary> summaries = Ordering.natural()
                    .onResultOf((ItemSummary summary) -> summary.getItemRef().getSortKey())
                    .nullsLast()
                    .immutableSortedCopy(container.getItemSummaries());
            writer.writeList(subItemSummaryListWriter, summaries, ctxt);
        }
    }
}
