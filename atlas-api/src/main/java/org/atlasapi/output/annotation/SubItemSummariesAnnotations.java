package org.atlasapi.output.annotation;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SubItemSummaryListWriter;
import org.atlasapi.util.ImmutableCollectors;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SubItemSummariesAnnotations extends OutputAnnotation<Content> {

    private static final String SUB_ITEM_OFFSET = "sub_items.offset";
    private static final String SUB_ITEM_LIMIT = "sub_items.limit";
    private final SubItemSummaryListWriter subItemSummaryListWriter;

    public SubItemSummariesAnnotations(SubItemSummaryListWriter subItemSummaryListWriter) {
        this.subItemSummaryListWriter = checkNotNull(subItemSummaryListWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if(entity instanceof Container) {
            Integer offset = 0;
            Integer limit = Integer.MAX_VALUE;
            String offsetStr = ctxt.getRequest().getParameter(SUB_ITEM_OFFSET);
            if (!Strings.isNullOrEmpty(offsetStr)) {
                offset = Integer.valueOf(offsetStr);
            }
            String limitStr = ctxt.getRequest().getParameter(SUB_ITEM_LIMIT);
            if (!Strings.isNullOrEmpty(limitStr)) {
                limit = Integer.valueOf(limitStr);
            }

            Container container = (Container) entity;
            ImmutableList<ItemSummary> summaries = Ordering.natural()
                    .onResultOf((ItemSummary summary) -> summary.getItemRef().getSortKey())
                    .reverse()
                    .nullsLast()
                    .immutableSortedCopy(container.getItemSummaries());
            summaries = summaries.stream()
                    .skip(offset)
                    .limit(limit)
                    .collect(ImmutableCollectors.toList());
            writer.writeList(subItemSummaryListWriter, summaries, ctxt);
        }
    }
}
