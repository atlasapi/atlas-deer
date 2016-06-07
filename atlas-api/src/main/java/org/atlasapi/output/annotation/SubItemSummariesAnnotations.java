package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SubItemSummaryListWriter;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import static com.google.common.base.Preconditions.checkNotNull;

public class SubItemSummariesAnnotations extends OutputAnnotation<Content> {

    private static final String SUB_ITEM_OFFSET = "sub_items_summaries.offset";
    private static final String SUB_ITEM_LIMIT = "sub_items_summaries.limit";
    private static final String SUB_ITEMS_ORDERING = "sub_items_summaries.ordering";
    private static final String REVERSE = "reverse";
    private final SubItemSummaryListWriter subItemSummaryListWriter;

    public SubItemSummariesAnnotations(SubItemSummaryListWriter subItemSummaryListWriter) {
        this.subItemSummaryListWriter = checkNotNull(subItemSummaryListWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Container) {
            Container container = (Container) entity;

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

            String orderingStr = ctxt.getRequest().getParameter(SUB_ITEMS_ORDERING);

            Ordering<ItemSummary> ordering = Ordering.natural()
                    .onResultOf((ItemSummary summary) -> summary.getItemRef().getSortKey())
                    .nullsLast();

            /* Item refs by default are ordered reverse lexicographically,
                so to reverse that ordering through the API is to simply leave it alone here */
            if (Strings.isNullOrEmpty(orderingStr) || !REVERSE.equalsIgnoreCase(orderingStr)) {
                ordering = ordering.reverse();
            }

            ImmutableList<ItemSummary> summaries = ordering.immutableSortedCopy(container.getItemSummaries())
                    .stream()
                    .skip(offset)
                    .limit(limit)
                    .collect(MoreCollectors.toList());

            writer.writeList(subItemSummaryListWriter, summaries, ctxt);
        }
    }
}
