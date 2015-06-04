package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.UpcomingContentDetailWriter;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpcomingContentDetailAnnotation extends OutputAnnotation<Content> {

    private final MergingEquivalentsResolver<Content> contentResolver;
    private final UpcomingContentDetailWriter upcomingContentDetailWriter;

    public UpcomingContentDetailAnnotation(MergingEquivalentsResolver<Content> contentResolver, UpcomingContentDetailWriter upcomingContentDetailWriter) {
        this.contentResolver = checkNotNull(contentResolver);
        this.upcomingContentDetailWriter = checkNotNull(upcomingContentDetailWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (!(entity instanceof Container)) {
            return;
        }

        Container container = (Container) entity;
        if (container.getUpcomingContent().isEmpty()) {
            writer.writeList(upcomingContentDetailWriter, ImmutableList.of(), ctxt);
            return;
        }

        Set<Id> contentIds = container.getUpcomingContent().keySet()
                .stream()
                .map(i -> i.getId())
                .collect(Collectors.toSet());

        final ResolvedEquivalents<Content> resolvedEquivalents = Futures.get(
                contentResolver.resolveIds(
                        contentIds,
                        ctxt.getApplicationSources(),
                        Annotation.all()
                ),
                IOException.class
        );

        Iterable<Item> items = contentIds.stream()
                .map(id -> {
                    Item item = (Item) resolvedEquivalents.get(id).asList().get(0);
                    item.setBroadcasts(
                            item.getBroadcasts()
                                    .stream()
                                    .filter(Broadcast.IS_UPCOMING)
                                    .collect(Collectors.toSet()
                                    )
                    );
                    return item;
                })
                .collect(Collectors.toList());

        writer.writeList(upcomingContentDetailWriter, items, ctxt);

    }
}
