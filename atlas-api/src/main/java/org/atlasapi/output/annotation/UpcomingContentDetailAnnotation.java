package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.UpcomingContentDetailWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpcomingContentDetailAnnotation extends OutputAnnotation<Content> {

    private final MergingEquivalentsResolver<Content> contentResolver;
    private final UpcomingContentDetailWriter upcomingContentDetailWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter;

    public UpcomingContentDetailAnnotation(
            MergingEquivalentsResolver<Content> contentResolver,
            UpcomingContentDetailWriter upcomingContentDetailWriter
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.upcomingContentDetailWriter = checkNotNull(upcomingContentDetailWriter);
        this.channelsBroadcastFilter = ChannelsBroadcastFilter.create();
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
                .flatMap(id -> {
                    ImmutableList<Content> equivs = resolvedEquivalents.get(id).asList();
                    if (equivs.isEmpty()) {
                        return Stream.empty();
                    }
                    Item item = (Item) equivs.get(0);
                    Iterable<Broadcast> upcomingBroadcasts = item.getBroadcasts()
                            .stream()
                            .filter(Broadcast::isUpcoming)
                            .collect(Collectors.toSet());
                    if (ctxt.getRegion().isPresent()) {
                        upcomingBroadcasts = channelsBroadcastFilter.sortAndFilter(
                                upcomingBroadcasts,
                                ctxt.getRegion().get()
                        );
                    }
                    item.setBroadcasts(
                            ImmutableSet.copyOf(upcomingBroadcasts)
                    );
                    return Stream.of(item);
                })
                .filter(i -> !i.getBroadcasts().isEmpty())
                .collect(Collectors.toList());

        writer.writeList(upcomingContentDetailWriter, items, ctxt);

    }
}
