package org.atlasapi.output.annotation;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.UpcomingContentDetailWriter;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpcomingContentDetailAnnotation extends OutputAnnotation<Content> {

    private final MergingEquivalentsResolver<Content> contentResolver;
    private final UpcomingContentDetailWriter upcomingContentDetailWriter;
    private final ChannelGroupResolver channelGroupResolver;
    private final NumberToShortStringCodec codec;
    private final ChannelsBroadcastFilter channelsBroadcastFilter = new ChannelsBroadcastFilter();

    public UpcomingContentDetailAnnotation(
            MergingEquivalentsResolver<Content> contentResolver,
            UpcomingContentDetailWriter upcomingContentDetailWriter,
            ChannelGroupResolver channelGroupResolver,
            NumberToShortStringCodec codec
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.upcomingContentDetailWriter = checkNotNull(upcomingContentDetailWriter);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.codec = checkNotNull(codec);
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


        Optional<Region> region;
        String regionParam = ctxt.getRequest().getParameter(Attributes.REGION.externalName());
        if(Strings.isNullOrEmpty(regionParam)) {
            region = Optional.empty();
        } else {
            Id regionId = Id.valueOf(codec.decode(regionParam));
            com.google.common.base.Optional<ChannelGroup> channelGroup = Futures.get(
                    channelGroupResolver.resolveIds(ImmutableList.of(regionId)),
                    1, TimeUnit.MINUTES,
                    IOException.class
            ).getResources().first();
            if (!channelGroup.isPresent()) {
                Throwables.propagate(new NotFoundException(regionId));
            } else if (!(channelGroup.get() instanceof Region)) {
                Throwables.propagate(new NotAcceptableException(
                                String.format(
                                        "%s is a channel group of type '%s', should be 'region",
                                        regionParam,
                                        channelGroup.get().getType()
                                )
                        )
                );
            }

            region = Optional.of((Region)channelGroup.get());
        }

        Iterable<Item> items = contentIds.stream()
                .map(id -> {
                    Item item = (Item) resolvedEquivalents.get(id).asList().get(0);
                    Iterable<Broadcast> upcomingBroadcasts = item.getBroadcasts()
                            .stream()
                            .filter(Broadcast.IS_UPCOMING)
                            .collect(Collectors.toSet());
                    if (region.isPresent()) {
                        upcomingBroadcasts = channelsBroadcastFilter.sortAndFilter(
                                upcomingBroadcasts,
                                region.get()
                        );
                    }
                    item.setBroadcasts(
                            ImmutableSet.copyOf(upcomingBroadcasts)
                    );
                    return item;
                })
                .collect(Collectors.toList());

        writer.writeList(upcomingContentDetailWriter, items, ctxt);

    }
}
