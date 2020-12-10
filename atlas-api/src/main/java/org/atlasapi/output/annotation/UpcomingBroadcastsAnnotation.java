package org.atlasapi.output.annotation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Region;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResolvedChannelResolver;
import org.atlasapi.output.writers.BroadcastWriter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Marker for {@link BroadcastsAnnotation} to only write broadcasts that have not yet broadcast.
 */
public class UpcomingBroadcastsAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter;
    private final ResolvedChannelResolver resolvedChannelResolver;

    private UpcomingBroadcastsAnnotation(
            BroadcastWriter broadcastWriter,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        this.broadcastWriter = broadcastWriter;
        this.channelsBroadcastFilter = ChannelsBroadcastFilter.create();
        this.resolvedChannelResolver = resolvedChannelResolver;
    }

    public static UpcomingBroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        return new UpcomingBroadcastsAnnotation(
                BroadcastWriter.create(
                        "broadcasts",
                        "broadcast",
                        codec
                ),
                resolvedChannelResolver
        );
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            List<Broadcast> filteredBroadcasts = item.getBroadcasts().stream()
                    .filter(Broadcast::isActivelyPublished)
                    .filter(b -> b.getTransmissionTime().isAfter(DateTime.now(DateTimeZone.UTC)))
                    .collect(MoreCollectors.toImmutableList());

            if (ctxt.getRegions().isPresent()) {
                List<Region> regions = ctxt.getRegions().get();
                boolean lcnSharing = ctxt.getActiveAnnotations().contains(Annotation.LCN_SHARING);

                List<Broadcast> broadcasts = Lists.newArrayList();
                regions.forEach(region -> {
                    Iterable<Broadcast> broadcastsToAdd = channelsBroadcastFilter.sortAndFilter(
                            filteredBroadcasts,
                            region,
                            lcnSharing
                    );
                    Iterables.addAll(broadcasts, broadcastsToAdd);
                });

                Map<Id, ResolvedChannel> channelMap = resolvedChannelResolver.resolveChannelMap(broadcasts);

                List<ResolvedBroadcast> resolvedBroadcasts = broadcasts.stream()
                        .map(broadcast -> ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId())))
                        .collect(MoreCollectors.toImmutableList());

                writer.writeList(
                        broadcastWriter,
                        resolvedBroadcasts,
                        ctxt
                );
            } else {
                Map<Id, ResolvedChannel> channelMap = resolvedChannelResolver.resolveChannelMap(filteredBroadcasts);
                writer.writeList(
                        broadcastWriter,
                        filteredBroadcasts.stream().map(broadcast ->
                                ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId()))
                        )
                                .collect(MoreCollectors.toImmutableList()),
                        ctxt
                );
            }
        }
    }
}
