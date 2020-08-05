package org.atlasapi.output.annotation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BroadcastsAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter;
    private final ResolvedChannelResolver resolvedChannelResolver;

    private BroadcastsAnnotation(
            BroadcastWriter broadcastWriter,
            ChannelsBroadcastFilter channelsBroadcastFilter,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        this.broadcastWriter = broadcastWriter;
        this.channelsBroadcastFilter = channelsBroadcastFilter;
        this.resolvedChannelResolver = resolvedChannelResolver;
    }

    public static BroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        return new BroadcastsAnnotation(
                BroadcastWriter.create(
                        "broadcasts",
                        "broadcast",
                        codec
                ),
                ChannelsBroadcastFilter.create(),
                resolvedChannelResolver
        );
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            writeBroadcasts(writer, (Item) entity, ctxt);
        }
    }

    private void writeBroadcasts(
            FieldWriter writer,
            Item item,
            OutputContext ctxt
    ) throws IOException {
        List<Broadcast> filteredBroadcasts = item.getBroadcasts().stream()
                .filter(Broadcast::isActivelyPublished)
                .collect(MoreCollectors.toImmutableList());

        if (ctxt.getRegions().isPresent()) {
            List<Region> regions = ctxt.getRegions().get();

            List<Broadcast> broadcasts = Lists.newArrayList();
            regions.forEach(region -> {
                Iterable<Broadcast> broadcastsToAdd = channelsBroadcastFilter.sortAndFilter(
                        filteredBroadcasts,
                        region
                );
                Iterables.addAll(broadcasts, broadcastsToAdd);
            });

            Set<Id> channelIds = broadcasts.stream()
                    .map(Broadcast::getChannelId)
                    .collect(MoreCollectors.toImmutableSet());
            Map<Id, ResolvedChannel> channelMap = resolvedChannelResolver.resolveChannelMap(channelIds);

            List<ResolvedBroadcast> resolvedBroadcasts = broadcasts.stream()
                    .map(broadcast -> ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId())))
                    .collect(MoreCollectors.toImmutableList());

            writer.writeList(broadcastWriter, resolvedBroadcasts, ctxt);
        } else {
            Set<Id> channelIds = filteredBroadcasts.stream().map(Broadcast::getChannelId)
                    .collect(MoreCollectors.toImmutableSet());
            Map<Id, ResolvedChannel> channelMap = resolvedChannelResolver.resolveChannelMap(channelIds);
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
