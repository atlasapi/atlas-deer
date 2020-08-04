package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelResolver;
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
import org.atlasapi.output.writers.BroadcastWriter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class CurrentAndFutureBroadcastsAnnotation extends OutputAnnotation<Content> {

    private static final Logger log = LoggerFactory.getLogger(CurrentAndFutureBroadcastsAnnotation.class);
    private static final int CHANNEL_RESOLVING_RETRIES = 3;

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter;
    private final ChannelResolver channelResolver;

    private CurrentAndFutureBroadcastsAnnotation(
            BroadcastWriter broadcastWriter,
            ChannelResolver channelResolver
    ) {
        this.broadcastWriter = broadcastWriter;
        this.channelsBroadcastFilter = ChannelsBroadcastFilter.create();
        this.channelResolver = channelResolver;
    }

    public static CurrentAndFutureBroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new CurrentAndFutureBroadcastsAnnotation(
                BroadcastWriter.create(
                        "broadcasts",
                        "broadcast",
                        codec
                ),
                channelResolver
        );
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                    .filter(Broadcast::isActivelyPublished)
                    .filter(b -> b.getTransmissionEndTime()
                            .isAfter(DateTime.now(DateTimeZone.UTC)));

            if (ctxt.getRegions().isPresent()) {
                List<Region> regions = ctxt.getRegions().get();

                List<Broadcast> broadcasts = Lists.newArrayList();
                regions.forEach(region -> {
                    Iterable<Broadcast> broadcastsToAdd = channelsBroadcastFilter.sortAndFilter(
                            broadcastStream.collect(MoreCollectors.toImmutableList()),
                            region
                    );
                    Iterables.addAll(broadcasts, broadcastsToAdd);
                });

                Set<Id> channelIds = broadcasts.stream()
                        .map(Broadcast::getChannelId)
                        .collect(MoreCollectors.toImmutableSet());
                Map<Id, ResolvedChannel> channelMap = resolveChannelMap(channelIds);

                List<ResolvedBroadcast> resolvedBroadcasts = broadcasts.stream()
                        .map(broadcast -> ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId())))
                        .collect(MoreCollectors.toImmutableList());

                writer.writeList(
                        broadcastWriter,
                        resolvedBroadcasts,
                        ctxt
                );
            } else {
                Set<Id> channelIds = broadcastStream.map(Broadcast::getChannelId)
                        .collect(MoreCollectors.toImmutableSet());
                Map<Id, ResolvedChannel> channelMap = resolveChannelMap(channelIds);
                writer.writeList(
                        broadcastWriter,
                        broadcastStream.map(broadcast ->
                                ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId()))
                        )
                                .collect(MoreCollectors.toImmutableList()),
                        ctxt
                );
            }
        }
    }

    //TODO: Move resolution logic to query executor
    private Map<Id, ResolvedChannel> resolveChannelMap(Set<Id> channelIds) {
        Map<Id, Channel> channelMap = new HashMap<>(channelIds.size());

        Iterable<Channel> resolvedChannels = resolveChannels(channelIds);
        Set<Id> equivalentIds = new HashSet<>();
        for (Channel channel : resolvedChannels) {
            channelMap.put(channel.getId(), channel);
            for (ChannelEquivRef equivRef : channel.getSameAs()) {
                equivalentIds.add(equivRef.getId());
            }
        }
        Set<Id> equivalentsToResolve = Sets.difference(equivalentIds, channelMap.keySet());
        Iterable<Channel> resolvedEquivalents = resolveChannels(equivalentsToResolve);
        for (Channel channel : resolvedEquivalents) {
            channelMap.put(channel.getId(), channel);
        }

        return channelIds.stream().map(channelId -> {
            Channel channel = channelMap.get(channelId);
            if (channel == null) {
                return null;
            }
            List<Channel> equivalentChannels = channel.getSameAs().stream()
                    .map(channelEquivRef -> channelMap.get(channelEquivRef.getId()))
                    .filter(Objects::nonNull)
                    .collect(MoreCollectors.toImmutableList());
            return ResolvedChannel.builder(channel)
                    .withResolvedEquivalents(equivalentChannels)
                    .build();
        })
                .filter(Objects::nonNull)
                .collect(MoreCollectors.toImmutableMap(
                        resolvedChannel -> resolvedChannel.getChannel().getId(),
                        resolvedChannel -> resolvedChannel
                ));

    }

    private Iterable<Channel> resolveChannels(Set<Id> channelIds) {
        if (channelIds.isEmpty()) {
            return ImmutableList.of();
        }
        for (int i = 1; i <= CHANNEL_RESOLVING_RETRIES; i++) {
            if (i > 1) {
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException e) {
                    return ImmutableList.of();
                }
            }
            try {
                return Futures.getChecked(
                        channelResolver.resolveIds(channelIds),
                        IOException.class
                ).getResources();
            } catch (IOException e) {
                log.error("Failed to resolve channels: {}", channelIds, e);
            }
        }
        return ImmutableList.of();
    }

}
