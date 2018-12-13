package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
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
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class CurrentAndFutureBroadcastsAnnotation extends OutputAnnotation<Content> {

    private static final Logger log = LoggerFactory.getLogger(CurrentAndFutureBroadcastsAnnotation.class);

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

                List<ResolvedBroadcast> resolvedBroadcasts = broadcasts.stream()
                        .map(broadcast -> ResolvedBroadcast.create(broadcast, resolveChannel(broadcast)))
                        .collect(MoreCollectors.toImmutableList());

                writer.writeList(
                        broadcastWriter,
                        resolvedBroadcasts,
                        ctxt
                );
            } else {
                writer.writeList(
                        broadcastWriter,
                        broadcastStream.map(broadcast -> ResolvedBroadcast.create(
                                        broadcast,
                                        resolveChannel(broadcast)
                                ))
                                .collect(MoreCollectors.toImmutableList()),
                        ctxt
                );
            }
        }
    }

    //TODO: Move resolution logic to query executor
    private ResolvedChannel resolveChannel(Broadcast broadcast) {

        try {
            Channel channel = Futures.getChecked(
                    channelResolver.resolveIds(
                            ImmutableList.of(broadcast.getChannelId())
                    ),
                    IOException.class
            )
                    .getResources()
                    .first()
                    .orNull();

            return ResolvedChannel.builder()
                    .withChannel(channel)
                    .withResolvedEquivalents(resolveEquivalents(channel.getSameAs()))
                    .build();

        } catch (IOException e) {
            log.error("Failed to resolve channel: {}", broadcast.getChannelId(), e);
            return null;
        }

    }

    @Nullable
    private Iterable<Channel> resolveEquivalents(Set<ChannelEquivRef> channelRefs) {
        try {
            if (channelRefs != null && !channelRefs.isEmpty()) {
                Iterable<Id> ids = Iterables.transform(channelRefs, ResourceRef::getId);
                return channelResolver.resolveIds(ids).get(1, TimeUnit.MINUTES).getResources();
            }

            return null;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to resolve channel equivlents", e);
            return null;
        }
    }
}
