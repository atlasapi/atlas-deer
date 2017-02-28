package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NextBroadcastAnnotation extends OutputAnnotation<Content> {

    private static final Logger log = LoggerFactory.getLogger(NextBroadcastAnnotation.class);
    private final BroadcastWriter broadcastWriter;
    private final Clock clock;
    private final ChannelResolver channelResolver;

    private NextBroadcastAnnotation(
            Clock clock,
            BroadcastWriter broadcastWriter,
            ChannelResolver channelResolver
    ) {
        super();
        this.clock = clock;
        this.broadcastWriter = broadcastWriter;
        this.channelResolver = channelResolver;
    }

    public static NextBroadcastAnnotation create(
            Clock clock,
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new NextBroadcastAnnotation(
                clock,
                BroadcastWriter.create(
                        "next_broadcasts",
                        "broadcast",
                        codec
                ),
                channelResolver
        );
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            writeBroadcasts(writer, (Item) entity, ctxt);
        }
    }

    private void writeBroadcasts(FieldWriter writer, Item item, OutputContext ctxt)
            throws IOException {

        List<Broadcast> broadcasts = item.getBroadcasts().stream()
                .filter(Broadcast::isActivelyPublished)
                .collect(MoreCollectors.toImmutableList());

        List<ResolvedBroadcast> resolvedBroadcasts = StreamSupport.stream(
                nextBroadcast(broadcasts).spliterator(), false)
                .map(broadcast -> ResolvedBroadcast.create(broadcast, resolveChannel(broadcast)))
                .collect(MoreCollectors.toImmutableList());

        writer.writeList(
                broadcastWriter,
                resolvedBroadcasts,
                ctxt
        );
    }

    private Iterable<Broadcast> nextBroadcast(Iterable<Broadcast> broadcasts) {
        DateTime now = clock.now();
        DateTime earliest = null;
        Builder<Broadcast> filteredBroadcasts = ImmutableSet.builder();
        for (Broadcast broadcast : broadcasts) {
            DateTime transmissionTime = broadcast.getTransmissionTime();
            if (transmissionTime.isAfter(now) && (earliest == null || transmissionTime.isBefore(
                    earliest))) {
                earliest = transmissionTime;
                filteredBroadcasts = ImmutableSet.<Broadcast>builder().add(broadcast);
            } else if (transmissionTime.isEqual(earliest)) {
                filteredBroadcasts.add(broadcast);
            }
        }
        return filteredBroadcasts.build();
    }

    private ResolvedChannel resolveChannel(Broadcast broadcast) {

        try {
            return ResolvedChannel.builder(
                    Futures.getChecked(
                            channelResolver.resolveIds(
                                    ImmutableList.of(broadcast.getChannelId())
                            ),
                            IOException.class
                    )
                            .getResources()
                            .first()
                            .orNull()
            )
                    .build();

        } catch (IOException e) {
            log.error("Failed to resolve channel: {}", broadcast.getChannelId(), e);
            return null;
        }

    }
}
