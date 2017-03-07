package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            List<Broadcast> broadcasts = item.getBroadcasts().stream()
                    .filter(Broadcast::isActivelyPublished)
                    .filter(b -> b.getTransmissionEndTime()
                            .isAfter(DateTime.now(DateTimeZone.UTC)))
                    .collect(MoreCollectors.toImmutableList());

            if (ctxt.getRegion().isPresent()) {

                List<ResolvedBroadcast> resolvedBroadcasts = StreamSupport.stream(
                        channelsBroadcastFilter.sortAndFilter(
                                broadcasts,
                                ctxt.getRegion().get()
                        )
                                .spliterator(), false)
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
                        broadcasts.stream()
                                .map(broadcast -> ResolvedBroadcast.create(
                                        broadcast,
                                        resolveChannel(broadcast)
                                ))
                                .collect(MoreCollectors.toImmutableList()),
                        ctxt
                );
            }
        }
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
