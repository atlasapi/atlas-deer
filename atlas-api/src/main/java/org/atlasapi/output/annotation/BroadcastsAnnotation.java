package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
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

public class BroadcastsAnnotation extends OutputAnnotation<Content> {

    private static final Logger log = LoggerFactory.getLogger(BroadcastsAnnotation.class);

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter;
    private final ChannelResolver channelResolver;

    private BroadcastsAnnotation(
            BroadcastWriter broadcastWriter,
            ChannelsBroadcastFilter channelsBroadcastFilter,
            ChannelResolver channelResolver
    ) {
        this.broadcastWriter = broadcastWriter;
        this.channelsBroadcastFilter = channelsBroadcastFilter;
        this.channelResolver = channelResolver;
    }

    public static BroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new BroadcastsAnnotation(
                new BroadcastWriter(
                        "broadcasts",
                        "broadcast",
                        codec
                ),
                ChannelsBroadcastFilter.create(),
                channelResolver
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
        Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                .filter(Broadcast::isActivelyPublished)
                .filter(b -> b.getTransmissionTime().isAfter(DateTime.now(DateTimeZone.UTC)));

        if (ctxt.getRegion().isPresent()) {
            List<ResolvedBroadcast> resolvedBroadcasts = StreamSupport.stream(
                    channelsBroadcastFilter.sortAndFilter(
                            broadcastStream.collect(MoreCollectors.toImmutableList()),
                            ctxt.getRegion().get()
                    ).spliterator(),
                    false
            )
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
                    broadcastStream.map(broadcast ->
                            ResolvedBroadcast.create(broadcast, resolveChannel(broadcast))
                    )
                            .collect(MoreCollectors.toImmutableList()),
                    ctxt
            );
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
