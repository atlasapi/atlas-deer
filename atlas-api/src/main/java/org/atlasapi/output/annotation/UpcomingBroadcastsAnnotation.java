package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.stream.Stream;

import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Marker for {@link BroadcastsAnnotation} to only write broadcasts that have not yet broadcast.
 */
public class UpcomingBroadcastsAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter;

    public UpcomingBroadcastsAnnotation(
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        this.broadcastWriter = BroadcastWriter.create(
                "broadcasts",
                "broadcast",
                codec,
                channelResolver
        );
        this.channelsBroadcastFilter = ChannelsBroadcastFilter.create();
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                    .filter(Broadcast::isActivelyPublished)
                    .filter(b -> b.getTransmissionTime().isAfter(DateTime.now(DateTimeZone.UTC)));

            if (ctxt.getRegion().isPresent()) {
                writer.writeList(
                        broadcastWriter,
                        channelsBroadcastFilter.sortAndFilter(
                                broadcastStream.collect(MoreCollectors.toImmutableList()),
                                ctxt.getRegion().get()
                        ),
                        ctxt
                );
            } else {
                writer.writeList(
                        broadcastWriter,
                        broadcastStream.collect(MoreCollectors.toImmutableList()),
                        ctxt
                );
            }
        }
    }
}
