package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.stream.Stream;

import org.atlasapi.channel.ChannelGroupResolver;
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

public class CurrentAndFutureBroadcastsAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter = new ChannelsBroadcastFilter();

    public CurrentAndFutureBroadcastsAnnotation(NumberToShortStringCodec codec,
            ChannelResolver channelResolver, ChannelGroupResolver channelGroupResolver) {
        this.broadcastWriter = new BroadcastWriter(
                "broadcasts",
                codec,
                channelResolver,
                channelGroupResolver
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

            if (ctxt.getRegion().isPresent()) {
                writer.writeList(
                        broadcastWriter,
                        channelsBroadcastFilter.sortAndFilter(
                                broadcastStream.collect(MoreCollectors.toList()),
                                ctxt.getRegion().get()
                        ),
                        ctxt
                );
            } else {
                writer.writeList(
                        broadcastWriter,
                        broadcastStream.collect(MoreCollectors.toList()),
                        ctxt
                );
            }
        }
    }
}
