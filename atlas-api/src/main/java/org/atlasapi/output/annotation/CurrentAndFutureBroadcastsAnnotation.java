package org.atlasapi.output.annotation;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;
import org.atlasapi.util.ImmutableCollectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.stream.Stream;

import static org.atlasapi.content.Broadcast.ACTIVELY_PUBLISHED;

public class CurrentAndFutureBroadcastsAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter = new ChannelsBroadcastFilter();

    public CurrentAndFutureBroadcastsAnnotation(NumberToShortStringCodec codec, ChannelResolver channelResolver, ChannelGroupResolver channelGroupResolver) {
        this.broadcastWriter = new BroadcastWriter("broadcasts", codec, channelResolver, channelGroupResolver);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                    .filter(b -> ACTIVELY_PUBLISHED.apply(b))
                    .filter(b -> b.getTransmissionEndTime().isAfter(DateTime.now(DateTimeZone.UTC)));

            if(ctxt.getRegion().isPresent()) {
                writer.writeList(
                        broadcastWriter,
                        channelsBroadcastFilter.sortAndFilter(
                                broadcastStream.collect(ImmutableCollectors.toList()),
                                ctxt.getRegion().get()
                        ),
                        ctxt
                );
            } else {
                writer.writeList(broadcastWriter, broadcastStream.collect(ImmutableCollectors.toList()), ctxt);
            }
        }
    }
}
