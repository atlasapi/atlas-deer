package org.atlasapi.output.annotation;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
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

/**
 * Marker for {@link BroadcastsAnnotation} to only write broadcasts that have not yet broadcast.
 */
public class UpcomingBroadcastsAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter = new ChannelsBroadcastFilter();

    public UpcomingBroadcastsAnnotation(NumberToShortStringCodec codec, ChannelResolver channelResolver) {
        this.broadcastWriter = new BroadcastWriter("broadcasts", codec, channelResolver);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                    .filter(b -> ACTIVELY_PUBLISHED.apply(b))
                    .filter(b -> b.getTransmissionTime().isAfter(DateTime.now(DateTimeZone.UTC)));

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
