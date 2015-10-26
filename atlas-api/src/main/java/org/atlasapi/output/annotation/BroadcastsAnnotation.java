package org.atlasapi.output.annotation;

import static org.atlasapi.content.Broadcast.ACTIVELY_PUBLISHED;

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
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class BroadcastsAnnotation extends OutputAnnotation<Content> {
    
    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter = new ChannelsBroadcastFilter();

    public BroadcastsAnnotation(NumberToShortStringCodec codec, ChannelResolver channelResolver, ChannelGroupResolver channelGroupResolver) {
        broadcastWriter = new BroadcastWriter("broadcasts", codec, channelResolver, channelGroupResolver);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            writeBroadcasts(writer, (Item) entity, ctxt);
        }
    }

    private void writeBroadcasts(FieldWriter writer, Item item, OutputContext ctxt) throws IOException {
        Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                .filter(b -> ACTIVELY_PUBLISHED.apply(b));

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
