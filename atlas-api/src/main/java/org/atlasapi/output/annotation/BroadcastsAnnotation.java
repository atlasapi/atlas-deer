package org.atlasapi.output.annotation;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;
import org.atlasapi.query.QueryWebModule;
import org.atlasapi.util.ImmutableCollectors;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.content.Broadcast.ACTIVELY_PUBLISHED;

public class BroadcastsAnnotation extends OutputAnnotation<Content> {
    
    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter = new ChannelsBroadcastFilter();

    public BroadcastsAnnotation(NumberToShortStringCodec codec) {
        broadcastWriter = new BroadcastWriter("broadcasts", codec);
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
