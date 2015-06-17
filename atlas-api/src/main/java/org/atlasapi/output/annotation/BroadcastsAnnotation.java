package org.atlasapi.output.annotation;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;
import org.atlasapi.util.ImmutableCollectors;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.content.Broadcast.ACTIVELY_PUBLISHED;

public class BroadcastsAnnotation extends OutputAnnotation<Content> {
    
    private final BroadcastWriter broadcastWriter;
    private final ChannelGroupResolver channelGroupResolver;
    private final NumberToShortStringCodec codec;
    private final ChannelsBroadcastFilter channelsBroadcastFilter = new ChannelsBroadcastFilter();
    
    public BroadcastsAnnotation(NumberToShortStringCodec codec, ChannelGroupResolver channelGroupResolver) {
        broadcastWriter = new BroadcastWriter("broadcasts", codec);
        this.codec = checkNotNull(codec);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
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
        Map params = ctxt.getRequest().getParameterMap();

        if (params.containsKey("broadcasts.transmissionStartTime.gt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionStartTime.gt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionTime().isAfter(time));
            }
        }

        if (params.containsKey("broadcasts.transmissionStartTime.lt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionStartTime.lt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionTime().isBefore(time));
            }
        }

        if (params.containsKey("broadcasts.transmissionEndTime.gt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionEndTime.gt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionEndTime().isAfter(time));
            }
        }

        if (params.containsKey("broadcasts.transmissionEndTime.lt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionEndTime.lt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionEndTime().isBefore(time));
            }
        }

        String regionParam = ctxt.getRequest().getParameter(Attributes.REGION.externalName());
        if(!Strings.isNullOrEmpty(regionParam)) {
            Id regionId = Id.valueOf(codec.decode(regionParam));
            Optional<ChannelGroup> channelGroup = Futures.get(
                    channelGroupResolver.resolveIds(ImmutableList.of(regionId)),
                    1, TimeUnit.MINUTES,
                    IOException.class
            ).getResources().first();
            if (!channelGroup.isPresent()) {
                Throwables.propagate(new NotFoundException(regionId));
            } else if (!(channelGroup.get() instanceof Region)) {
                Throwables.propagate(new NotAcceptableException(
                                String.format(
                                        "%s is a channel group of type '%s', should be 'region",
                                        regionParam,
                                        channelGroup.get().getType()
                                )
                        )
                );
            }
            writer.writeList(
                    broadcastWriter,
                    channelsBroadcastFilter.sortAndFilter(
                            broadcastStream.collect(Collectors.toList()),
                            channelGroup.get()
                    ),
                    ctxt
            );
            return;
        }

        writer.writeList(broadcastWriter, broadcastStream.collect(ImmutableCollectors.toList()), ctxt);
    }

}
