package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

public class PlatformAnnotation extends OutputAnnotation<ChannelGroup<?>> {

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "regions",
            "region"
    );
    private final ChannelGroupResolver channelGroupResolver;

    public PlatformAnnotation(ChannelGroupResolver channelGroupResolver) {
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
    }

    @Override
    public void write(ChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        if (!(entity instanceof Platform)) {
            return;
        }
        Platform platform = (Platform) entity;
        Iterable<Id> regionIds = Iterables.transform(
                platform.getRegions(),
                new Function<ChannelGroupRef, Id>() {

                    @Override
                    public Id apply(ChannelGroupRef input) {
                        return input.getId();
                    }
                }
        );

        Iterable<ChannelGroup<?>> channelGroups = Futures.get(
                Futures.transform(
                        channelGroupResolver.resolveIds(regionIds),
                        (Resolved<ChannelGroup<?>> input) -> {
                            return input.getResources();
                        }
                ), 1, TimeUnit.MINUTES, IOException.class
        );

        writer.writeList(CHANNEL_GROUP_WRITER, channelGroups, ctxt);
    }
}