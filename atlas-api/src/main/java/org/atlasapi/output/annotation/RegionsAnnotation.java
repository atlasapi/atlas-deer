package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.channel.Region;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

public class RegionsAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "regions",
            "region"
    );

    private final ChannelGroupResolver channelGroupResolver;

    public RegionsAnnotation(ChannelGroupResolver channelGroupResolver) {

        this.channelGroupResolver = checkNotNull(channelGroupResolver);
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        if (!(entity.getChannelGroup() instanceof Region)) {
            return;
        }
        Region region = (Region) entity.getChannelGroup();

        Id platformId = region.getPlatform().getId();

        ChannelGroup channelGroup = Futures.get(
                Futures.transform(
                        channelGroupResolver.resolveIds(ImmutableSet.of(platformId)),
                        (Resolved<ChannelGroup<?>> input) -> {
                            return input.getResources().first().get();
                        }
                ), 1, TimeUnit.MINUTES, IOException.class
        );

        writer.writeObject(CHANNEL_GROUP_WRITER, channelGroup, ctxt);
    }
}
