package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupChannelIdsWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelIdsAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private final ChannelGroupChannelIdsWriter channelIdsWriter;

    public ChannelGroupChannelIdsAnnotation(ChannelGroupChannelIdsWriter channelIdsWriter) {
        this.channelIdsWriter = checkNotNull(channelIdsWriter);
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeList(channelIdsWriter, entity.getChannels().get(), ctxt);
    }
}
