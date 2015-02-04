package org.atlasapi.output.annotation;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channelgroup.ChannelGroupChannelWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelsAnnotation extends OutputAnnotation<org.atlasapi.channel.ChannelGroup> {

    private final ChannelGroupChannelWriter channelWriter;

    public ChannelGroupChannelsAnnotation(ChannelGroupChannelWriter channelWriter) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(ChannelGroup entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(channelWriter, entity.getChannels(), ctxt);
    }
}
