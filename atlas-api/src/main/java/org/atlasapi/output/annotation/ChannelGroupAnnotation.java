package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;

public class ChannelGroupAnnotation extends OutputAnnotation<ChannelGroup<?>> {

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "channel_groups",
            "channels"
    );

    @Override
    public void write(ChannelGroup entity, FieldWriter format, OutputContext ctxt)
            throws IOException {
        CHANNEL_GROUP_WRITER.write(entity, format, ctxt);
    }
}
