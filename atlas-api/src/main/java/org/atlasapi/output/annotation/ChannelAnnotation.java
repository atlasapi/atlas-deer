package org.atlasapi.output.annotation;

import org.atlasapi.channel.Channel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;


import java.io.IOException;

public class ChannelAnnotation extends OutputAnnotation<Channel> {

    private static final ChannelWriter CHANNEL_WRITER = new ChannelWriter("channels", "channel");

    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        CHANNEL_WRITER.write(entity, format, ctxt);
    }

}