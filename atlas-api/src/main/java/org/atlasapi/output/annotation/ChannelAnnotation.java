package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelAnnotation extends OutputAnnotation<ResolvedChannel, ResolvedChannel> {

    private final ChannelWriter channelWriter;

    public ChannelAnnotation(ChannelWriter channelWriter) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(ResolvedChannel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        channelWriter.write(entity, format, ctxt);
    }

}