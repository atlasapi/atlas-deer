package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Optional;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ParentChannelAnnotation extends OutputAnnotation<ResolvedChannel, ResolvedChannel> {

    private final ChannelWriter channelWriter;

    public ParentChannelAnnotation(ChannelWriter channelWriter) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(ResolvedChannel entity, FieldWriter format, OutputContext ctxt) throws IOException {

       Optional<Channel> parentChannel = entity.getParentChannel();
        if (entity.getParentChannel().isPresent()) {
            ResolvedChannel resolvedParentChannel = ResolvedChannel.builder(parentChannel.get()).build();
            format.writeObject(channelWriter, "parent", resolvedParentChannel, ctxt);
        } else {
            format.writeField("parent", null);
        }

    }
}
