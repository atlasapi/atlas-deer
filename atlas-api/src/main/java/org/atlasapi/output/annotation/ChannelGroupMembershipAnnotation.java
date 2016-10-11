package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupMembershipAnnotation extends OutputAnnotation<ResolvedChannel> {

    private final ChannelGroupMembershipListWriter channelGroupMembershipWriter;

    public ChannelGroupMembershipAnnotation(
            ChannelGroupMembershipListWriter channelGroupMembershipWriter) {
        this.channelGroupMembershipWriter = checkNotNull(channelGroupMembershipWriter);
    }

    @Override
    public void write(ResolvedChannel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        format.writeList(channelGroupMembershipWriter, entity.getChannel().getChannelGroups(), ctxt);

    }
}
