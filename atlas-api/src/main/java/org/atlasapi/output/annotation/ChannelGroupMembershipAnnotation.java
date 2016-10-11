package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.Channel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupMembershipAnnotation extends OutputAnnotation<Channel> {

    private final ChannelGroupMembershipListWriter channelGroupMembershipWriter;

    public ChannelGroupMembershipAnnotation(
            ChannelGroupMembershipListWriter channelGroupMembershipWriter) {
        this.channelGroupMembershipWriter = checkNotNull(channelGroupMembershipWriter);
    }

    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        format.writeList(channelGroupMembershipWriter, entity.getChannelGroups(), ctxt);

    }
}
