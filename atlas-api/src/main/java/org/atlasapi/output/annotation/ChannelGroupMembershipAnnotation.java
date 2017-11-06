package org.atlasapi.output.annotation;

import java.io.IOException;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupMembershipAnnotation extends OutputAnnotation<ResolvedChannel> {

    private final ChannelGroupMembershipListWriter channelGroupMembershipWriter;
    private final FullyResolvingChannelGroupMerger channelGroupMerger;

    public ChannelGroupMembershipAnnotation(
            ChannelGroupMembershipListWriter channelGroupMembershipWriter,
            FullyResolvingChannelGroupMerger channelGroupMerger
    ) {
        this.channelGroupMembershipWriter = checkNotNull(channelGroupMembershipWriter);
        this.channelGroupMerger = checkNotNull(channelGroupMerger);
    }

    @Override
    public void write(ResolvedChannel entity, FieldWriter format, OutputContext ctxt) throws IOException {

        ImmutableSet<ChannelGroupMembership> channelGroupMemberships;

        if (ctxt.getApplication().getConfiguration().isPrecedenceEnabled()) {
            channelGroupMemberships = channelGroupMerger.resolveAndMergeChannelGroups(
                    ctxt,
                    entity.getChannel()
            );
        } else {
            channelGroupMemberships = entity.getChannel().getChannelGroups();
        }

        format.writeList(channelGroupMembershipWriter, channelGroupMemberships, ctxt);

    }
}
