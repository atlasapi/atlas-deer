package org.atlasapi.output;

import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ResolvedChannel;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelWithChannelGroupMembership {

    private final ResolvedChannel channel;
    private final ChannelGroupMembership channelGroupMembership;

    public ResolvedChannelWithChannelGroupMembership(ResolvedChannel channel,
            ChannelGroupMembership channelGroupMembership) {
        this.channel = checkNotNull(channel);
        this.channelGroupMembership = checkNotNull(channelGroupMembership);
    }

    public ResolvedChannel getResolvedChannel() {
        return channel;
    }

    public ChannelGroupMembership getChannelGroupMembership() {
        return channelGroupMembership;
    }
}
