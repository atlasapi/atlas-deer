package org.atlasapi.output;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelWithChannelGroupMembership {

    private final Channel channel;
    private final ChannelGroupMembership channelGroupMembership;

    public ChannelWithChannelGroupMembership(Channel channel,
            ChannelGroupMembership channelGroupMembership) {
        this.channel = checkNotNull(channel);
        this.channelGroupMembership = checkNotNull(channelGroupMembership);
    }

    public Channel getChannel() {
        return channel;
    }

    public ChannelGroupMembership getChannelGroupMembership() {
        return channelGroupMembership;
    }
}
