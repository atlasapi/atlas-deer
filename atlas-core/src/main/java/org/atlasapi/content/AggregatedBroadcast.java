package org.atlasapi.content;

import org.atlasapi.channel.Channel;

import static com.google.common.base.Preconditions.checkNotNull;

public class AggregatedBroadcast {

    private final Broadcast broadcast;
    private final Channel channel;
    private final boolean channelIsParent;

    private AggregatedBroadcast(Broadcast broadcast, Channel channel) {
        this.broadcast = checkNotNull(broadcast);
        this.channel = checkNotNull(channel);
        channelIsParent = !broadcast.getChannelId().equals(channel.getId());
    }

    public static AggregatedBroadcast create(Broadcast broadcast, Channel channel) {
        return new AggregatedBroadcast(broadcast, channel);
    }

    public Broadcast getBroadcast() {
        return broadcast;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean channelIsParent() {
        return channelIsParent;
    }
}
