package org.atlasapi.content;

import org.atlasapi.channel.ResolvedChannel;

import static com.google.common.base.Preconditions.checkNotNull;

public class AggregatedBroadcast {

    private final Broadcast broadcast;
    private final ResolvedChannel resolvedChannel;
    private final boolean channelIsParent;

    private AggregatedBroadcast(Broadcast broadcast, ResolvedChannel resolvedChannel) {
        this.broadcast = checkNotNull(broadcast);
        this.resolvedChannel = checkNotNull(resolvedChannel);
        channelIsParent = !broadcast.getChannelId().equals(resolvedChannel.getChannel().getId());
    }

    public static AggregatedBroadcast create(Broadcast broadcast, ResolvedChannel resolvedChannel) {
        return new AggregatedBroadcast(broadcast, resolvedChannel);
    }

    public Broadcast getBroadcast() {
        return broadcast;
    }

    public ResolvedChannel getResolvedChannel() {
        return resolvedChannel;
    }

    public boolean channelIsParent() {
        return channelIsParent;
    }
}
