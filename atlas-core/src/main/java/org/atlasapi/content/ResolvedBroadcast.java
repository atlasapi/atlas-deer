package org.atlasapi.content;

import org.atlasapi.channel.ResolvedChannel;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedBroadcast {

    private final Broadcast broadcast;
    private final ResolvedChannel resolvedChannel;

    private ResolvedBroadcast(Broadcast broadcast, ResolvedChannel resolvedChannel) {
        this.broadcast = checkNotNull(broadcast);
        this.resolvedChannel = checkNotNull(resolvedChannel);
    }

    public static ResolvedBroadcast create(Broadcast broadcast, ResolvedChannel resolvedChannel) {
        return new ResolvedBroadcast(broadcast, resolvedChannel);
    }

    public Broadcast getBroadcast() {
        return broadcast;
    }

    public ResolvedChannel getResolvedChannel() {
        return resolvedChannel;
    }
}
