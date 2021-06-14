package org.atlasapi.query.v4.schedule;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelGroupQuery;

public class NoOpChannelGroupResolver implements ChannelGroupResolver {

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> allChannelGroups() {
        return Futures.immediateFuture(Resolved.empty());
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.empty());
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveChannelGroups(ChannelGroupQuery channelGroupQuery) {
        return Futures.immediateFuture(Resolved.empty());
    }
}
