package org.atlasapi.system.legacy;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;

public class NoOpChannelResolver implements ChannelResolver {

    @Override
    public ListenableFuture<Resolved<Channel>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.empty());
    }

    @Override
    public ListenableFuture<Resolved<Channel>> resolveChannels(ChannelQuery channelQuery) {
        return Futures.immediateFuture(Resolved.empty());
    }
}
