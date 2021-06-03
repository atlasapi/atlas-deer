package org.atlasapi.channel;

import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelGroupQuery;

public interface ChannelGroupResolver extends IdResolver<ChannelGroup<?>> {

    ListenableFuture<Resolved<ChannelGroup<?>>> allChannelGroups();

    ListenableFuture<Resolved<ChannelGroup<?>>> resolveChannelGroups(ChannelGroupQuery channelGroupQuery);
}
