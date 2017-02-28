package org.atlasapi.channel;

import org.atlasapi.entity.IdResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;

import com.google.common.util.concurrent.ListenableFuture;

public interface ChannelResolver extends IdResolver<Channel> {

    ListenableFuture<Resolved<Channel>> resolveChannels(ChannelQuery channelQuery);

    ListenableFuture<Resolved<Channel>> resolveChannelsWithAliases(ChannelQuery channelQuery);

}
