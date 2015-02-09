package org.atlasapi.channel;

import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;

public interface ChannelResolver extends IdResolver<Channel> {

    ListenableFuture<Resolved<Channel>> resolveChannels(ChannelQuery channelQuery);


}
