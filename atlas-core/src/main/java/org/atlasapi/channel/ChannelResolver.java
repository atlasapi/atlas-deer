package org.atlasapi.channel;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;

public interface ChannelResolver extends IdResolver<Channel> {

    ListenableFuture<Resolved<Channel>> resolveIds(Iterable<Id> ids, Optional<String> genre);
    ListenableFuture<Resolved<Channel>> resolveChannels(ChannelQuery channelQuery);


}
