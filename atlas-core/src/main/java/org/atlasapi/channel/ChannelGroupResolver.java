package org.atlasapi.channel;

import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.entity.util.Resolved;

public interface ChannelGroupResolver extends IdResolver<ChannelGroup<?>> {

    ListenableFuture<Resolved<ChannelGroup<?>>> allChannels();
}
