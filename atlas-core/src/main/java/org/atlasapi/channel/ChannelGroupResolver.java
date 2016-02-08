package org.atlasapi.channel;

import org.atlasapi.entity.IdResolver;
import org.atlasapi.entity.util.Resolved;

import com.google.common.util.concurrent.ListenableFuture;

public interface ChannelGroupResolver extends IdResolver<ChannelGroup<?>> {

    ListenableFuture<Resolved<ChannelGroup<?>>> allChannels();
}
