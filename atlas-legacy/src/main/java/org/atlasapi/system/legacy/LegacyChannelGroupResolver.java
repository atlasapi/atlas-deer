package org.atlasapi.system.legacy;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelGroupQuery;

import java.util.Objects;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyChannelGroupResolver implements ChannelGroupResolver {

    private final org.atlasapi.media.channel.ChannelGroupResolver legacyResolver;
    private final LegacyChannelGroupTransformer transformer;

    public LegacyChannelGroupResolver(
            org.atlasapi.media.channel.ChannelGroupResolver legacyResolver,
            LegacyChannelGroupTransformer transformer
    ) {
        this.legacyResolver = checkNotNull(legacyResolver);
        this.transformer = checkNotNull(transformer);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> allChannelGroups() {
        Iterable<org.atlasapi.media.channel.ChannelGroup> resolved = legacyResolver.channelGroups();
        Iterable<ChannelGroup<?>> transformed = transformer.transform(resolved);
        return Futures.immediateFuture(Resolved.valueOf(transformed));
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveIds(Iterable<Id> ids) {
        Iterable<Long> lids = Iterables.transform(ids, Id.toLongValue());
        Iterable<org.atlasapi.media.channel.ChannelGroup> resolvedChannels = legacyResolver.channelGroupsFor(
                lids);
        Iterable<ChannelGroup<?>> transformed = transformer.transform(resolvedChannels);
        return Futures.immediateFuture(Resolved.valueOf(transformed));
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveIds(Iterable<Id> ids, boolean refreshCache) {
        if (refreshCache) {
            StreamSupport.stream(ids.spliterator(), false)
                    .map(Id.toLongValue()::apply)
                    .filter(Objects::nonNull)
                    .forEach(legacyResolver::invalidateCache);
        }

        return resolveIds(ids);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveChannelGroups(ChannelGroupQuery channelGroupQuery) {
        Iterable<org.atlasapi.media.channel.ChannelGroup> resolvedChannels = legacyResolver.channelGroupsFor(
                channelGroupQuery
        );
        Iterable<ChannelGroup<?>> transformed = transformer.transform(resolvedChannels);
        return Futures.immediateFuture(Resolved.valueOf(transformed));
    }
}
