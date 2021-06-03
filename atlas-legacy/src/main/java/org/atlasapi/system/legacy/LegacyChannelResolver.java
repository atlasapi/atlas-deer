package org.atlasapi.system.legacy;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyChannelResolver implements ChannelResolver {

    private final org.atlasapi.media.channel.ChannelResolver legacyResolver;
    private final LegacyChannelTransformer transformer;

    public LegacyChannelResolver(
            org.atlasapi.media.channel.ChannelResolver legacyResolver,
            LegacyChannelTransformer transformer
    ) {
        this.legacyResolver = checkNotNull(legacyResolver);
        this.transformer = checkNotNull(transformer);
    }

    @Override
    public ListenableFuture<Resolved<Channel>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(
                Resolved.valueOf(
                        resolveAndTransformLegacyChannels(ids)
                )
        );
    }

    @Override
    public ListenableFuture<Resolved<Channel>> resolveIds(Iterable<Id> ids, boolean refreshCache) {
        if (refreshCache) {
            legacyResolver.refreshCache();
        }
        return resolveIds(ids);
    }

    private Iterable<Channel> resolveAndTransformLegacyChannels(Iterable<Id> ids) {
        Iterable<Long> lids = Iterables.transform(ids, Id.toLongValue());
        Iterable<org.atlasapi.media.channel.Channel> resolvedChannels = legacyResolver.forIds(lids);
        return transformer.transform(resolvedChannels);
    }

    @Override
    public ListenableFuture<Resolved<Channel>> resolveChannels(ChannelQuery channelQuery) {
        Iterable<org.atlasapi.media.channel.Channel> resolvedChannels = legacyResolver.allChannels(
                channelQuery);
        Iterable<Channel> transformed = transformer.transform(resolvedChannels);
        return Futures.immediateFuture(Resolved.valueOf(transformed));
    }

    @Override
    public ListenableFuture<Resolved<Channel>> resolveChannelsWithAliases(
            ChannelQuery channelQuery
    ) {
        Iterable<org.atlasapi.media.channel.Channel> resolvedChannels = legacyResolver.forKeyPairAlias(
                channelQuery
        );
        Iterable<Channel> transformed = transformer.transform(resolvedChannels);

        return Futures.immediateFuture(Resolved.valueOf(transformed));
    }
}
