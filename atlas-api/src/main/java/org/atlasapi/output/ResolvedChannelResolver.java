package org.atlasapi.output;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.entity.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelResolver {
    private static final Logger log = LoggerFactory.getLogger(ResolvedChannelResolver.class);
    private static final int CHANNEL_RESOLVING_RETRIES = 3;

    private final ChannelResolver channelResolver;

    public ResolvedChannelResolver(ChannelResolver channelResolver) {
        this.channelResolver = checkNotNull(channelResolver);
    }

    public ResolvedChannel resolveChannel(Id channelId) {
        return resolveChannelMap(ImmutableSet.of(channelId)).get(channelId);
    }

    //TODO: Move resolution logic to query executor?
    public Map<Id, ResolvedChannel> resolveChannelMap(Set<Id> channelIds) {
        Map<Id, Channel> channelMap = new HashMap<>(channelIds.size());

        Iterable<Channel> resolvedChannels = resolveChannels(channelIds);
        Set<Id> equivalentIds = new HashSet<>();
        for (Channel channel : resolvedChannels) {
            channelMap.put(channel.getId(), channel);
            for (ChannelEquivRef equivRef : channel.getSameAs()) {
                equivalentIds.add(equivRef.getId());
            }
        }
        Set<Id> equivalentsToResolve = Sets.difference(equivalentIds, channelMap.keySet());
        Iterable<Channel> resolvedEquivalents = resolveChannels(equivalentsToResolve);
        for (Channel channel : resolvedEquivalents) {
            channelMap.put(channel.getId(), channel);
        }

        return channelIds.stream().map(channelId -> {
            Channel channel = channelMap.get(channelId);
            if (channel == null) {
                return null;
            }
            List<Channel> equivalentChannels = channel.getSameAs().stream()
                    .map(channelEquivRef -> channelMap.get(channelEquivRef.getId()))
                    .filter(Objects::nonNull)
                    .collect(MoreCollectors.toImmutableList());
            return ResolvedChannel.builder(channel)
                    .withResolvedEquivalents(equivalentChannels)
                    .build();
        })
                .filter(Objects::nonNull)
                .collect(MoreCollectors.toImmutableMap(
                        resolvedChannel -> resolvedChannel.getChannel().getId(),
                        resolvedChannel -> resolvedChannel
                ));

    }

    private Iterable<Channel> resolveChannels(Set<Id> channelIds) {
        if (channelIds.isEmpty()) {
            return ImmutableList.of();
        }
        for (int i = 1; i <= CHANNEL_RESOLVING_RETRIES; i++) {
            if (i > 1) {
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException e) {
                    return ImmutableList.of();
                }
            }
            try {
                return Futures.getChecked(
                        channelResolver.resolveIds(channelIds),
                        IOException.class
                ).getResources();
            } catch (IOException e) {
                log.error("Failed to resolve channels: {}", channelIds, e);
            }
        }
        return ImmutableList.of();
    }
}
