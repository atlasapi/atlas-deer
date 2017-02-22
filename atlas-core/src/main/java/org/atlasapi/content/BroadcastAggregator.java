package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.channel.TemporalField;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BroadcastAggregator {

    private static final Logger log = LoggerFactory.getLogger(BroadcastAggregator.class);

    private final ChannelResolver channelResolver;

    private BroadcastAggregator(ChannelResolver channelResolver) {
        this.channelResolver = channelResolver;
    }

    public static BroadcastAggregator create(ChannelResolver channelResolver) {
        return new BroadcastAggregator(channelResolver);
    }

    public Set<AggregatedBroadcast> aggregateBroadcasts(
            Set<Broadcast> broadcasts,
            Optional<Platform> platformOptional,
            List<Id> downweighChannelIds
    ) {
        // Remove broadcasts we don't care about
        if (platformOptional.isPresent()) {
            broadcasts = removeBroadcastsNotOnPlatform(broadcasts, platformOptional.get());
        }

        // Filter out previous broadcasts and collect by transmission time
        Multimap<DateTime, AggregatedBroadcast> broadcastMap = broadcasts.stream()
                .filter(broadcast -> broadcast.getTransmissionTime().isAfterNow())
                .map(broadcast -> AggregatedBroadcast.create(
                        broadcast,
                        ResolvedChannel.builder(resolveChannel(broadcast.getChannelId())).build())
                )
                .collect(MoreCollectors.toImmutableListMultiMap(
                        aggregatedBroadcast -> aggregatedBroadcast.getBroadcast().getTransmissionTime(),
                        aggregatedBroadcast -> aggregatedBroadcast

                ));

        // Aggregate the broadcasts with same transmission times
        for(DateTime dateTime : broadcastMap.keySet()) {
            Collection<AggregatedBroadcast> sameTimeBroadcasts = broadcastMap.get(dateTime);
            if (sameTimeBroadcasts.size() > 1) {
                broadcastMap.replaceValues(dateTime, aggregateBroadcastsInternal(sameTimeBroadcasts));
            }
        }

        return sortByDownweighChannelIds(downweighChannelIds, broadcastMap.values());

    }

    private Set<AggregatedBroadcast> sortByDownweighChannelIds(List<Id> ids, Collection<AggregatedBroadcast> aggregatedBroadcasts) {
        if (ids.isEmpty()) {
            return ImmutableSet.copyOf(aggregatedBroadcasts);
        }

        ImmutableSet.Builder<AggregatedBroadcast> broadcastBuilder = ImmutableSet.builder();

        Optional<AggregatedBroadcast> keyBroadcast = aggregatedBroadcasts.stream()
                .filter(aggregatedBroadcast -> ids.contains(aggregatedBroadcast.getBroadcast()
                        .getChannelId()))
                .findFirst();

        if (!keyBroadcast.isPresent()) {
            return ImmutableSet.copyOf(aggregatedBroadcasts);
        }

        return broadcastBuilder.add(keyBroadcast.get())
                .addAll(aggregatedBroadcasts.stream()
                        .filter(aggregatedBroadcast -> !aggregatedBroadcasts.equals(keyBroadcast.get()))
                        .collect(Collectors.toList()))
                .build();
    }

    private Set<AggregatedBroadcast> aggregateBroadcastsInternal(Collection<AggregatedBroadcast> broadcasts) {

        // Map parent channels to parent
        Multimap<ChannelRef, AggregatedBroadcast> parentChannelMap = broadcasts.stream()
                .collect(MoreCollectors.toImmutableListMultiMap(
                        aggregatedBroadcast -> aggregatedBroadcast.getResolvedChannel()
                                .getChannel()
                                .getParent(),
                        aggregatedBroadcast -> aggregatedBroadcast
                ));

        Set<AggregatedBroadcast> aggregatedBroadcasts = Sets.newHashSet();
        // For each parent channel, merge the children
        for (ChannelRef channelRef : parentChannelMap.keySet()) {
            aggregatedBroadcasts.add(
                    aggregateChannelsFromParent(channelRef, parentChannelMap.get(channelRef))
            );
        }

        return aggregatedBroadcasts;
    }

    @Nullable
    private Channel resolveChannel(Id id) {
        try {
            return Futures.getChecked(
                    channelResolver.resolveIds(
                            ImmutableList.of(id)
                    ),
                    IOException.class
            )
                    .getResources()
                    .first()
                    .orNull();

        } catch (IOException e) {
            log.error("Failed to resolve channel: {}", id, e);
            return null;
        }
    }

    private AggregatedBroadcast aggregateChannelsFromParent(
            ChannelRef channelRef,
            Collection<AggregatedBroadcast> broadcasts
    ) {
        // If there's just one, no need to merge
        if (broadcasts.size() == 1) {
            return Iterables.getOnlyElement(broadcasts);
        }

        Channel parent = resolveChannel(channelRef.getId());

        Map<Id, String> channelIdAndTitles = broadcasts.stream()
                .map(AggregatedBroadcast::getResolvedChannel)
                .map(ResolvedChannel::getChannel)
                .collect(MoreCollectors.toImmutableMap(Channel::getId, Channel::getTitle));

        return AggregatedBroadcast.create(
                broadcasts.iterator().next().getBroadcast(),
                buildMergedChannel(parent, channelIdAndTitles)
        );
    }

    private ResolvedChannel buildMergedChannel(Channel parent, Map<Id, String> channelIdsAndTitles) {

        ResolvedChannel.Builder resolvedChannelBuilder = ResolvedChannel.builder(
                Channel.builderFrom(parent)
                        .withTitles(ImmutableList.of(
                                new TemporalField<>(parent.getTitle(), null, null)
                        ))
                        .build()
        );

        return addChannelVariants(resolvedChannelBuilder, parent, channelIdsAndTitles).build();
    }

    private Set<Broadcast> removeBroadcastsNotOnPlatform(
            Iterable<Broadcast> broadcasts,
            Platform platform
    ) {
        List<Id> platformIds = StreamSupport.stream(platform.getChannels().spliterator(), false)
                .map(ChannelGroupMembership::getChannel)
                .map(ResourceRef::getId)
                .collect(Collectors.toList());

        return StreamSupport.stream(broadcasts.spliterator(), false)
                .filter(broadcast -> platformIds.contains(broadcast.getChannelId()))
                .collect(MoreCollectors.toImmutableSet());
    }

    private ResolvedChannel.Builder addChannelVariants(
            ResolvedChannel.Builder builder,
            Channel parent,
            Map<Id, String> children
    ) {

        builder.withIncludedVariants(
                Optional.of(getIncludedVariantRefs(parent, children.entrySet()))
        );
        builder.withExcludedVariants(
                Optional.of(resolveExcludedVariantRefs(parent, children.keySet()))
        );
        return builder;

    }

    // Create refs of resolved channels
    private List<ChannelVariantRef> getIncludedVariantRefs(
            Channel parent,
            Set<Map.Entry<Id, String>> children
    ) {

        return children.stream()
                .map(entry -> ChannelVariantRef.create(
                        parseChildTitle(parent.getTitle(), entry.getValue()),
                        entry.getKey()
                ))
                .collect(MoreCollectors.toImmutableList());
    }

    private String parseChildTitle(String parent, String child) {
        return child.replaceAll(parent, "").trim();
    }

    // Create refs of unresolved channels
    private List<ChannelVariantRef> resolveExcludedVariantRefs(Channel parent, Set<Id> ids) {
        return parent.getVariations().stream()
                .filter(channelRef -> !ids.contains(channelRef.getId()))
                .map(channelRef -> resolveChannel(channelRef.getId()))
                .map(channel -> ChannelVariantRef.create(
                        parseChildTitle(parent.getTitle(), channel.getTitle()),
                        channel.getId())
                )
                .collect(MoreCollectors.toImmutableList());
    }

}
