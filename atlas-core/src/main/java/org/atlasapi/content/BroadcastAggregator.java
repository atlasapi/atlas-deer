package org.atlasapi.content;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BroadcastAggregator {

    private static final Logger log = LoggerFactory.getLogger(BroadcastAggregator.class);

    private static final Set<String> INVALID_CHILD_NAMES = ImmutableSet.of("", "hd", "+1");

    private final ChannelResolver channelResolver;

    private BroadcastAggregator(ChannelResolver channelResolver) {
        this.channelResolver = channelResolver;
    }

    public static BroadcastAggregator create(ChannelResolver channelResolver) {
        return new BroadcastAggregator(channelResolver);
    }

    public Set<ResolvedBroadcast> aggregateBroadcasts(
            Set<Broadcast> broadcasts,
            Optional<Platform> platformOptional,
            List<Id> downweighChannelIds
    ) {
        // Remove broadcasts we don't care about
        if (platformOptional.isPresent()) {
            broadcasts = removeBroadcastsNotOnPlatform(broadcasts, platformOptional.get());
        }

        // Filter out previous broadcasts and collect by transmission time
        Multimap<DateTime, ResolvedBroadcast> broadcastMap = broadcasts.stream()
                .filter(broadcast -> broadcast.getTransmissionTime().isAfterNow())
                .map(broadcast -> ResolvedBroadcast.create(
                        broadcast,
                        ResolvedChannel.builder(resolveChannel(broadcast.getChannelId())).build())
                )
                .collect(MoreCollectors.toImmutableListMultiMap(
                        resolvedBroadcast -> resolvedBroadcast.getBroadcast().getTransmissionTime(),
                        resolvedBroadcast -> resolvedBroadcast

                ));

        // Aggregate the broadcasts with same transmission times

        ImmutableMultimap.Builder<DateTime, ResolvedBroadcast> aggregatedBroadcasts = ImmutableMultimap.builder();

        for(DateTime dateTime : broadcastMap.keySet()) {
            Collection<ResolvedBroadcast> sameTimeBroadcasts = broadcastMap.get(dateTime);
            if (sameTimeBroadcasts.size() > 1) {
                aggregatedBroadcasts.putAll(
                        dateTime,
                        aggregateBroadcastsInternal(sameTimeBroadcasts)
                );
            } else {
                aggregatedBroadcasts.put(dateTime, Iterables.getOnlyElement(sameTimeBroadcasts));
            }
        }

        return sortByDownweighChannelIds(
                downweighChannelIds,
                sortBroadcastsByDateTime(aggregatedBroadcasts.build())
        );

    }

    private Set<ResolvedBroadcast> aggregateBroadcastsInternal(Collection<ResolvedBroadcast> broadcasts) {

        // Map parent channels to parent
        Multimap<ChannelRef, ResolvedBroadcast> parentChannelMap = broadcasts.stream()
                .collect(MoreCollectors.toImmutableListMultiMap(
                        resolvedBroadcast -> MoreObjects.firstNonNull(
                                resolvedBroadcast.getResolvedChannel()
                                        .getChannel()
                                        .getParent(),
                                resolvedBroadcast.getResolvedChannel()
                                        .getChannel()
                                        .toRef()
                        ),
                        resolvedBroadcast -> resolvedBroadcast
                ));

        Set<ResolvedBroadcast> resolvedBroadcasts = Sets.newHashSet();
        // For each parent channel, merge the children
        for (ChannelRef channelRef : parentChannelMap.keySet()) {
            resolvedBroadcasts.add(
                    aggregateChannelsFromParent(channelRef, parentChannelMap.get(channelRef))
            );
        }

        return resolvedBroadcasts;
    }

    Set<ResolvedBroadcast> sortByDownweighChannelIds(List<Id> ids, Collection<ResolvedBroadcast> resolvedBroadcasts) {
        if (ids.isEmpty()) {
            return ImmutableSet.copyOf(resolvedBroadcasts);
        }

        ImmutableSet.Builder<ResolvedBroadcast> broadcastBuilder = ImmutableSet.builder();

        Optional<ResolvedBroadcast> keyBroadcast = resolvedBroadcasts.stream()
                .filter(resolvedBroadcast ->
                        variantIdIsNotDownweighed(ids, resolvedBroadcast)
                        && !ids.contains(resolvedBroadcast.getResolvedChannel()
                                .getChannel()
                                .getId()
                        )
                )
                .findFirst();

        if (!keyBroadcast.isPresent()
                || resolvedBroadcasts.iterator().next().equals(keyBroadcast.get())) {
            return ImmutableSet.copyOf(resolvedBroadcasts);
        }

        return broadcastBuilder.add(keyBroadcast.get())
                .addAll(resolvedBroadcasts.stream()
                        .filter(resolvedBroadcast -> !resolvedBroadcasts.equals(keyBroadcast.get()))
                        .collect(Collectors.toList()))
                .build();
    }

    Collection<ResolvedBroadcast> sortBroadcastsByDateTime(ImmutableMultimap<DateTime, ResolvedBroadcast> broadcastMap) {

        List<DateTime> dateTimes = Lists.newArrayList(broadcastMap.keySet());
        dateTimes.sort(DateTimeComparator.getInstance());

        Ordering<ResolvedBroadcast> dateTimeOrdering = Ordering.explicit(dateTimes)
                .onResultOf(resolvedBroadcast ->
                        resolvedBroadcast.getBroadcast().getTransmissionTime()
                );

        return dateTimeOrdering.sortedCopy(broadcastMap.values());
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

    private ResolvedBroadcast aggregateChannelsFromParent(
            ChannelRef channelRef,
            Collection<ResolvedBroadcast> broadcasts
    ) {
        // If there's just one, no need to merge
        if (broadcasts.size() == 1) {
            return Iterables.getOnlyElement(broadcasts);
        }

        Channel parent = resolveChannel(channelRef.getId());

        Map<Id, String> channelIdAndTitles = broadcasts.stream()
                .map(ResolvedBroadcast::getResolvedChannel)
                .map(ResolvedChannel::getChannel)
                .collect(MoreCollectors.toImmutableMap(Channel::getId, Channel::getTitle));

        return ResolvedBroadcast.create(
                broadcasts.iterator().next().getBroadcast(),
                buildResolvedChannel(parent, channelIdAndTitles)
        );
    }

    private ResolvedChannel buildResolvedChannel(Channel parent, Map<Id, String> channelIdsAndTitles) {

        return ResolvedChannel.builder(parent)
                .withIncludedVariants(
                        Optional.of(getIncludedVariantRefs(parent, channelIdsAndTitles.entrySet()))
                )
                .withExcludedVariants(
                        Optional.of(resolveExcludedVariantRefs(parent, channelIdsAndTitles.keySet()))
                )
                .build();

    }

    Set<Broadcast> removeBroadcastsNotOnPlatform(
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

    // Create refs of resolved channels
    List<ChannelVariantRef> getIncludedVariantRefs(
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

    // Create refs of unresolved channels
    List<ChannelVariantRef> resolveExcludedVariantRefs(Channel parent, Set<Id> ids) {
        return parent.getVariations().stream()
                .filter(channelRef -> !ids.contains(channelRef.getId()))
                .map(channelRef -> resolveChannel(channelRef.getId()))
                .map(channel -> ChannelVariantRef.create(
                        parseChildTitle(parent.getTitle(), channel.getTitle()),
                        channel.getId())
                )
                .collect(MoreCollectors.toImmutableList());
    }

    String parseChildTitle(String parentTitle, String childTitle) {
        String parsedTitle = childTitle.replaceAll(parentTitle, "").trim();

        return INVALID_CHILD_NAMES.containsAll(
                Arrays.asList(parsedTitle.toLowerCase().split(" "))
        )
               ? childTitle
               : parsedTitle;
    }

    private boolean variantIdIsNotDownweighed(List<Id> downweighedIds, ResolvedBroadcast resolvedBroadcast) {

        List<Id> includedVariantIds = resolvedBroadcast.getResolvedChannel()
                .getIncludedVariants().orElse(ImmutableList.of())
                .stream()
                .map(ChannelVariantRef::getId)
                .collect(MoreCollectors.toImmutableList());

        return downweighedIds.stream().noneMatch(includedVariantIds::contains);
    }

}
