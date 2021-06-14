package org.atlasapi.content;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
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
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
            Optional<List<Platform>> platformsOptional,
            List<Id> downweighChannelIds,
            boolean includePastBroadcasts
    ) {

        // Remove broadcasts we don't care about
        Set<Broadcast> platformBroadcasts = platformsOptional.isPresent()
                ? removeBroadcastsNotOnPlatform(broadcasts, platformsOptional.get())
                : broadcasts;

        // Merge broadcast continuations
        Stream<Broadcast> mergedContinuations = mergeBroadcastContinuations(platformBroadcasts).stream();

        // Filter out previous broadcasts and collect by transmission time
        if (!includePastBroadcasts) {
            mergedContinuations = removePastBroadcasts(mergedContinuations);
        }

        Multimap<DateTime, ResolvedBroadcast> broadcastMap = mergedContinuations.map(
                broadcast -> ResolvedBroadcast.create(
                        broadcast,
                        ResolvedChannel.builder(resolveChannel(broadcast.getChannelId())).build())
                )
                .filter(resolved -> isNotTimeshiftedOrHd(resolved.getResolvedChannel().getChannel()))
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
                        aggregateBroadcastsInternal(sameTimeBroadcasts, platformsOptional)
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

    Stream<Broadcast> removePastBroadcasts(Stream<Broadcast> mergedContinuations) {
        return mergedContinuations.filter(broadcast -> broadcast.getTransmissionEndTime().isAfterNow());
    }

    private Set<ResolvedBroadcast> aggregateBroadcastsInternal(
            Collection<ResolvedBroadcast> broadcasts,
            Optional<List<Platform>> platformsOptional
    ) {

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
                    aggregateChannelsFromParent(
                            channelRef,
                            parentChannelMap.get(channelRef),
                            platformsOptional
                    )
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
            Collection<ResolvedBroadcast> broadcasts,
            Optional<List<Platform>> platformsOptional
    ) {
        // If there's just one, no need to merge
        if (broadcasts.size() == 1) {
            return Iterables.getOnlyElement(broadcasts);
        }

        Channel parent = resolveChannel(channelRef.getId());

        Map<Id, String> channelIdAndTitles = broadcasts.stream()
                .map(ResolvedBroadcast::getResolvedChannel)
                .map(ResolvedChannel::getChannel)
                .filter(this::isNotTimeshiftedOrHd)
                .collect(Collectors.toMap(
                        Channel::getId,
                        Channel::getTitle,
                        (id1, id2) -> {
                            log.warn("duplicate key found for channel {}", id1);
                            return id1;
                        }
                ));

        return ResolvedBroadcast.create(
                broadcasts.iterator().next().getBroadcast(),
                buildResolvedChannel(parent, ImmutableMap.copyOf(channelIdAndTitles), platformsOptional)
        );
    }

    private ResolvedChannel buildResolvedChannel(
            Channel parent,
            Map<Id, String> channelIdsAndTitles,
            Optional<List<Platform>> platformsOptional
    ) {

        return ResolvedChannel.builder(parent)
                .withIncludedVariants(
                        getIncludedVariantRefs(parent, channelIdsAndTitles.entrySet())
                )
                .withExcludedVariants(
                        resolveExcludedVariantRefs(
                                parent,
                                channelIdsAndTitles.keySet(),
                                platformsOptional
                        )
                )
                .build();

    }

    Set<Broadcast> removeBroadcastsNotOnPlatform(
            Iterable<Broadcast> broadcasts,
            List<Platform> platforms
    ) {
        List<ChannelGroupMembership> channels = Lists.newArrayList();
        platforms.forEach(platform -> Iterables.addAll(channels, platform.getChannels()));

        List<Id> platformIds = channels.stream()
                .map(ChannelGroupMembership::getChannel)
                .map(ResourceRef::getId)
                .collect(Collectors.toList());

        return StreamSupport.stream(broadcasts.spliterator(), false)
                .filter(broadcast -> platformIds.contains(broadcast.getChannelId()))
                .collect(MoreCollectors.toImmutableSet());
    }

    Set<Broadcast> mergeBroadcastContinuations(Collection<Broadcast> broadcasts) {
        return broadcasts.stream()
                .sorted(Comparator.comparing(Broadcast::getTransmissionEndTime))
                .collect(
                        Collector.<Broadcast, ListMultimap<Id, Broadcast>, Set<Broadcast>>of(
                                ArrayListMultimap::create,
                                this::accumulateBroadcastContinuations,
                                this::combineBroadcastContinuations,
                                channelId2broadcasts ->
                                        ImmutableSet.copyOf(channelId2broadcasts.values())));
    }

    private void accumulateBroadcastContinuations(
            ListMultimap<Id, Broadcast> channelId2Broadcast,
            Broadcast broadcast
    ) {
        // This is a Collector.accumulator method, the second argument is folded into the first.
        List<Broadcast> broadcasts = channelId2Broadcast.get(broadcast.getChannelId());
        // Note: we can't use !broadcast.getContinuation() as it can be null
        if (broadcasts.isEmpty() || !Boolean.TRUE.equals(broadcast.getContinuation())) {
            // If we don't have a broadcast on the channel yet, or if it's not a continuation,
            // just add it.
            broadcasts.add(broadcast);
        } else {
            // Otherwise remove the previous broadcast, merge it with this one, and add the result.
            int last = broadcasts.size() - 1;
            Broadcast original = broadcasts.get(last);
            broadcasts.set(
                    last,
                    original.copyWithNewInterval(
                            new Interval(
                                    original.getTransmissionTime(),
                                    broadcast.getTransmissionEndTime())));
        }
    }

    private ListMultimap<Id, Broadcast> combineBroadcastContinuations(
            ListMultimap<Id, Broadcast> broadcasts1,
            ListMultimap<Id, Broadcast> broadcasts2
    ) {
        // This is a Collector.combiner method, the second argument is folded into the first.
        broadcasts2.values().forEach(b -> accumulateBroadcastContinuations(broadcasts1, b));
        return broadcasts1;
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
    List<ChannelVariantRef> resolveExcludedVariantRefs(
            Channel parent,
            Set<Id> ids,
            Optional<List<Platform>> platformsOptional
    ) {

        List<Id> platformsIds = Lists.newArrayList();
        platformsOptional.ifPresent(platforms -> platforms.forEach(
                platform -> platformsIds.addAll(
                        platform.getChannels()
                                .stream()
                                .map(channelNumbering -> channelNumbering.getChannel().getId())
                                .collect(MoreCollectors.toImmutableList())
                )
        ));

        return parent.getVariations().stream()
                .filter(channelRef -> !ids.contains(channelRef.getId()))
                .filter(channelRef -> platformsIds.isEmpty() || platformsIds.contains(channelRef.getId()))
                .map(channelRef -> resolveChannel(channelRef.getId()))
                .filter(this::isNotTimeshiftedOrHd)
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

    boolean isNotTimeshiftedOrHd(Channel channel) {
        boolean timeshifted = MoreObjects.firstNonNull(channel.getTimeshifted(), false);
        boolean hd = MoreObjects.firstNonNull(channel.getHighDefinition(), false);

        return !(timeshifted || hd);
    }

}
