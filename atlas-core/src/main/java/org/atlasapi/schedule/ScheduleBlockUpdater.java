package org.atlasapi.schedule;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.util.ImmutableCollectors;
import org.joda.time.Interval;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.MorePredicates;

class ScheduleBlockUpdater {

    public ScheduleBlocksUpdate updateBlocks(
            List<ChannelSchedule> currentBlocks,
            List<ChannelSchedule> staleBlocks,
            List<ItemAndBroadcast> updatedSchedule,
            Channel channel,
            Interval interval
    ) {
        
        Predicate<Broadcast> blockFilter = broadcastIntervalAndChannelFilter(channel, interval);

        Set<ItemAndBroadcast> staleEntries = Sets.newHashSet();
        ImmutableSet.Builder<ItemAndBroadcast> staleContent = ImmutableSet.builder();
        List<ChannelSchedule> updatedBlocks = Lists.newArrayListWithCapacity(currentBlocks.size());
        ImmutableSet.Builder<ItemAndBroadcast> allUpdatedItemsAndBroadcasts = ImmutableSet.builder();
        for(ChannelSchedule block : currentBlocks) {

            Iterable<ItemAndBroadcast> updatedBroadcasts = updateItems(block, updatedSchedule);
            staleEntries.addAll(
                    staleBroadcasts(
                            block,
                            updatedSchedule
                    )
            );
            staleContent.addAll(
                    staleContent(block, updatedSchedule)
            );
            ImmutableSet<ItemAndBroadcast> updatedItemAndBroadcasts = ImmutableSet.<ItemAndBroadcast>builder()
                    .addAll(broadcastOutsideUpdate(blockFilter, block.getEntries()))
                    .addAll(updatedBroadcasts)
                    .build();
            updatedBlocks.add(block.copyWithEntries(updatedItemAndBroadcasts));
            allUpdatedItemsAndBroadcasts.addAll(updatedItemAndBroadcasts);
        }

        ImmutableSet<ItemAndBroadcast> currentItemsAndBroadcasts = allUpdatedItemsAndBroadcasts.build();

        /*
        Sometimes a broadcast which was stale becomes current again
        We need to make sure that it is not returned in stale broadcasts so that the actively published flag
        is not set to false on it.
        */
        for (ChannelSchedule pastBlock : staleBlocks) {
            staleEntries.addAll(
                    Sets.difference(
                            ImmutableSet.copyOf(pastBlock.getEntries()),
                            currentItemsAndBroadcasts
                    )
            );
        }

        return new ScheduleBlocksUpdate(
                updatedBlocks,
                staleEntries,
                staleContent.build()
        );
    }

    private Iterable<ItemAndBroadcast> broadcastOutsideUpdate(Predicate<Broadcast> blockFilter, List<ItemAndBroadcast> entries) {
        return entries
                .stream()
                .filter(iab -> !blockFilter.apply(iab.getBroadcast()))
                .collect(ImmutableCollectors.toList());
    }

    /**
     * Content which is no longer broadcast in the slot it was before.
     * We need it to be able to make sure that the broadcast is marked
     * as not actively published in the content store.
     * @param block
     * @param updatedSchedule
     * @return
     */
    private Iterable<ItemAndBroadcast> staleContent(
            ChannelSchedule block,
            List<ItemAndBroadcast> updatedSchedule
    ) {
        Map<String, Id> validIds = index(updatedSchedule);
        return block.getEntries()
                .stream()
                .filter(
                        iab -> validIds.containsKey(iab.getBroadcast().getSourceId())
                                && !validIds.get(iab.getBroadcast().getSourceId()).equals(iab.getItem().getId())
                ).collect(ImmutableCollectors.toList());

    }

    /**
     * Get broadcasts that got removed from the schedule and need to be removed
     * from schedule store and equivalent schedule store.
     * @param currentSchedule
     * @param updateItems
     * @return
     */
    private Collection<ItemAndBroadcast> staleBroadcasts(
            ChannelSchedule currentSchedule,
            List<ItemAndBroadcast> updateItems
    ) {
        Predicate<Broadcast> blockFilter = broadcastIntervalAndChannelFilter(
                currentSchedule.getChannel(),
                currentSchedule.getInterval()
        );

        Set<String> validBroadcastIds = updateItems
                .stream()
                .map(iab -> iab.getBroadcast().getSourceId())
                .collect(ImmutableCollectors.toSet());

        return currentSchedule.getEntries()
                        .stream()
                        .filter(iab -> blockFilter.apply(iab.getBroadcast()))
                        .filter(iab -> !validBroadcastIds.contains(iab.getBroadcast().getSourceId()))
                        .collect(ImmutableCollectors.toList());

    }

    private Predicate<Broadcast> broadcastIntervalAndChannelFilter(Channel channel, Interval interval) {
        return Predicates.and(
                Broadcast.channelFilter(channel),
                Broadcast.intervalFilter(interval));
    }

    private Iterable<ItemAndBroadcast> updateItems(ChannelSchedule schedule, List<ItemAndBroadcast> entries) {
        Predicate<Broadcast> blockFilter = broadcastIntervalAndChannelFilter(schedule.getChannel(), schedule.getInterval());
        return Iterables.filter(entries, MorePredicates.transformingPredicate(ItemAndBroadcast.toBroadcast(), blockFilter));
    }

    private Map<String, Id> index(List<ItemAndBroadcast> broadcastsAndItems) {
        ImmutableMap.Builder<String, Id> builder = ImmutableMap.builder();
        for (ItemAndBroadcast itemAndBroadcast : broadcastsAndItems) {
            builder.put(itemAndBroadcast.getBroadcast().getSourceId(),
                    itemAndBroadcast.getItem().getId());
        }
        return builder.build();
    }
}
