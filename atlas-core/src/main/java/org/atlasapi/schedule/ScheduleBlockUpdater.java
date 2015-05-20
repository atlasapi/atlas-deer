package org.atlasapi.schedule;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.joda.time.Interval;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
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
        
        Map<String, Id> validIds = index(updatedSchedule);
        Predicate<Broadcast> broadcastFilter = broadcastFilter(channel, interval);
        
        Set<ItemAndBroadcast> staleEntries = Sets.newHashSet();
        List<ChannelSchedule> updatedBlocks = Lists.newArrayListWithCapacity(currentBlocks.size());
        
        for(ChannelSchedule block : currentBlocks) {
            List<ItemAndBroadcast> survivors = survivingEntries(block, validIds, broadcastFilter);
            staleEntries.addAll(staleBroadcasts(block, survivors));
            updatedBlocks.add(block.copyWithEntries(ImmutableSet.<ItemAndBroadcast>builder()
                    .addAll(updateItems(block, updatedSchedule))
                    .addAll(survivors)
                    .build()));
        }
        for (ChannelSchedule pastBlock : staleBlocks) {
            staleEntries.addAll(pastBlock.getEntries());
        }

        return new ScheduleBlocksUpdate(updatedBlocks, staleEntries);
    }

    private Collection<ItemAndBroadcast> staleBroadcasts(ChannelSchedule schedule,
            List<ItemAndBroadcast> filteredItems) {
        return Collections2.filter(schedule.getEntries(), 
                Predicates.not(Predicates.in(filteredItems)));
    }

    private Predicate<Broadcast> broadcastFilter(Channel channel, Interval interval) {
        return Predicates.<Broadcast>and(
                Broadcast.channelFilter(channel), 
                Broadcast.intervalFilter(interval));
    }

    private List<ItemAndBroadcast> survivingEntries(ChannelSchedule schedule,
            Map<String, Id> updateBroadcastIds, Predicate<Broadcast> broadcastFilter) {
        List<ItemAndBroadcast> survivingEntries = Lists.newArrayListWithCapacity(schedule.getEntries().size());
        for (ItemAndBroadcast entry : schedule.getEntries()) {
            if (!broadcastFilter.apply(entry.getBroadcast())
                || validBroadcastId(entry, updateBroadcastIds)) {
                survivingEntries.add(entry);
            }
        }
        return survivingEntries;
    }
    
    private Iterable<ItemAndBroadcast> updateItems(ChannelSchedule schedule, List<ItemAndBroadcast> entries) {
        Predicate<Broadcast> blockFilter = broadcastFilter(schedule.getChannel(), schedule.getInterval());
        return Iterables.filter(entries, MorePredicates.transformingPredicate(ItemAndBroadcast.toBroadcast(), blockFilter));
    }

    private boolean validBroadcastId(ItemAndBroadcast iab, Map<String, Id> updateBroadcasts) {
        Id itemIdForBroadcast = updateBroadcasts.get(iab.getBroadcast().getSourceId());
        return iab.getItem().getId().equals(itemIdForBroadcast);
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
