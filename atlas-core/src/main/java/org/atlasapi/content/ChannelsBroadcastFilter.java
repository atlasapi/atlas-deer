package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.atlasapi.channel.Channel;
import org.atlasapi.entity.Id;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ChannelsBroadcastFilter {

    public Iterable<Broadcast> sortAndFilter(Iterable<Broadcast> broadcasts, List<Channel> regionChannels) {

        if(Iterables.isEmpty(broadcasts)) {
            return ImmutableList.of();
        }

        List<Id> channelIds = regionChannels.stream()
                .map(Identified::getId)
                .collect(Collectors.toList());

        Ordering<Broadcast> channelOrdering = Ordering.from(new Comparator<Broadcast>() {

            Ordering<Id> channelIdOrdering = Ordering.explicit(channelIds);

            @Override
            public int compare(Broadcast o1, Broadcast o2) {
                return channelIdOrdering.compare(o1.getChannelId(), o2.getChannelId());
            }
        });

        List<Broadcast> filteredSortedBroadcasts = StreamSupport.stream(broadcasts.spliterator(), false)
                .filter(b -> channelIds.contains(b.getChannelId()))
                .sorted(Ordering.compound(ImmutableList.of(Broadcast.startTimeOrdering(), channelOrdering)))
                .collect(Collectors.toList());

        ImmutableList.Builder<Broadcast> deduped = ImmutableList.builder();
        Broadcast currentBroadcast = null;
        for (Broadcast broadcast : filteredSortedBroadcasts) {
            if (currentBroadcast == null) {
                currentBroadcast = broadcast;
                deduped.add(broadcast);
            }
            if(currentBroadcast.getTransmissionTime().equals(broadcast.getTransmissionTime())) {
                continue;
            }

            currentBroadcast = broadcast;
            deduped.add(currentBroadcast);
        }

        return deduped.build();
    }
}
