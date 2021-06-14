package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.NumberedChannelGroup;
import org.atlasapi.entity.Id;
import org.joda.time.LocalDate;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ChannelsBroadcastFilter {

    private ChannelsBroadcastFilter() {
    }

    public static ChannelsBroadcastFilter create() {
        return new ChannelsBroadcastFilter();
    }

    public Iterable<Broadcast> sortAndFilter(
            Iterable<Broadcast> broadcasts,
            ChannelGroup<?> channelGroup,
            boolean lcnSharing
    ) {
        if (Iterables.isEmpty(broadcasts)) {
            return ImmutableList.of();
        }

        Set<? extends ChannelGroupMembership> availableChannels = channelGroup instanceof NumberedChannelGroup
                ? ((NumberedChannelGroup) channelGroup).getChannelsAvailable(LocalDate.now(), lcnSharing)
                : channelGroup.getChannelsAvailable(LocalDate.now());

        ImmutableList<Id> channelIds = availableChannels.stream()
                .map(channel -> channel.getChannel().getId())
                .distinct()
                .collect(MoreCollectors.toImmutableList());

        Ordering<Broadcast> channelOrdering = Ordering
                .explicit(channelIds)
                .onResultOf(Broadcast::getChannelId);

        List<Broadcast> filteredSortedBroadcasts = StreamSupport.stream(
                broadcasts.spliterator(),
                false
        )
                .filter(b -> channelIds.contains(b.getChannelId()))
                .sorted(Ordering.compound(ImmutableList.of(
                        Broadcast.startTimeOrdering(),
                        channelOrdering
                )))
                .collect(Collectors.toList());

        // ENG-574: This logic is dumb and causes issues (missing broadcasts), so it was removed.
        // (Maybe they had their reasons for adding them at the time, but we can't see any.)

//        ImmutableList.Builder<Broadcast> deduped = ImmutableList.builder();
//        // This logic will intentionally remove broadcasts starting at the same time on different
//        // channels. This is because channels tend to have multiple variants with the same
//        // schedule (e.g. SD and HD) and adding all of those broadcasts would significantly
//        // increase the size of the output. This logic will fail on the edge case where two
//        // broadcasts on two unrelated channels happen to start at exactly the same time.
//        DateTime lastTransmissionTime = null;
//        for (Broadcast broadcast : filteredSortedBroadcasts) {
//            if (broadcast.getTransmissionTime().equals(lastTransmissionTime)) {
//                continue;
//            }
//            lastTransmissionTime = broadcast.getTransmissionTime();
//            deduped.add(broadcast);
//        }

        return filteredSortedBroadcasts;
    }
}
