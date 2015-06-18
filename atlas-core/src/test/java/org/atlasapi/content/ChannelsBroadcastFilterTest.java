package org.atlasapi.content;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ChannelsBroadcastFilterTest {


    @Test
    public void testSortAndFilter() {
        Id channel1 = Id.valueOf(1L);
        Id channel2 = Id.valueOf(2L);
        Id channel3 = Id.valueOf(3L);

        ChannelGroup<?> channelGroup = new ChannelGroup(
                Id.valueOf(42L),
                Publisher.METABROADCAST,
                ImmutableSet.of(
                        new ChannelGroupMembership(
                                new ChannelGroupRef(Id.valueOf(42L), Publisher.METABROADCAST),
                                new ChannelRef(channel1, Publisher.METABROADCAST),
                                null,
                                null
                        ),
                        new ChannelGroupMembership(
                                new ChannelGroupRef(Id.valueOf(42L), Publisher.METABROADCAST),
                                new ChannelRef(channel2, Publisher.METABROADCAST),
                                null,
                                null
                        )
                ),
                ImmutableSet.of(),
                ImmutableSet.of()

        );

        DateTime now = DateTime.now();

        Broadcast b1 = broadcast(channel1, now);
        Broadcast b2 = broadcast(channel2, now);
        Broadcast b3 = broadcast(channel1, now.plusHours(1));
        Broadcast b4 = broadcast(channel1, now.plusHours(2));
        Broadcast b5 = broadcast(channel2, now.plusHours(2));
        Broadcast b6 = broadcast(channel2, now.plusHours(3));
        Broadcast b7 = broadcast(channel3, now.plusHours(4));

        ImmutableList<Broadcast> broadcasts = ImmutableList.of(b1, b2, b3, b4, b5, b6, b7);

        ChannelsBroadcastFilter filter = new ChannelsBroadcastFilter();
        List<Broadcast> expected = ImmutableList.of(b1, b3, b4, b6);
        for (List<Broadcast> broadcastList : Collections2.permutations(broadcasts)) {
            Iterable<Broadcast> orderedAndDeduped = filter.sortAndFilter(broadcastList, channelGroup);
            assertThat(orderedAndDeduped, is(expected));
        }
    }

    private Broadcast broadcast(Id channel, DateTime transmissionTime) {
        Broadcast broadcast = new Broadcast(channel, transmissionTime, Duration.standardMinutes(1));
        return broadcast;
    }

    private Channel channel(Long id, String key) {
        return Channel.builder(Publisher.METABROADCAST)
                .withId(id)
                .withKey(key)
                .withUri(key)
                .build();
    }

}