package org.atlasapi.content;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import org.atlasapi.channel.Channel;
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
        Channel channel1 = channel(1L, "c1");
        Channel channel2 = channel(2L, "c2");
        Channel channel3 = channel(3L, "c3");

        List<Channel> channels = ImmutableList.of(channel1, channel2);

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
            Iterable<Broadcast> orderedAndDeduped = filter.sortAndFilter(broadcastList, channels);
            assertThat(orderedAndDeduped, is(expected));
        }
    }

    private Broadcast broadcast(Channel channel, DateTime transmissionTime) {
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