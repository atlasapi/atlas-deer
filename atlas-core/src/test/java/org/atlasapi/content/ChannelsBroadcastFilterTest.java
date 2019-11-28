package org.atlasapi.content;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

public class ChannelsBroadcastFilterTest {

    private Id channelIdA;
    private Id channelIdB;
    private Id channelIdC;
    private ChannelGroup<ChannelGroupMembership> channelGroup;
    private DateTime now;

    private ChannelsBroadcastFilter filter;

    @Before
    public void setUp() throws Exception {
        channelIdA = Id.valueOf(1L);
        channelIdB = Id.valueOf(2L);
        channelIdC = Id.valueOf(3L);

        channelGroup = new ChannelGroup<>(
                Id.valueOf(42L),
                Publisher.METABROADCAST,
                ImmutableSet.of(
                        getChannelGroupMembership(channelIdA),
                        getChannelGroupMembership(channelIdB)
                ),
                ImmutableSet.of(),
                ImmutableSet.of()
        );

        now = DateTime.now();
        filter = ChannelsBroadcastFilter.create();
    }

    @Test
    @Ignore // Removed as part of ENG-574
    public void broadcastsOnSameChannelWithSameStartDateTimeAreFilteredOut() throws Exception {
        Broadcast broadcastA = broadcast(channelIdA, now);
        Broadcast broadcastB = broadcast(channelIdA, now);

        ImmutableList<Broadcast> filteredBroadcasts = filterBroadcasts(broadcastA, broadcastB);

        assertThat(filteredBroadcasts.size(), is(1));
    }

    @Test
    @Ignore // Removed as part of ENG-574
    public void broadcastsOnDifferentChannelWithSameStartDateTimeAreFilteredOut() throws Exception {
        Broadcast broadcastA = broadcast(channelIdA, now);
        Broadcast broadcastB = broadcast(channelIdB, now);

        ImmutableList<Broadcast> filteredBroadcasts = filterBroadcasts(broadcastA, broadcastB);

        assertThat(filteredBroadcasts.size(), is(1));
    }

    @Test
    public void broadcastsAreSortedByStartDateTime() throws Exception {
        Broadcast broadcastA = broadcast(channelIdA, now.plusHours(1));
        Broadcast broadcastB = broadcast(channelIdA, now);
        Broadcast broadcastC = broadcast(channelIdB, now.plusHours(2));
        Broadcast broadcastD = broadcast(channelIdB, now.minusHours(2));

        ImmutableList<Broadcast> filteredBroadcasts = filterBroadcasts(
                broadcastA,
                broadcastB,
                broadcastC,
                broadcastD
        );

        assertThat(filteredBroadcasts.get(0), sameInstance(broadcastD));
        assertThat(filteredBroadcasts.get(1), sameInstance(broadcastB));
        assertThat(filteredBroadcasts.get(2), sameInstance(broadcastA));
        assertThat(filteredBroadcasts.get(3), sameInstance(broadcastC));
    }

    @Test
    public void broadcastsOnChannelNotOnChannelGroupAreFilteredOut() throws Exception {
        Broadcast broadcastA = broadcast(channelIdA, now.plusHours(1));
        Broadcast broadcastB = broadcast(channelIdC, now);

        ImmutableList<Broadcast> filteredBroadcasts = filterBroadcasts(broadcastA, broadcastB);

        assertThat(filteredBroadcasts.size(), is(1));
        assertThat(filteredBroadcasts.get(0), sameInstance(broadcastA));
    }

    private ImmutableList<Broadcast> filterBroadcasts(Broadcast... broadcasts) {
        return ImmutableList.copyOf(filter.sortAndFilter(
                    ImmutableList.copyOf(broadcasts),
                    channelGroup
            ));
    }

    private ChannelGroupMembership getChannelGroupMembership(Id channelId) {
        return new ChannelGroupMembership(
                new ChannelGroupRef(Id.valueOf(42L), Publisher.METABROADCAST),
                new ChannelRef(channelId, Publisher.METABROADCAST),
                null,
                null
        );
    }

    private Broadcast broadcast(Id channel, DateTime transmissionTime) {
        return new Broadcast(channel, transmissionTime, Duration.standardMinutes(1));
    }
}
