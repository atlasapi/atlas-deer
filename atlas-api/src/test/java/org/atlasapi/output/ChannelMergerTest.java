package org.atlasapi.output;

import com.google.common.collect.ImmutableList;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

public class ChannelMergerTest {

    private ChannelMerger channelMerger;
    private List<Publisher> publisherList;

    @Before
    public void setUp() {
        this.channelMerger = ChannelMerger.create();
        this.publisherList = ImmutableList.of(
                Publisher.BT_TVE_VOD,
                Publisher.BT_TVE_VOD_VOLD_CONFIG_1,
                Publisher.BT_TVE_VOD_VOLE_CONFIG_1,
                Publisher.BT_TVE_VOD_SYSTEST2_CONFIG_1
        );
    }


    @Test
    public void channelGroupsNotInPublisherListAreFilteredOut() {

        List<Publisher> allowedPublishers = ImmutableList.of(
                Publisher.BT_TVE_VOD,
                Publisher.BT_TVE_VOD_VOLE_CONFIG_1
        );

        Iterable<ChannelGroupMembership> memberships = createMemberships();

        Channel channel = Channel.builder(Publisher.METABROADCAST)
        .withChannelGroups(memberships)
        .build();

        Channel.Builder mergedChannel = Channel.builderFrom(channel);

        channelMerger.filterChannelGroups(allowedPublishers, channel, mergedChannel);

        Channel finishedChannel = mergedChannel.build();

        Set<ChannelGroupMembership> finalMemberships = finishedChannel.getChannelGroups();

        assertThat(finalMemberships.size(), is(2));
        assertTrue(finalMemberships.stream().anyMatch(membership ->
                membership.getChannelGroup().getSource().equals(Publisher.BT_TVE_VOD))
        );
        assertTrue(finalMemberships.stream().anyMatch(membership ->
                membership.getChannelGroup().getSource().equals(Publisher.BT_TVE_VOD_VOLE_CONFIG_1))
        );
    }

    private Iterable<ChannelGroupMembership> createMemberships() {

        ImmutableList.Builder<ChannelGroupMembership> listBuilder = ImmutableList.builder();

        for (Publisher p : publisherList) {
            listBuilder.add(
                    ChannelGroupMembership.builder(p)
                    .withChannelGroupId(2L)
                    .withChannelId(1L)
                    .build()
            );
        }

        return listBuilder.build();
    }

}
