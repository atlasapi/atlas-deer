package org.atlasapi.channel;

import org.atlasapi.media.entity.Publisher;

import org.joda.time.LocalDate;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ChannelGroupMembershipTest {

    @Test
    public void testisAvailableIfGivenDateBetweenStartAndEndDate() throws Exception {
        LocalDate now = LocalDate.now();

        ChannelGroupMembership channel = ChannelGroupMembership
                .builder(Publisher.METABROADCAST)
                .withChannelId(0L)
                .withChannelGroupId(1L)
                .withChannelNumber("1")
                .withStartDate(now.minusDays(1))
                .withEndDate(now.plusDays(1))
                .build();

        assertThat(channel.isAvailable(now), is(true));
    }

    @Test
    public void testIsAvailableIfItStartsOnTheGivenDate() throws Exception {
        LocalDate now = LocalDate.now();

        ChannelGroupMembership channel = ChannelGroupMembership
                .builder(Publisher.METABROADCAST)
                .withChannelId(0L)
                .withChannelGroupId(1L)
                .withChannelNumber("1")
                .withStartDate(now)
                .withEndDate(now.plusDays(1))
                .build();

        assertThat(channel.isAvailable(now), is(true));
    }

    @Test
    public void testIsNotAvailableIfItEndsOnTheGivenDate() throws Exception {
        LocalDate now = LocalDate.now();

        ChannelGroupMembership channel = ChannelGroupMembership
                .builder(Publisher.METABROADCAST)
                .withChannelId(0L)
                .withChannelGroupId(1L)
                .withChannelNumber("1")
                .withStartDate(now.minusDays(1))
                .withEndDate(now)
                .build();

        assertThat(channel.isAvailable(now), is(false));
    }
}
