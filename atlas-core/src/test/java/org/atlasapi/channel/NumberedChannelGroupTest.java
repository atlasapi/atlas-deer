package org.atlasapi.channel;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.intl.Country;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.LocalDate;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class NumberedChannelGroupTest {

    private static class TestNumberedChannelGroup extends NumberedChannelGroup {

        protected TestNumberedChannelGroup(Id id, Publisher publisher,
                Set<ChannelNumbering> channels, Set<Country> availableCountries,
                Set<TemporalField<String>> titles) {
            super(id, publisher, channels, availableCountries, titles);
        }
    }

    @Test
    public void testGetChannelsAvailableOn() throws Exception {

        LocalDate today = LocalDate.now();
        ChannelGroupRef channelGroup = new ChannelGroupRef(Id.valueOf(1), Publisher.METABROADCAST);
        ChannelNumbering channelNumbering1 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(1), Publisher.METABROADCAST),
                null,
                null,
                "1"
        );

        ChannelNumbering channelNumbering2 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(2), Publisher.METABROADCAST),
                today.minusDays(1),
                null,
                "2"
        );

        ChannelNumbering channelNumbering3 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(3), Publisher.METABROADCAST),
                today.minusDays(2),
                null,
                "2"
        );

        ChannelNumbering channelNumbering4 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(4), Publisher.METABROADCAST),
                today.plusDays(1),
                null,
                "3"
        );

        ChannelNumbering channelNumbering5 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(5), Publisher.METABROADCAST),
                today.minusDays(2),
                today.minusDays(1),
                "111"
        );

        TestNumberedChannelGroup objectUnderTest = new TestNumberedChannelGroup(
                channelGroup.getId(),
                channelGroup.getSource(),
                ImmutableSet.of(
                        channelNumbering5,
                        channelNumbering3,
                        channelNumbering2,
                        channelNumbering4,
                        channelNumbering1
                ),
                ImmutableSet.of(),
                ImmutableSet.of()
        );

        assertThat(
                objectUnderTest.getChannelsAvailable(today),
                is(
                        ImmutableList.of(channelNumbering1, channelNumbering2)
                )
        );

    }

    @Test
    public void testGetChannelsAvailableOnIfChannelHasNullNumbering() throws Exception {

        LocalDate today = LocalDate.now();
        ChannelGroupRef channelGroup = new ChannelGroupRef(Id.valueOf(1), Publisher.METABROADCAST);
        ChannelNumbering channelNumbering1 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(1), Publisher.METABROADCAST),
                null,
                null,
                "1"
        );

        ChannelNumbering channelNumbering2 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(2), Publisher.METABROADCAST),
                today.minusDays(1),
                null,
                "2"
        );

        ChannelNumbering channelNumbering3 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(3), Publisher.METABROADCAST),
                today.minusDays(2),
                null,
                "2"
        );

        ChannelNumbering channelNumbering4 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(4), Publisher.METABROADCAST),
                today.plusDays(1),
                null,
                "3"
        );

        ChannelNumbering channelNumbering5 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(5), Publisher.METABROADCAST),
                today.minusDays(2),
                today.minusDays(1),
                "111"
        );

        ChannelNumbering channelNumbering6 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(6), Publisher.METABROADCAST),
                today.minusDays(1),
                today.plusDays(1),
                null
        );

        ChannelNumbering channelNumbering7 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(7), Publisher.METABROADCAST),
                today.minusDays(1),
                today.plusDays(1),
                null
        );

        ChannelNumbering channelNumbering8 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(8), Publisher.METABROADCAST),
                today.minusDays(2),
                today.minusDays(1),
                null
        );

        TestNumberedChannelGroup objectUnderTest = new TestNumberedChannelGroup(
                channelGroup.getId(),
                channelGroup.getSource(),
                ImmutableSet.of(
                        channelNumbering5,
                        channelNumbering3,
                        channelNumbering2,
                        channelNumbering4,
                        channelNumbering8,
                        channelNumbering1,
                        channelNumbering6,
                        channelNumbering7
                ),
                ImmutableSet.of(),
                ImmutableSet.of()
        );

        ImmutableList<ChannelNumbering> channels = ImmutableList.copyOf(objectUnderTest.getChannelsAvailable(
                today));
        assertThat(
                channels.subList(0, 2),
                is(
                        ImmutableList.of(channelNumbering1, channelNumbering2)
                )
        );

        assertThat(
                channels.subList(2, 4),
                containsInAnyOrder(channelNumbering6, channelNumbering7)
        );

    }
}