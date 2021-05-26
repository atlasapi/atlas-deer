package org.atlasapi.channel;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NumberedChannelGroupTest {

    private static class TestNumberedChannelGroup extends NumberedChannelGroup {

        protected TestNumberedChannelGroup(Id id, Publisher publisher,
                Set<ChannelNumbering> channels, Set<Country> availableCountries,
                Set<TemporalField<String>> titles) {
            super(id, publisher, channels, availableCountries, titles, null);
        }
    }

    @Test
    public void testGetChannelsAvailableOn() {

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

        assertEquals(
                ImmutableSet.of(channelNumbering1, channelNumbering2),
                objectUnderTest.getChannelsAvailable(today)
        );

    }

    @Test
    public void testGetChannelsAvailableOnIfChannelHasNullNumbering() {

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

        assertEquals(
                ImmutableSet.of(channelNumbering1, channelNumbering2, channelNumbering6, channelNumbering7),
                objectUnderTest.getChannelsAvailable(today)
        );

    }

    @Test
    public void testGetChannelsWithOrdering() {
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
                null,
                null,
                "2"
        );

        ChannelNumbering channelNumbering3 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(3), Publisher.METABROADCAST),
                null,
                null,
                "3"
        );

        TestNumberedChannelGroup objectUnderTest = new TestNumberedChannelGroup(
                channelGroup.getId(),
                channelGroup.getSource(),
                ImmutableSet.of(
                        channelNumbering3,
                        channelNumbering2,
                        channelNumbering1
                ),
                ImmutableSet.of(),
                ImmutableSet.of()
        );
        assertEquals(
                ImmutableSet.of(channelNumbering3, channelNumbering2, channelNumbering1),
                objectUnderTest.getChannels(NumberedChannelGroup.ChannelOrdering.SPECIFIED)
        );
        assertEquals(
                ImmutableSet.of(channelNumbering1, channelNumbering2, channelNumbering3),
                objectUnderTest.getChannels(NumberedChannelGroup.ChannelOrdering.CHANNEL_NUMBER)
        );
        assertEquals(
                ImmutableSet.of(channelNumbering1, channelNumbering2, channelNumbering3),
                objectUnderTest.getChannels()
        );
    }

    @Test
    public void testGetChannelsAvailableOnWithOrdering() {
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
                null,
                today.minusDays(1),
                "4"
        );

        ChannelNumbering channelNumbering5 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(5), Publisher.METABROADCAST),
                today.plusDays(1),
                null,
                "5"
        );

        TestNumberedChannelGroup objectUnderTest = new TestNumberedChannelGroup(
                channelGroup.getId(),
                channelGroup.getSource(),
                ImmutableSet.of(
                        channelNumbering3,
                        channelNumbering5,
                        channelNumbering2,
                        channelNumbering4,
                        channelNumbering1
                ),
                ImmutableSet.of(),
                ImmutableSet.of()
        );
        assertEquals(
                ImmutableSet.of(channelNumbering2, channelNumbering1),
                objectUnderTest.getChannelsAvailable(today, NumberedChannelGroup.ChannelOrdering.SPECIFIED)
        );
        assertEquals(
                ImmutableSet.of(channelNumbering1, channelNumbering2),
                objectUnderTest.getChannelsAvailable(today, NumberedChannelGroup.ChannelOrdering.CHANNEL_NUMBER)
        );
        assertEquals(
                ImmutableSet.of(channelNumbering1, channelNumbering2),
                objectUnderTest.getChannelsAvailable(today)
        );
    }


    @Test
    public void testGetChannelsWithChannelNumberOrderingPreservesSpecifiedOrderWhenNoChannelNumber() {
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
                null,
                null,
                null
        );

        ChannelNumbering channelNumbering3 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(3), Publisher.METABROADCAST),
                null,
                null,
                "3"
        );

        ChannelNumbering channelNumbering4 = new ChannelNumbering(
                channelGroup,
                new ChannelRef(Id.valueOf(4), Publisher.METABROADCAST),
                null,
                null,
                null
        );

        TestNumberedChannelGroup objectUnderTest = new TestNumberedChannelGroup(
                channelGroup.getId(),
                channelGroup.getSource(),
                ImmutableSet.of(
                        channelNumbering4,
                        channelNumbering3,
                        channelNumbering2,
                        channelNumbering1
                ),
                ImmutableSet.of(),
                ImmutableSet.of()
        );
        assertEquals(
                ImmutableSet.of(channelNumbering1, channelNumbering3, channelNumbering4, channelNumbering2),
                objectUnderTest.getChannels(NumberedChannelGroup.ChannelOrdering.CHANNEL_NUMBER)
        );
    }
}