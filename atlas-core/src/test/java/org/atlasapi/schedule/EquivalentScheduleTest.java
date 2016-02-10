package org.atlasapi.schedule;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EquivalentScheduleTest {

    @Test
    public void testWithLimitedBroadcasts() throws Exception {
        DateTime originalStart = DateTime.now();
        DateTime originalEnd = originalStart.plusHours(3);
        Interval originalInterval = new Interval(originalStart, originalEnd);

        DateTime limitedSchedule1End = originalEnd.plusMinutes(15);
        DateTime limitedSchedule2End = originalEnd.plusMinutes(179);
        DateTime limitedSchedule3End = originalEnd.plusMinutes(160);

        EquivalentChannelSchedule originalChannelSchedule1 = mock(EquivalentChannelSchedule.class);
        EquivalentChannelSchedule originalChannelSchedule2 = mock(EquivalentChannelSchedule.class);
        EquivalentChannelSchedule originalChannelSchedule3 = mock(EquivalentChannelSchedule.class);

        Integer broadcastCount = 5;
        EquivalentChannelSchedule limitedChannelSchedule1 = mock(EquivalentChannelSchedule.class);
        EquivalentChannelSchedule limitedChannelSchedule2 = mock(EquivalentChannelSchedule.class);
        EquivalentChannelSchedule limitedChannelSchedule3 = mock(EquivalentChannelSchedule.class);

        when(limitedChannelSchedule1.getInterval()).thenReturn(new Interval(
                originalStart,
                limitedSchedule1End
        ));
        when(limitedChannelSchedule2.getInterval()).thenReturn(new Interval(
                originalStart,
                limitedSchedule2End
        ));
        when(limitedChannelSchedule3.getInterval()).thenReturn(new Interval(
                originalStart,
                limitedSchedule3End
        ));

        when(originalChannelSchedule1.withLimitedBroadcasts(broadcastCount)).thenReturn(
                limitedChannelSchedule1);
        when(originalChannelSchedule2.withLimitedBroadcasts(broadcastCount)).thenReturn(
                limitedChannelSchedule2);
        when(originalChannelSchedule3.withLimitedBroadcasts(broadcastCount)).thenReturn(
                limitedChannelSchedule3);

        EquivalentSchedule objectUnderTest = new EquivalentSchedule(
                ImmutableList.of(
                        originalChannelSchedule1,
                        originalChannelSchedule2,
                        originalChannelSchedule3
                ),
                originalInterval
        );

        EquivalentSchedule limitedSchedule = objectUnderTest.withLimitedBroadcasts(broadcastCount);

        assertThat(
                limitedSchedule.channelSchedules(),
                contains(
                        limitedChannelSchedule1,
                        limitedChannelSchedule2,
                        limitedChannelSchedule3
                )
        );

        assertThat(limitedSchedule.interval().getEnd(), is(limitedSchedule2End));

    }
}