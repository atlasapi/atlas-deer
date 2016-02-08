package org.atlasapi.schedule;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ScheduleIntervalDatesTest {

    @Test
    public void testEmptyInterval() {
        DateTime date = new DateTime(2013, 05, 28, 13, 45, 0, 0, DateTimeZones.UTC);
        assertThat(
                Iterables.getOnlyElement(new ScheduleIntervalDates(new Interval(date, date))),
                is(date.toLocalDate())
        );
    }

    @Test
    public void testSingleDayInterval() {
        DateTime start = new DateTime(2013, 05, 28, 13, 45, 0, 0, DateTimeZones.UTC);
        DateTime end = new DateTime(2013, 05, 28, 18, 45, 0, 0, DateTimeZones.UTC);
        assertThat(
                Iterables.getOnlyElement(new ScheduleIntervalDates(new Interval(start, end))),
                is(start.toLocalDate())
        );
    }

    @Test
    public void testTwoDayInterval() {
        DateTime start = new DateTime(2013, 05, 28, 13, 45, 0, 0, DateTimeZones.UTC);
        DateTime end = new DateTime(2013, 05, 29, 18, 45, 0, 0, DateTimeZones.UTC);
        assertThat(
                new ScheduleIntervalDates(new Interval(start, end)),
                hasItems(start.toLocalDate(), end.toLocalDate())
        );
    }

    @Test
    public void testMultiDayInterval() {
        DateTime start = new DateTime(2013, 05, 28, 13, 45, 0, 0, DateTimeZones.UTC);
        DateTime end = new DateTime(2013, 05, 30, 18, 45, 0, 0, DateTimeZones.UTC);
        assertThat(
                new ScheduleIntervalDates(new Interval(start, end)),
                hasItems(start.toLocalDate(), start.plusDays(1).toLocalDate(), end.toLocalDate())
        );
    }

    @Test
    public void testEmptyIntervalAtMidnight() {
        DateTime start = new DateTime(2013, 05, 28, 0, 0, 0, 0, DateTimeZones.UTC);
        DateTime end = new DateTime(2013, 05, 28, 0, 0, 0, 0, DateTimeZones.UTC);
        assertThat(
                Iterables.getOnlyElement(new ScheduleIntervalDates(new Interval(start, end))),
                is(start.toLocalDate())
        );
    }

    @Test
    public void testDayIntervalAtMidnightIsOnlyOneDay() {
        DateTime start = new DateTime(2013, 05, 28, 0, 0, 0, 0, DateTimeZones.UTC);
        DateTime end = new DateTime(2013, 05, 29, 0, 0, 0, 0, DateTimeZones.UTC);
        assertThat(
                Iterables.getOnlyElement(new ScheduleIntervalDates(new Interval(start, end))),
                is(start.toLocalDate())
        );
    }

    @Test
    public void testDayIntervalAtMidnightIsOnlyOneDayInLondon() {
        DateTime start = new DateTime(2013, 05, 28, 0, 0, 0, 0, DateTimeZones.LONDON);
        DateTime end = new DateTime(2013, 05, 29, 0, 0, 0, 0, DateTimeZones.LONDON);
        assertThat(
                Iterables.getOnlyElement(new ScheduleIntervalDates(new Interval(start, end))),
                is(start.toLocalDate())
        );
    }

}
