package org.atlasapi.schedule;

import com.google.common.base.Predicate;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScheduleBroadcastFilterTest {

    private DateTime start = new DateTime("2014-05-06T17:00:00.000Z");
    private DateTime end = new DateTime("2014-05-06T17:30:00.000Z");

    private final Interval emptyInterval = new Interval(start, start);
    private final Interval nonEmptyInterval = new Interval(start, end);

    private final Predicate<Interval> empty = ScheduleBroadcastFilter.valueOf(emptyInterval);
    private final Predicate<Interval> nonEmpty = ScheduleBroadcastFilter.valueOf(nonEmptyInterval);

    @Test
    public void testEmptyScheduleIntervalIncludesNonEmptyInterval() {
        assertTrue(empty.apply(new Interval(start.minusMinutes(30), start.plusMinutes(30))));
    }

    @Test
    public void testEmptyScheduleIntervalIncludesNonEmptyIntervalStartingAtEmptyStart() {
        assertTrue(empty.apply(new Interval(start, start.plusMinutes(30))));
    }

    @Test
    public void testEmptyScheduleIntervalDoesntIncludeNonEmptyIntervalEndingAtEmptyStart() {
        assertFalse(empty.apply(new Interval(start.minusMinutes(30), start)));
    }

    @Test
    public void testEmptyScheduleIntervalDoesntIncludeNonEmptyIntervalBeforeEmptyStart() {
        assertFalse(empty.apply(new Interval(start.minusMinutes(60), start.minusMinutes(30))));
    }

    @Test
    public void testEmptyScheduleIntervalDoesntIncludeNonEmptyIntervalAfterEmptyStart() {
        assertFalse(empty.apply(new Interval(start.plusMinutes(30), start.plusMinutes(60))));
    }

    @Test
    public void testScheduleIntervalIncludesContainedNonEmptyInterval() {
        assertTrue(nonEmpty.apply(new Interval(start.plusMinutes(10), start.plusMinutes(20))));
    }

    @Test
    public void testScheduleIntervalIncludesNonEmptyIntervalStartingBeforeStart() {
        assertTrue(nonEmpty.apply(new Interval(start.minusMinutes(10), start.plusMinutes(10))));
    }

    @Test
    public void testScheduleIntervalIncludesNonEmptyIntervalFinishingAfterEnd() {
        assertTrue(nonEmpty.apply(new Interval(start.plusMinutes(10), start.plusMinutes(60))));
    }

    @Test
    public void testScheduleIntervalIncludesNonEmptyIntervalStartingBeforeStartAndEndingAfterEnd() {
        assertTrue(nonEmpty.apply(new Interval(start.minusMinutes(10), start.plusMinutes(60))));
    }

    @Test
    public void testScheduleIntervalDoesntIncludeNonEmptyIntervalFinishingAtIntervalStart() {
        assertFalse(nonEmpty.apply(new Interval(start.minusMinutes(60), start)));
    }

    @Test
    public void testScheduleIntervalDoesntIncludeNonEmptyIntervalStartingAtIntervalEnd() {
        assertFalse(nonEmpty.apply(new Interval(end, end.plusMinutes(10))));
    }

    @Test
    public void testScheduleIntervalDoesntIncludeNonEmptyIntervalBefore() {
        assertFalse(nonEmpty.apply(new Interval(start.minusMinutes(10), start.minusMinutes(5))));
    }

    @Test
    public void testScheduleIntervalDoesntIncludeNonEmptyIntervalAfter() {
        assertFalse(nonEmpty.apply(new Interval(end.plusMinutes(5), end.plusMinutes(10))));
    }

    @Test
    public void testScheduleIntervalIncludesEmptyInterval() {
        assertTrue(nonEmpty.apply(new Interval(start.plusMinutes(5), start.plusMinutes(5))));
    }

    @Test
    public void testScheduleIntervalIncludesEmptyIntervalAtStart() {
        assertTrue(nonEmpty.apply(new Interval(start, start)));
    }

    @Test
    public void testScheduleIntervalIncludesEmptyIntervalAtEnd() {
        assertTrue(nonEmpty.apply(new Interval(end, end)));
    }

    @Test
    public void testScheduleIntervalDoesntIncludeEmptyIntervalAfter() {
        assertFalse(nonEmpty.apply(new Interval(end.plusMinutes(10), end.plusMinutes(10))));
    }

    @Test
    public void testScheduleIntervalDoesntIncludeEmptyIntervalBefore() {
        assertFalse(nonEmpty.apply(new Interval(start.minusMinutes(10), start.minusMinutes(10))));
    }

}
