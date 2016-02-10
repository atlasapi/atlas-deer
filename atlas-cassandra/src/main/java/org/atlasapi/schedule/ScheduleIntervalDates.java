package org.atlasapi.schedule;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides the relevant {@link LocalDate}s for an {@link Interval} in terms of a schedule.
 */
public class ScheduleIntervalDates implements Iterable<LocalDate> {

    private Interval interval;

    public ScheduleIntervalDates(Interval interval) {
        this.interval = checkNotNull(interval);
    }

    @Override
    public Iterator<LocalDate> iterator() {
        return new AbstractIterator<LocalDate>() {

            private LocalDate next = interval.getStart().toLocalDate();
            private final LocalDate end = computeEnd(interval);

            private LocalDate computeEnd(Interval interval) {
                DateTime end = interval.getEnd();
                LocalDate endDay = end.toLocalDate();
                // if the end is at midnight and the interval is not empty then
                // the final day should not be included.
                if (end.getMillisOfDay() == 0 && !interval.getStart().equals(interval.getEnd())) {
                    return endDay.minusDays(1);
                }
                return endDay;
            }

            @Override
            protected LocalDate computeNext() {
                LocalDate next = this.next;
                if (next.isAfter(end)) {
                    return endOfData();
                } else {
                    this.next = this.next.plusDays(1);
                }
                return next;
            }

        };
    }

}
