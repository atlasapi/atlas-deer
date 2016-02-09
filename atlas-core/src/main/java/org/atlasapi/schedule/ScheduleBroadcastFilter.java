package org.atlasapi.schedule;

import com.google.common.base.Predicate;
import org.joda.time.Duration;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>Predicate for filtering broadcast intervals in a schedule interval. Valid broadcast intervals
 * either entirely or partially overlap with the schedule interval or, if the schedule interval is
 * an instance, abut its end.</p>
 * <p>
 * <p>For a non-empty schedule interval the following cases are covered:</p>
 * <p>
 * <pre>
 * Schedule Interval:          SS-------------------SE
 * Totally Contained:                bs------be
 * Only Start Contained:                        bs-------be
 * Only End Contained:     bs------be
 * Exact match:                bs-------------------be
 * Entirely Overlapping:   bs----------------------------be
 * </pre>
 * <p>Empty intervals are included both at the start and end, and within the interval.</p>
 * <p>
 * <p>For an empty interval the following cases are covered:</p>
 * <pre>
 * Schedule Interval:          |
 * Contains:              bs--------be
 * Abuts end:                  bs------be
 * </pre>
 *
 * @author Fred van den Driessche (fred@metabroadcast.com)
 */
public abstract class ScheduleBroadcastFilter implements Predicate<Interval> {

    public static final ScheduleBroadcastFilter valueOf(Interval scheduleInterval) {
        checkNotNull(scheduleInterval);
        if (Duration.ZERO.equals(scheduleInterval.toDuration())) {
            return new EmptyScheduleBroadcastFilter(scheduleInterval);
        }
        return new RegularScheduleBroadcastFilter(scheduleInterval);
    }

    private static final class EmptyScheduleBroadcastFilter extends ScheduleBroadcastFilter {

        private Interval scheduleInterval;

        public EmptyScheduleBroadcastFilter(Interval scheduleInterval) {
            this.scheduleInterval = scheduleInterval;
        }

        @Override
        public boolean apply(Interval broadcastInterval) {
            return !broadcastInterval.getStart().isAfter(scheduleInterval.getStart())
                    && broadcastInterval.getEnd().isAfter(scheduleInterval.getEnd());
        }

        @Override
        public String toString() {
            return "interval covering " + scheduleInterval.getStart().toString();
        }

    }

    private static final class RegularScheduleBroadcastFilter extends ScheduleBroadcastFilter {

        private final Interval scheduleInterval;

        private RegularScheduleBroadcastFilter(Interval scheduleInterval) {
            this.scheduleInterval = scheduleInterval;
        }

        @Override
        public boolean apply(Interval broadcastInterval) {
            return scheduleInterval.overlaps(broadcastInterval)
                    || (empty(broadcastInterval) &&
                    broadcastInterval.getStart().equals(scheduleInterval.getStart())
                    || broadcastInterval.getEnd().equals(scheduleInterval.getEnd()));
        }

        private boolean empty(Interval broadcastInterval) {
            return broadcastInterval.getStart().equals(broadcastInterval.getEnd());
        }

        @Override
        public String toString() {
            return "interval in " + scheduleInterval;
        }

    }

}