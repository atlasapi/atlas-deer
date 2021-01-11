package org.atlasapi.schedule;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentSchedule {

    private final Interval interval;
    private final List<EquivalentChannelSchedule> channelSchedules;

    public EquivalentSchedule(List<EquivalentChannelSchedule> channelSchedules, Interval interval) {
        this.channelSchedules = ImmutableList.copyOf(channelSchedules);
        this.interval = checkNotNull(interval);
    }

    public Interval interval() {
        return interval;
    }

    public List<EquivalentChannelSchedule> channelSchedules() {
        return this.channelSchedules;
    }

    @Override
    public int hashCode() {
        return channelSchedules.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalentSchedule) {
            EquivalentSchedule other = (EquivalentSchedule) that;
            return channelSchedules.equals(other.channelSchedules);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(channelSchedules)
                .toString();
    }

    public EquivalentSchedule withLimitedBroadcasts(final Integer count) {
        Iterable<EquivalentChannelSchedule> limitedChannelSchedules = Iterables.transform(
                channelSchedules,
                new Function<EquivalentChannelSchedule, EquivalentChannelSchedule>() {

                    @Override
                    public EquivalentChannelSchedule apply(EquivalentChannelSchedule input) {
                        return input.withLimitedBroadcasts(count);
                    }
                }
        );

        DateTime newEndTime = interval.getStart();
        for (EquivalentChannelSchedule limitedChannelSchedule : limitedChannelSchedules) {
            DateTime scheduleEndTime = limitedChannelSchedule.getInterval().getEnd();
            if (scheduleEndTime.isAfter(newEndTime)) {
                newEndTime = scheduleEndTime;
            }
        }
        Interval newScheduleInterval = new Interval(interval.getStart(), newEndTime);

        return new EquivalentSchedule(
                ImmutableList.copyOf(limitedChannelSchedules),
                newScheduleInterval
        );
    }

}
