package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import org.atlasapi.media.channel.Channel;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

public class EquivalentChannelSchedule {

    private final Channel channel;
    private final Interval interval;
    private final ImmutableList<EquivalentScheduleEntry> entries;
    
    public EquivalentChannelSchedule(Channel channel, Interval interval, Iterable<EquivalentScheduleEntry> entries) {
        this.channel = checkNotNull(channel);
        this.interval = checkNotNull(interval);
        this.entries = Ordering.natural().immutableSortedCopy(entries);
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public Interval getInterval() {
        return interval;
    }

    public ImmutableList<EquivalentScheduleEntry> getEntries() {
        return entries;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalentChannelSchedule) {
            EquivalentChannelSchedule other = (EquivalentChannelSchedule) that;
            return channel.equals(other.channel)
                && interval.equals(other.interval)
                && entries.equals(other.entries);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return channel.hashCode();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("channel", channel)
                .add("interval", interval)
                .add("entries", entries)
                .toString();
    }

    public EquivalentChannelSchedule withLimitedBroadcasts(Integer count) {
        Iterable<EquivalentScheduleEntry> limitedEntries = Iterables.limit(entries, count);
        DateTime end = interval.getEnd();
        if (limitedEntries.iterator().hasNext()) {
            end = Iterables.getLast(limitedEntries).getBroadcast().getTransmissionEndTime();
        }
        Interval limitedInterval = new Interval(
                interval.getStart(),
                end
        );
        return new EquivalentChannelSchedule(channel, limitedInterval, limitedEntries);
    }
    
}
