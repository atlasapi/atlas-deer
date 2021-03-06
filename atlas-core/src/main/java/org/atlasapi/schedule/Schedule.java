package org.atlasapi.schedule;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.ItemAndBroadcast;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

public final class Schedule {

    private final Interval interval;
    private final List<ChannelSchedule> channelSchedules;

    public static Schedule fromChannelMap(Map<Channel, Collection<ItemAndBroadcast>> channelMap,
            Interval interval) {
        ImmutableList.Builder<ChannelSchedule> scheduleChannels = ImmutableList.builder();
        for (Entry<Channel, Collection<ItemAndBroadcast>> channel : channelMap.entrySet()) {
            scheduleChannels.add(new ChannelSchedule(
                    channel.getKey(),
                    interval,
                    channel.getValue()
            ));
        }
        return new Schedule(scheduleChannels.build(), interval);
    }

    public Schedule(List<ChannelSchedule> channelSchedules, Interval interval) {
        this.channelSchedules = ImmutableList.copyOf(channelSchedules);
        this.interval = checkNotNull(interval);
    }

    public Interval interval() {
        return interval;
    }

    public List<ChannelSchedule> channelSchedules() {
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
        if (that instanceof Schedule) {
            Schedule other = (Schedule) that;
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
}
