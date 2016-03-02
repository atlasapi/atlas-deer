package org.atlasapi.schedule;

import java.util.List;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelSchedule {

    public static Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>> toEntries() {
        return TO_ENTRIES;
    }

    private static final Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>> TO_ENTRIES
            = new Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>>() {

        @Override
        public ImmutableList<ItemAndBroadcast> apply(ChannelSchedule input) {
            return input.entries;
        }
    };

    private final Channel channel;
    private final Interval interval;
    private final ImmutableList<ItemAndBroadcast> entries;

    public ChannelSchedule(Channel channel, Interval interval, Iterable<ItemAndBroadcast> entries) {
        this.channel = checkNotNull(channel);
        this.interval = checkNotNull(interval);
        this.entries = Ordering.natural().immutableSortedCopy(entries);
    }

    @FieldName("channel")
    public Channel getChannel() {
        return channel;
    }

    @FieldName("interval")
    public Interval getInterval() {
        return interval;
    }

    @FieldName("entries")
    public List<ItemAndBroadcast> getEntries() {
        return entries;
    }

    public ChannelSchedule copyWithEntries(Iterable<ItemAndBroadcast> entries) {
        return new ChannelSchedule(channel, interval, entries);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChannelSchedule that = (ChannelSchedule) o;
        return java.util.Objects.equals(channel, that.channel) &&
                java.util.Objects.equals(interval, that.interval) &&
                java.util.Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(channel, interval, entries);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(ChannelSchedule.class)
                .add("channel", channel)
                .add("interval", interval)
                .add("entries", entries)
                .toString();
    }
}
