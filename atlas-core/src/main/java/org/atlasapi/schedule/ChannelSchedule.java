package org.atlasapi.schedule;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.Interval;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelSchedule {

    public static Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>> toEntries() {
        return TO_ENTRIES;
    }

    private static final Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>> TO_ENTRIES
            = input -> input.entries;

    private final Channel channel;
    private final Interval interval;
    private final ImmutableList<ItemAndBroadcast> entries;
    private final Publisher source;

    public ChannelSchedule(
            Channel channel,
            Interval interval,
            Iterable<ItemAndBroadcast> entries
    ) {
        this.channel = checkNotNull(channel);
        this.interval = checkNotNull(interval);
        this.entries = Ordering.natural().immutableSortedCopy(entries);
        this.source = null;
    }

    private ChannelSchedule(
            Channel channel,
            Interval interval,
            Iterable<ItemAndBroadcast> entries,
            Publisher source
    ) {
        this.channel = checkNotNull(channel);
        this.interval = checkNotNull(interval);
        this.entries = Ordering.natural().immutableSortedCopy(entries);
        this.source = source;
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

    public Publisher getSource() {
        return source;
    }

    public ChannelSchedule copyWithEntries(Iterable<ItemAndBroadcast> entries) {
        return new ChannelSchedule(channel, interval, entries, source);
    }

    public ChannelSchedule copyWithScheduleSource(Publisher source) {
        return new ChannelSchedule(channel, interval, entries, source);
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
        return MoreObjects.toStringHelper(ChannelSchedule.class)
                .add("channel", channel)
                .add("interval", interval)
                .add("entries", entries)
                .toString();
    }
}
