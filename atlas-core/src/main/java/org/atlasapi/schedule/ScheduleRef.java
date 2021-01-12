package org.atlasapi.schedule;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.time.IntervalOrdering;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.entity.Id;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ScheduleRef {

    public static final Builder forChannel(Id channel, Interval interval) {
        return new Builder(checkNotNull(channel), checkNotNull(interval));
    }

    public static final class Builder {

        private final Id channel;
        private final Interval interval;
        private ImmutableSet.Builder<Entry> entries;

        private Builder(Id channel, Interval interval) {
            this.channel = checkNotNull(channel);
            this.interval = checkNotNull(interval);
            this.entries = ImmutableSet.builder();
        }

        public Builder addEntry(Id item, BroadcastRef broadcast) {
            this.entries.add(new Entry(item, broadcast));
            return this;
        }

        public ScheduleRef build() {
            return new ScheduleRef(
                    channel,
                    interval,
                    Ordering.natural().immutableSortedCopy(entries.build())
            );
        }
    }

    private final Id channel;
    private final Interval interval;
    private final ImmutableList<Entry> entries;

    private ScheduleRef(Id channel, Interval interval, ImmutableList<Entry> entries) {
        this.channel = channel;
        this.interval = interval;
        this.entries = entries;
    }

    public Id getChannel() {
        return channel;
    }

    public Interval getInterval() {
        return interval;
    }

    public ImmutableList<Entry> getScheduleEntries() {
        return entries;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScheduleRef that = (ScheduleRef) o;
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
        return MoreObjects.toStringHelper(this)
                .add("channel", channel)
                .add("entries", entries)
                .toString();
    }

    public static final class Entry implements Comparable<Entry> {

        private final Id item;
        private final BroadcastRef broadcast;

        public Entry(Id item, BroadcastRef broadcast) {
            this.item = checkNotNull(item);
            this.broadcast = checkNotNull(broadcast);
        }

        @Override
        public int compareTo(Entry o) {
            return ComparisonChain.start()
                    .compare(broadcast.getChannelId(), o.broadcast.getChannelId())
                    .compare(broadcast.getTransmissionInterval(),
                            o.broadcast.getTransmissionInterval(),
                            IntervalOrdering.byStartShortestFirst()
                    )
                    .result();
        }

        public Id getItem() {
            return item;
        }

        public BroadcastRef getBroadcast() {
            return broadcast;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that instanceof Entry) {
                Entry other = (Entry) that;
                return item.equals(other.item)
                        && broadcast.equals(other.broadcast);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(item, broadcast);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .omitNullValues()
                    .add("item", item)
                    .add("channel", broadcast)
                    .toString();
        }

    }
}
