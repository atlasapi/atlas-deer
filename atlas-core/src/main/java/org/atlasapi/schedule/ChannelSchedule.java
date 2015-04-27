package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

public class ChannelSchedule {

    private static final Function<ChannelSchedule, Channel> TO_CHANNEL
        = new Function<ChannelSchedule, Channel>() {
            @Override
            public Channel apply(ChannelSchedule input) {
                return input.getChannel();
            }
        };
        
    public static final Function<ChannelSchedule, Channel> toChannel() {
        return TO_CHANNEL;
    }
    
    private static final Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>> TO_ENTRIES
         = new Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>>() {
            @Override
            public ImmutableList<ItemAndBroadcast> apply(ChannelSchedule input) {
                return input.entries;
            }
        };
        
    public static final Function<ChannelSchedule, ImmutableList<ItemAndBroadcast>> toEntries() {
        return TO_ENTRIES;
    }
    
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
    public boolean equals(Object obj) {
        if (obj instanceof ChannelSchedule) {
            ChannelSchedule scheduleChannel = (ChannelSchedule) obj;
            return channel.equals(scheduleChannel.channel)
                && interval.equals(interval)
                && entries.equals(scheduleChannel.entries);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(channel, interval);
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