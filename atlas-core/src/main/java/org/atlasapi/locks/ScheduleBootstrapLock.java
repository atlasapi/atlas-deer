package org.atlasapi.locks;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSortedSet;
import org.atlasapi.channel.Channel;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleBootstrapLock {
    private ConcurrentHashMap<Key, ReentrantLock> lockMap = new ConcurrentHashMap<>();

    private ReentrantLock getLock(Key key) {
        return lockMap.computeIfAbsent(key, (k) -> new ReentrantLock());
    }

    //Locks keys in a sorted order to prevent deadlocking
    public void lock(Channel channel, String source, Interval interval) {
        for(Key k : getKeys(channel, source, interval)) {
            getLock(k).lock();
        }
    }

    public boolean tryLock(Channel channel, String source, Interval interval) {
        NavigableSet<Key> keys = getKeys(channel, source, interval);
        for(Key key : keys) {
            if(!getLock(key).tryLock()) {
                unlockInternal(keys.headSet(key));
                return false;
            }
        }
        return true;
    }

    public void unlock(Channel channel, String source, Interval interval) {
        unlockInternal(getKeys(channel, source, interval));
    }

    private void unlockInternal(SortedSet<Key> keys) {
        IllegalMonitorStateException ex = null;
        for(Key key : keys) {
            try {
                getLock(key).unlock();
            } catch(IllegalMonitorStateException e) {
                if(ex == null) {
                    ex = e;
                } else {
                    ex.addSuppressed(e);
                }
            }
        }
        if(ex != null) {
            throw ex;
        }
    }

    //All days in the interval
    private NavigableSet<Key> getKeys(Channel channel, String source, Interval interval) {
        long channelId = channel.getId().longValue();
        LocalDate startDate = interval.getStart().toLocalDate();
        LocalDate endDate = interval.getEnd().toLocalDate();
        ImmutableSortedSet.Builder<Key> keys = new ImmutableSortedSet.Builder<>(Key::compareTo);
        for(LocalDate date = startDate; !startDate.isAfter(endDate); date = date.plusDays(1)) {
            keys.add(new Key(channelId, source, date));
        }
        return keys.build();
    }


    private class Key implements Comparable<Key> {
        private final Long channelId;
        private final String source;
        private final LocalDate date;

        Key(Long channelId, String source, LocalDate date) {
            this.channelId = checkNotNull(channelId);
            this.source = checkNotNull(source);
            this.date = checkNotNull(date);

        }

        @Override
        public boolean equals(Object that) {
            if(this == that) {
                return true;
            }
            if(that != null && getClass().equals(that.getClass())) {
                Key key = (Key) that;
                if(this.channelId.equals(key.channelId) &&
                        this.source.equals(key.source) &&
                        this.date.equals(key.date)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(channelId, date);
        }

        @Override
        public int compareTo(Key that) {
            int comp = this.channelId.compareTo(that.channelId);
            if (comp != 0) return comp;
            comp = this.source.compareTo(that.source);
            if (comp != 0) return comp;
            comp = this.date.compareTo(that.date);
            if (comp != 0) return comp;
            return 0;
        }
    }

}
