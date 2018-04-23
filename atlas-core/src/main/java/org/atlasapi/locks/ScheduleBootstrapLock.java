package org.atlasapi.locks;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSortedSet;
import org.atlasapi.channel.Channel;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleBootstrapLock {
    private ConcurrentHashMap<Key, ReentrantLock> lockMap = new ConcurrentHashMap<>();

    private ReentrantLock getLock(Key key) {
        return lockMap.computeIfAbsent(key, (k) -> new ReentrantLock());
    }

    public void lock(Channel channel, Interval interval) {
        for(Key k : getKeys(channel, interval)) {
            getLock(k).lock();
        }
    }

    public boolean tryLock(Channel channel, Interval interval) {
        SortedSet<Key> keys = getKeys(channel, interval);
        for(Key key : keys) {
            if(!getLock(key).tryLock()) {
                for(Key k : keys) {
                    if(k.equals(key)) {
                        break;
                    }
                    getLock(k).unlock();
                }
                return false;
            }
        }
        return true;
    }

    public void unlock(Channel channel, Interval interval) {
        for(Key k : getKeys(channel, interval)) {
            getLock(k).unlock();
        }
    }

    //All days in the interval
    private SortedSet<Key> getKeys(Channel channel, Interval interval) {
        String channelId = channel.getId().toString();
        LocalDate date = interval.getStart().toLocalDate();
        Set<Key> keys = new HashSet<>();
        while(date.compareTo(interval.getEnd().toLocalDate()) <= 0) {
            keys.add(new Key(channelId, date));
            date = date.plusDays(1);
        }
        return ImmutableSortedSet.copyOf(keys);
    }


    private class Key implements Comparable<Key> {
        private final String channelId;
        private final LocalDate date;

        Key(String channelId, LocalDate date) {
            this.channelId = channelId;
            this.date = date;

        }

        @Override
        public boolean equals(Object that) {
            if(this == that) {
                return true;
            }
            if(that != null && getClass().equals(that.getClass())) {
                Key key = (Key) that;
                if(this.channelId.equals(key.channelId) &&
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
            return this.date.compareTo(that.date);
        }
    }

}
