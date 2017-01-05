package org.atlasapi.schedule;

import java.util.Optional;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.Equivalent;

import com.google.common.base.MoreObjects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Value type of a {@link Broadcast} and the (possibly filtered) {@link Equivalent} {@link Item}s
 * related to it.
 */
public final class EquivalentScheduleEntry implements Comparable<EquivalentScheduleEntry> {

    private final Broadcast broadcast;
    private final Optional<Id> broadcastItemId;
    private final Equivalent<Item> items;

    private EquivalentScheduleEntry(
            Broadcast broadcast,
            Optional<Id> broadcastItemId,
            Equivalent<Item> items
    ) {
        this.broadcast = checkNotNull(broadcast);
        this.broadcastItemId = checkNotNull(broadcastItemId);
        this.items = checkNotNull(items);
    }

    public static EquivalentScheduleEntry create(
            Broadcast broadcast,
            Id broadcastItemId,
            Equivalent<Item> items
    ) {
        return new EquivalentScheduleEntry(
                broadcast,
                Optional.of(broadcastItemId),
                items
        );
    }

    public static EquivalentScheduleEntry createFromDb(
            Broadcast broadcast,
            Optional<Id> broadcastItemId,
            Equivalent<Item> items
    ) {
        return new EquivalentScheduleEntry(
                broadcast,
                broadcastItemId,
                items
        );
    }

    public Broadcast getBroadcast() {
        return broadcast;
    }

    /**
     * Old schedules in the C* equiv schedule store may not have this set, but new data should
     * always have it
     */
    public Optional<Id> getBroadcastItemId() {
        return broadcastItemId;
    }

    public Equivalent<Item> getItems() {
        return items;
    }

    @Override
    public int compareTo(EquivalentScheduleEntry other) {
        return Broadcast.startTimeOrdering().compare(broadcast, other.broadcast);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalentScheduleEntry) {
            EquivalentScheduleEntry other = (EquivalentScheduleEntry) that;
            return broadcast.equals(other.broadcast);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return broadcast.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("broadcast", broadcast)
                .add("broadcastItemId", broadcastItemId)
                .add("items", items)
                .toString();
    }
}
