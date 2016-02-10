package org.atlasapi.schedule;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.equivalence.Equivalent;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Value type of a {@link Broadcast} and the (possibly filtered) {@link Equivalent} {@link Item}s
 * related to it.
 */
public final class EquivalentScheduleEntry implements Comparable<EquivalentScheduleEntry> {

    private final Broadcast broadcast;
    private final Equivalent<Item> items;

    public EquivalentScheduleEntry(Broadcast broadcast, Equivalent<Item> items) {
        this.broadcast = checkNotNull(broadcast);
        this.items = checkNotNull(items);
    }

    public Broadcast getBroadcast() {
        return broadcast;
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
        return Objects.toStringHelper(getClass())
                .add("broadcast", broadcast)
                .add("items", items)
                .toString();
    }

}
