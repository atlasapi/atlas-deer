package org.atlasapi.content;

import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemAndBroadcast implements Comparable<ItemAndBroadcast> {

    private static final Function<ItemAndBroadcast, Broadcast> TO_BROADCAST =
            new Function<ItemAndBroadcast, Broadcast>() {

                @Override
                public Broadcast apply(ItemAndBroadcast input) {
                    return input.getBroadcast();
                }

            };

    public static final Function<ItemAndBroadcast, Broadcast> toBroadcast() {
        return TO_BROADCAST;
    }

    private static final Function<ItemAndBroadcast, Item> TO_ITEM =
            new Function<ItemAndBroadcast, Item>() {

                @Override
                public Item apply(ItemAndBroadcast input) {
                    return input.getItem();
                }
            };

    public static final Function<ItemAndBroadcast, Item> toItem() {
        return TO_ITEM;
    }

    private final Item item;
    private final Broadcast broadcast;

    public ItemAndBroadcast(Item item, Broadcast broadcast) {
        this.item = checkNotNull(item);
        this.broadcast = checkNotNull(broadcast);
    }

    @FieldName("broadcast")
    public Broadcast getBroadcast() {
        return broadcast;
    }

    @FieldName("item")
    public Item getItem() {
        return item;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(item, broadcast);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ItemAndBroadcast) {
            ItemAndBroadcast other = (ItemAndBroadcast) obj;
            return item.equals(other.item) && broadcast.equals(other.broadcast);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("item", item)
                .add("broadcast", broadcast)
                .toString();
    }

    @Override
    public int compareTo(ItemAndBroadcast o) {
        return ComparisonChain.start()
                .compare(broadcast, o.broadcast, Broadcast.startTimeOrdering())
                .compare(item.getId(), o.item.getId())
                .result();
    }

}