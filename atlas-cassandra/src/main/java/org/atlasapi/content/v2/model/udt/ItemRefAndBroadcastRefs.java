package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.util.NullOrEmptyEquality;

import java.util.List;
import java.util.Objects;

@UDT(name = "itemrefandbroadcastrefs")
public class ItemRefAndBroadcastRefs {

    @Frozen
    @Field(name = "item")
    private PartialItemRef itemRef;

    @Field(name = "broadcasts")
    private List<BroadcastRef> broadcastRefs;

    public ItemRefAndBroadcastRefs() {}

    public ItemRefAndBroadcastRefs(PartialItemRef itemRef, List<BroadcastRef> broadcastRefs) {
        this.itemRef = itemRef;
        this.broadcastRefs = broadcastRefs;
    }

    public PartialItemRef getItemRef() {
        return itemRef;
    }

    public void setItemRef(PartialItemRef itemRef) {
        this.itemRef = itemRef;
    }

    public List<BroadcastRef> getBroadcastRefs() {
        return broadcastRefs;
    }

    public void setBroadcastRefs(
            List<BroadcastRef> broadcastRefs) {
        this.broadcastRefs = broadcastRefs;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ItemRefAndBroadcastRefs that = (ItemRefAndBroadcastRefs) object;
        return Objects.equals(itemRef, that.itemRef) &&
                NullOrEmptyEquality.equals(broadcastRefs, that.broadcastRefs);
    }
}
