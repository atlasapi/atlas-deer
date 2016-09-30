package org.atlasapi.content.v2.model.udt;

import java.util.List;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "itemrefandbroadcastrefs")
public class ItemRefAndBroadcastRefs {

    @Field(name = "item") private ItemRef itemRef;
    @Field(name = "broadcasts") private List<BroadcastRef> broadcastRefs;

    public ItemRef getItemRef() {
        return itemRef;
    }

    public void setItemRef(ItemRef itemRef) {
        this.itemRef = itemRef;
    }

    public List<BroadcastRef> getBroadcastRefs() {
        return broadcastRefs;
    }

    public void setBroadcastRefs(
            List<BroadcastRef> broadcastRefs) {
        this.broadcastRefs = broadcastRefs;
    }
}
