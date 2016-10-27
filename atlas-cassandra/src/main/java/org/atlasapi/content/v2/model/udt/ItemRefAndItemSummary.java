package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "itemrefanditemsummary")
public class ItemRefAndItemSummary {

    @Frozen
    @Field(name = "item")
    private ItemRef itemRef;

    @Frozen
    @Field(name = "summary")
    private ItemSummary summary;

    ItemRefAndItemSummary() {}

    public ItemRefAndItemSummary(
            ItemRef itemRef,
            ItemSummary summary
    ) {
        this.itemRef = itemRef;
        this.summary = summary;
    }

    public ItemRef getItemRef() {
        return itemRef;
    }

    public void setItemRef(ItemRef itemRef) {
        this.itemRef = itemRef;
    }

    public ItemSummary getSummary() {
        return summary;
    }

    public void setSummary(ItemSummary summary) {
        this.summary = summary;
    }
}
