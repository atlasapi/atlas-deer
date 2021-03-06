package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "itemrefanditemsummary")
public class ItemRefAndItemSummary {

    @Frozen
    @Field(name = "item")
    private PartialItemRef itemRef;

    @Frozen
    @Field(name = "summary")
    private ItemSummary summary;

    public ItemRefAndItemSummary() {}

    public ItemRefAndItemSummary(
            PartialItemRef itemRef,
            ItemSummary summary
    ) {
        this.itemRef = itemRef;
        this.summary = summary;
    }

    public PartialItemRef getItemRef() {
        return itemRef;
    }

    public void setItemRef(PartialItemRef itemRef) {
        this.itemRef = itemRef;
    }

    public ItemSummary getSummary() {
        return summary;
    }

    public void setSummary(ItemSummary summary) {
        this.summary = summary;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ItemRefAndItemSummary that = (ItemRefAndItemSummary) object;
        return Objects.equals(itemRef, that.itemRef) &&
                Objects.equals(summary, that.summary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemRef, summary);
    }
}
