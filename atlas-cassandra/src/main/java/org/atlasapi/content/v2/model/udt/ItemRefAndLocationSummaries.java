package org.atlasapi.content.v2.model.udt;

import java.util.List;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "itemrefandlocationsummaries")
public class ItemRefAndLocationSummaries {

    @Field(name = "item") private ItemRef itemRef;
    @Field(name = "location_summaries") private List<LocationSummary> locationSummaries;

    public ItemRef getItemRef() {
        return itemRef;
    }

    public void setItemRef(ItemRef itemRef) {
        this.itemRef = itemRef;
    }

    public List<LocationSummary> getLocationSummaries() {
        return locationSummaries;
    }

    public void setLocationSummaries(List<LocationSummary> locationSummaries) {
        this.locationSummaries = locationSummaries;
    }
}
