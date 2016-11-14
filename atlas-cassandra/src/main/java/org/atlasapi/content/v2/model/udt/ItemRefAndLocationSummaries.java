package org.atlasapi.content.v2.model.udt;

import java.util.List;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "itemrefandlocationsummaries")
public class ItemRefAndLocationSummaries {

    @Frozen
    @Field(name = "item")
    private PartialItemRef itemRef;

    @Field(name = "location_summaries")
    private List<LocationSummary> locationSummaries;

    public ItemRefAndLocationSummaries() {}

    public ItemRefAndLocationSummaries(
            PartialItemRef itemRef,
            List<LocationSummary> locationSummaries
    ) {
        this.itemRef = itemRef;
        this.locationSummaries = locationSummaries;
    }

    public PartialItemRef getItemRef() {
        return itemRef;
    }

    public void setItemRef(PartialItemRef itemRef) {
        this.itemRef = itemRef;
    }

    public List<LocationSummary> getLocationSummaries() {
        return locationSummaries;
    }

    public void setLocationSummaries(List<LocationSummary> locationSummaries) {
        this.locationSummaries = locationSummaries;
    }
}
