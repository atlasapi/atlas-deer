package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

/** This doesn't hold the actual ID and publisher because those are the strict PK of
 * any resource ref. These objects are usually stored as a CQL {@code map<Ref, PartialItemRef>} and
 * serialised accordingly. If you need to store a full {@code PartialItemRef} as a field, you'll need a
 * to make a different UDT.
 *
 * @see Ref
 * @see SeriesRef
 */
@UDT(name = "itemref")
public class PartialItemRef {

    @Field(name = "sort_key") private String sortKey;
    @Field(name = "updated") private Instant updated;
    @Field(name = "type") private String type;

    public PartialItemRef() {}

    public String getSortKey() {
        return sortKey;
    }

    public void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
