package org.atlasapi.content.v2.model.udt;

import javax.annotation.Nullable;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "updatetimes")
public class UpdateTimes {

    @Field(name = "last_updated") private Instant lastUpdated;
    @Field(name = "equiv_update") private Instant equivUpdate;

    public UpdateTimes(@Nullable Instant lastUpdated, @Nullable Instant equivUpdate) {
        this.lastUpdated = lastUpdated;
        this.equivUpdate = equivUpdate;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Instant getEquivUpdate() {
        return equivUpdate;
    }

    public void setEquivUpdate(Instant equivUpdate) {
        this.equivUpdate = equivUpdate;
    }
}
