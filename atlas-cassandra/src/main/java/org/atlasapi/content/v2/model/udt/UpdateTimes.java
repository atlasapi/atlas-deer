package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.content.v2.model.WithUpdateTimes;
import org.joda.time.Instant;

import javax.annotation.Nullable;

@UDT(name = "updatetimes")
public class UpdateTimes implements WithUpdateTimes {

    @Field(name = "last_updated") private Instant lastUpdated;
    @Field(name = "equiv_update") private Instant equivalenceUpdate;

    public UpdateTimes() {}

    public UpdateTimes(@Nullable Instant lastUpdated, @Nullable Instant equivalenceUpdate) {
        this.lastUpdated = lastUpdated;
        this.equivalenceUpdate = equivalenceUpdate;
    }

    @Override
    public Instant getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Override
    public Instant getEquivalenceUpdate() {
        return equivalenceUpdate;
    }

    @Override
    public void setEquivalenceUpdate(Instant equivalenceUpdate) {
        this.equivalenceUpdate = equivalenceUpdate;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
