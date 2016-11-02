package org.atlasapi.content.v2.serialization.setters;

import org.atlasapi.content.v2.model.WithUpdateTimes;
import org.atlasapi.entity.Identified;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;
import static org.atlasapi.content.v2.serialization.DateTimeUtils.toInstant;

public class WithUpdateTimesSetter {

    public void serialize(WithUpdateTimes internal, Identified identified) {
        internal.setLastUpdated(toInstant(identified.getLastUpdated()));
        internal.setEquivalenceUpdate(toInstant(identified.getEquivalenceUpdate()));
    }

    public void deserialize(Identified identified, WithUpdateTimes internal) {
        identified.setLastUpdated(toDateTime(internal.getLastUpdated()));
        identified.setEquivalenceUpdate(toDateTime(internal.getEquivalenceUpdate()));
    }
}
