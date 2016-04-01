package org.atlasapi.content.v2.serialization;

import java.util.Optional;

import org.atlasapi.content.v2.model.udt.LocationSummary;

import org.joda.time.DateTime;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class LocationSummarySerialization {

    public LocationSummary serialize(org.atlasapi.content.LocationSummary locationSummary) {
        if (locationSummary == null) {
            return null;
        }

        LocationSummary internal =
                new LocationSummary();

        internal.setAvailable(locationSummary.getAvailable());
        internal.setUri(locationSummary.getUri());
        Optional<DateTime> availabilityStart = locationSummary.getAvailabilityStart();
        if (availabilityStart.isPresent()) {
            internal.setStart(availabilityStart.get().toInstant());
        }
        Optional<DateTime> availabilityEnd = locationSummary.getAvailabilityEnd();
        if (availabilityEnd.isPresent()) {
            internal.setEnd(availabilityEnd.get().toInstant());
        }

        return internal;
    }

    public org.atlasapi.content.LocationSummary deserialize(LocationSummary internal) {
        return new org.atlasapi.content.LocationSummary(
                internal.getAvailable(),
                internal.getUri(),
                toDateTime(internal.getStart()),
                toDateTime(internal.getEnd()));

    }
}