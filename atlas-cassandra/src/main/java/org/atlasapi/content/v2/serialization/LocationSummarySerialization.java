package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Interval;
import org.atlasapi.content.v2.model.udt.LocationSummary;

import org.joda.time.DateTime;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class LocationSummarySerialization {

    public LocationSummary serialize(org.atlasapi.content.LocationSummary locationSummary) {
        if (locationSummary == null) {
            return null;
        }

        LocationSummary internal = new LocationSummary();

        internal.setAvailable(locationSummary.getAvailable());
        internal.setUri(locationSummary.getUri());

        Interval interval = new Interval();

        locationSummary.getAvailabilityStart()
                .map(DateTime::toInstant)
                .ifPresent(interval::setStart);

        locationSummary.getAvailabilityEnd()
                .map(DateTime::toInstant)
                .ifPresent(interval::setEnd);

        if (interval.getStart() != null || interval.getEnd() != null) {
            internal.setInterval(interval);
        }

        return internal;
    }

    public org.atlasapi.content.LocationSummary deserialize(LocationSummary internal) {
        Interval interval = internal.getInterval();
        return new org.atlasapi.content.LocationSummary(
                internal.getAvailable(),
                internal.getUri(),
                toDateTime(interval != null ? interval.getStart() : null),
                toDateTime(interval != null ? interval.getEnd() : null)
        );

    }
}