package org.atlasapi.content;

import java.util.Optional;
import java.util.function.Predicate;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class LocationSummary {

    private final Boolean available;

    private final String uri;

    private final Optional<DateTime> availabilityStart;

    private final Optional<DateTime> availabilityEnd;

    public LocationSummary(
            Boolean available,
            String uri,
            DateTime availabilityStart,
            DateTime availabilityEnd
    ) {
        this.available = available;
        this.uri = uri;
        this.availabilityStart = Optional.ofNullable(availabilityStart);
        this.availabilityEnd = Optional.ofNullable(availabilityEnd);
    }

    public Boolean getAvailable() {
        return available;
    }

    public String getUri() {
        return uri;
    }

    public Optional<DateTime> getAvailabilityStart() {
        return availabilityStart;
    }

    public Optional<DateTime> getAvailabilityEnd() {
        return availabilityEnd;
    }

    public Boolean isAvailable() {
        return AVAILABLE.test(this);
    }

    public static final Predicate<LocationSummary> AVAILABLE = l -> {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        return l.getAvailable()
                && (
                now.isAfter(l.getAvailabilityStart().orElse(now.minus(1)))
                        && now.isBefore(l.getAvailabilityEnd().orElse(now.plus(1)))
        );
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocationSummary that = (LocationSummary) o;

        if (available != null ? !available.equals(that.available) : that.available != null) {
            return false;
        }
        if (uri != null ? !uri.equals(that.uri) : that.uri != null) {
            return false;
        }
        if (!availabilityStart.equals(that.availabilityStart)) {
            return false;
        }
        return availabilityEnd.equals(that.availabilityEnd);

    }

    @Override
    public int hashCode() {
        int result = available != null ? available.hashCode() : 0;
        result = 31 * result + (uri != null ? uri.hashCode() : 0);
        result = 31 * result + availabilityStart.hashCode();
        result = 31 * result + availabilityEnd.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LocationSummary{" +
                "available=" + available +
                ", uri='" + uri + '\'' +
                ", availabilityStart=" + availabilityStart.orElse(null) +
                ", availabilityEnd=" + availabilityEnd.orElse(null) +
                '}';
    }
}
