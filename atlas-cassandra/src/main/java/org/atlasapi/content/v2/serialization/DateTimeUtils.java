package org.atlasapi.content.v2.serialization;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

public class DateTimeUtils {

    public static Instant toInstant(DateTime joda) {
        if (joda != null) {
            return joda.toInstant();
        } else {
            return null;
        }
    }

    public static DateTime toDateTime(Instant dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.toDateTime(DateTimeZone.UTC);
    }
}
