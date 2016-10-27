package org.atlasapi.content.v2.serialization;

import javax.annotation.Nullable;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

public class DateTimeUtils {

    @Nullable
    public static Instant toInstant(@Nullable DateTime joda) {
        if (joda != null) {
            return joda.toInstant();
        } else {
            return null;
        }
    }

    @Nullable
    public static DateTime toDateTime(@Nullable Instant dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.toDateTime(DateTimeZone.UTC);
    }
}
