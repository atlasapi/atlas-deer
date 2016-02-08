package org.atlasapi.output.writers.time;

import java.time.Month;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * This class attempts to fix the Unix Millennium Bug for 32-bit clients by changing any date time
 * that is greater than 2038-01-01T00:00:00.0 to that.
 * <p>
 * This was needed because some Android devices are 32-bit and we ingest from content providers that
 * sometimes give us dates higher than the max that can be represented in signed 32-bit.
 * <p>
 * It is a temporary solution and it is set to be automatically disabled on 2016-03-05T08:00:00.0Z.
 * There is a unit test that will fail on that date time to alert us to remove this.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Year_2038_problem">Year 2038 problem</a>
 */
public class UnixMillenniumBugFixer {

    public static final DateTime FIX_DISABLE_DATE_TIME =
            new DateTime(2016, Month.MARCH.getValue(), 5, 8, 0, 0, 0, DateTimeZone.UTC);

    public static final DateTime MAX_ALLOWED_DATE_TIME =
            new DateTime(2038, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, DateTimeZone.UTC);

    public DateTime clampDateTime(DateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        if (DateTime.now().withZone(DateTimeZone.UTC).isAfter(FIX_DISABLE_DATE_TIME)) {
            return dateTime;
        }

        if (dateTime.isAfter(MAX_ALLOWED_DATE_TIME)) {
            return MAX_ALLOWED_DATE_TIME.withZone(dateTime.getZone());
        }
        return dateTime;
    }
}
