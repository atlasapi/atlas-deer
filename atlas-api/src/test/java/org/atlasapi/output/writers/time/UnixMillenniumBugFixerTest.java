package org.atlasapi.output.writers.time;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import static org.atlasapi.output.writers.time.UnixMillenniumBugFixer.FIX_DISABLE_DATE_TIME;
import static org.atlasapi.output.writers.time.UnixMillenniumBugFixer.MAX_ALLOWED_DATE_TIME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class UnixMillenniumBugFixerTest {

    private UnixMillenniumBugFixer fixer;
    private DateTimeZone alternativeZone;

    @Before
    public void setUp() throws Exception {
        fixer = new UnixMillenniumBugFixer();
        alternativeZone = DateTimeZone.forOffsetHours(1);
    }

    @Test
    public void testDateTimeGreaterThanMax32BitUnixTimeIsModified() throws Exception {
        DateTime requestedDateTime = new DateTime(2038, 2, 1, 0, 0, 0, DateTimeZone.UTC);
        DateTime dateTime = fixer.clampDateTime(requestedDateTime);

        assertThat(dateTime, is(MAX_ALLOWED_DATE_TIME));
    }

    @Test
    public void testDateTimeGreaterThanMax32BitUnixTimeIsModifiedButPreservesZone()
            throws Exception {
        DateTime requestedDateTime = new DateTime(2038, 2, 1, 0, 0, 0, alternativeZone);
        DateTime dateTime = fixer.clampDateTime(requestedDateTime);

        assertThat(dateTime, is(MAX_ALLOWED_DATE_TIME.withZone(alternativeZone)));
    }

    @Test
    public void testDateTimeLessThatMax32BitUnixTimeIsNotModified() throws Exception {
        DateTime requestedDateTime = new DateTime(2037, 2, 1, 0, 0, 0, alternativeZone);
        DateTime dateTime = fixer.clampDateTime(requestedDateTime);

        assertThat(dateTime, is(requestedDateTime));
    }

    @Test
    public void testFixExpirationDateHasNotPassed() throws Exception {
        assertThat(FIX_DISABLE_DATE_TIME.isAfterNow(), is(true));
    }

    @Test
    public void testReturnNullIfGivenNull() throws Exception {
        DateTime dateTime = fixer.clampDateTime(null);

        assertThat(dateTime, is(nullValue()));
    }
}