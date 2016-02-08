package org.atlasapi.content;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class LocationSummaryTest {

    @Test
    public void testNotAvailableWhenAvaialableSetToFalse() {
        LocationSummary locationSummary = new LocationSummary(false, null, null, null);

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(false));
    }

    @Test
    public void testAvailableWhenStartOnPolicyIsNullAandNowIsBeforeAvailabilityEnd() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        LocationSummary locationSummary = new LocationSummary(true, null, null, now.plusHours(1));

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(true));

    }

    @Test
    public void testAvailableWhenStartOnPolicyIsNullAandNowIsAfterAvailabilityEnd() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        LocationSummary locationSummary = new LocationSummary(true, null, null, now.minusHours(1));

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(false));
    }

    @Test
    public void testAvailableWhenEndOnPolicyIsNullAandNowIsAfterAvailabilityStart() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        LocationSummary locationSummary = new LocationSummary(true, null, now.minusHours(1), null);

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(true));
    }

    @Test
    public void testAvailableWhenEndOnPolicyIsNullAandNowIsBeforeAvailabilityStart() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Location location = new Location();
        location.setAvailable(true);
        Policy policy = new Policy();
        policy.setAvailabilityStart(now.plusHours(1));
        location.setPolicy(policy);

        assertThat(Location.AVAILABLE.apply(location), is(false));

        LocationSummary locationSummary = new LocationSummary(true, null, now.plusHours(1), null);

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(false));
    }

    @Test
    public void testAvailableWhenNowBetweenStartAndEndOnPolicy() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        LocationSummary locationSummary = new LocationSummary(
                true,
                null,
                now.minusHours(1),
                now.plusHours(1)
        );

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(true));
    }

    @Test
    public void testAvailableWhenNowBeforeStartOnPolicy() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        LocationSummary locationSummary = new LocationSummary(
                true,
                null,
                now.plusHours(1),
                now.plusHours(2)
        );

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(false));
    }

    @Test
    public void testAvailableWhenNowAfterEndOnPolicy() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        LocationSummary locationSummary = new LocationSummary(
                true,
                null,
                now.minusHours(2),
                now.minusHours(1)
        );

        assertThat(LocationSummary.AVAILABLE.test(locationSummary), is(false));
    }

}