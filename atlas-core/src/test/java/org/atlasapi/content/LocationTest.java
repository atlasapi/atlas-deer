package org.atlasapi.content;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class LocationTest {

    @Test
    public void testNotAvailableWhenAvaialableSetToFalse() {
        Location location = new Location();
        location.setAvailable(false);

        assertThat(Location.AVAILABLE.apply(location), is(false));
    }

    @Test
    public void testAvailableWhenNoPolicy() {
        Location location = new Location();
        location.setAvailable(true);
        location.setPolicy(null);

        assertThat(Location.AVAILABLE.apply(location), is(true));
    }

    @Test
    public void testAvailableWhenStartOnPolicyIsNullAandNowIsBeforeAvailabilityEnd() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Location location = new Location();
        location.setAvailable(true);
        Policy policy = new Policy();
        policy.setAvailabilityEnd(now.plusHours(1));
        location.setPolicy(policy);

        assertThat(Location.AVAILABLE.apply(location), is(true));

    }

    @Test
    public void testAvailableWhenStartOnPolicyIsNullAandNowIsAfterAvailabilityEnd() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Location location = new Location();
        location.setAvailable(true);
        Policy policy = new Policy();
        policy.setAvailabilityEnd(now.minusHours(1));
        location.setPolicy(policy);

        assertThat(Location.AVAILABLE.apply(location), is(false));
    }

    @Test
    public void testAvailableWhenEndOnPolicyIsNullAandNowIsAfterAvailabilityStart() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Location location = new Location();
        location.setAvailable(true);
        Policy policy = new Policy();
        policy.setAvailabilityStart(now.minusHours(1));
        location.setPolicy(policy);

        assertThat(Location.AVAILABLE.apply(location), is(true));
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
    }

    @Test
    public void testAvailableWhenNowBetweenStartAndEndOnPolicy() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Location location = new Location();
        location.setAvailable(true);
        Policy policy = new Policy();
        policy.setAvailabilityStart(now.minusHours(1));
        policy.setAvailabilityEnd(now.plusHours(1));
        location.setPolicy(policy);

        assertThat(Location.AVAILABLE.apply(location), is(true));
    }

    @Test
    public void testAvailableWhenNowBeforeStartOnPolicy() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Location location = new Location();
        location.setAvailable(true);
        Policy policy = new Policy();
        policy.setAvailabilityStart(now.plusHours(1));
        policy.setAvailabilityEnd(now.plusHours(2));
        location.setPolicy(policy);

        assertThat(Location.AVAILABLE.apply(location), is(false));
    }

    @Test
    public void testAvailableWhenNowAfterEndOnPolicy() {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Location location = new Location();
        location.setAvailable(true);
        Policy policy = new Policy();
        policy.setAvailabilityStart(now.minusHours(2));
        policy.setAvailabilityEnd(now.minusHours(1));
        location.setPolicy(policy);

        assertThat(Location.AVAILABLE.apply(location), is(false));
    }

}