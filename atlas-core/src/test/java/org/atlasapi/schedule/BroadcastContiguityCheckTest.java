package org.atlasapi.schedule;

import java.util.List;

import org.atlasapi.content.Broadcast;
import org.atlasapi.entity.Id;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BroadcastContiguityCheckTest {

    private final Duration maxGap = Duration.standardSeconds(5);
    private final BroadcastContiguityCheck check
            = new BroadcastContiguityCheck(maxGap);

    private final BroadcastContiguityCheck zeroGapCheck
            = new BroadcastContiguityCheck();

    @Test
    public void testContiguousBroadcastsAreValid() {
        assertTrue(check.apply(list(broadcast(0, 5), broadcast(5, 10), broadcast(10, 15))));
    }

    @Test
    public void testNonContiguousBroadcastsWithinMaxGapAreValid() {
        assertTrue(check.apply(list(broadcast(0, 3), broadcast(5, 7), broadcast(10, 15))));
    }

    @Test
    public void testNonContiguousBroadcastsOnMaxGapAreValid() {
        assertTrue(check.apply(list(broadcast(0, 3), broadcast(7, 8), broadcast(13, 15))));
    }

    @Test
    public void testNonContiguousBroadcastsBeyondMaxGapAreInvalid() {
        assertFalse(check.apply(list(broadcast(0, 2), broadcast(8, 9))));
    }

    @Test
    public void testOverlappingBroadcastsBeyondMaxGapAreInvalid() {
        assertFalse(check.apply(list(broadcast(0, 5), broadcast(4, 8))));
    }

    @Test
    public void testSingleBroadcastIsValid() {
        assertTrue(check.apply(list(broadcast(0, 5))));
    }

    @Test
    public void testNoBroadcastsIsValid() {
        assertTrue(check.apply(list()));
    }

    @Test
    public void testZeroGapCheckForContiguousBroadcastsIsValid() {
        assertTrue(zeroGapCheck.apply(list(broadcast(0, 5), broadcast(5, 10), broadcast(10, 15))));
    }

    @Test
    public void testZeroGapCheckNonContiguousBroadcastsIsInvalid() {
        assertFalse(zeroGapCheck.apply(list(broadcast(0, 5), broadcast(6, 10))));
    }

    private List<Broadcast> list(Broadcast... broadcasts) {
        return ImmutableList.copyOf(broadcasts);
    }

    private Broadcast broadcast(int s, int e) {
        return new Broadcast(Id.valueOf(1), new DateTime(s * 1000, DateTimeZones.UTC),
                new DateTime(e * 1000, DateTimeZones.UTC)
        );
    }

}
