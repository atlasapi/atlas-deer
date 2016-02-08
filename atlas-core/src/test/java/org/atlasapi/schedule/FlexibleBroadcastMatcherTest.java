package org.atlasapi.schedule;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FlexibleBroadcastMatcherTest {

    FlexibleBroadcastMatcher matcher = new FlexibleBroadcastMatcher(
            Duration.millis(5),
            Optional.of(Duration.millis(5)
            )
    );

    @Test
    public void testMatchesExactBroadcast() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(0L), dateTime(10L)),
                new Broadcast(channel(1L), dateTime(0L), dateTime(10L))
        );
    }

    @Test
    public void testMatchesBroadcastWithinBeforeFlexibilityAtStart() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(3L), dateTime(15L))
        );
    }

    @Test
    public void testMatchesBroadcastOnBeforeFlexibilityAtStart() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(0L), dateTime(15L))
        );
    }

    @Test
    public void testMatchesBroadcastOnAfterFlexibilityAtStart() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(10L), dateTime(15L))
        );
    }

    @Test
    public void testMatchesBroadcastWithinAfterFlexibilityAtStart() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(8L), dateTime(15L))
        );
    }

    @Test
    public void testMatchesBroadcastWithinBeforeFlexibilityAtEnd() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(12L))
        );
    }

    @Test
    public void testMatchesBroadcastOnBeforeFlexibilityAtEnd() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(10L))
        );
    }

    @Test
    public void testMatchesBroadcastOnAfterFlexibilityAtEnd() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(20L))
        );
    }

    @Test
    public void testMatchesBroadcastWithinAfterFlexibilityAtEnd() {
        matchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(17L))
        );
    }

    @Test
    public void testDoesnMatchBroadcastOutsideAfterFlexibilityAtEnd() {
        mismatchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(22L))
        );
    }

    @Test
    public void testDoesnMatchBroadcastOutsideBeforeFlexibilityAtEnd() {
        mismatchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(8L))
        );
    }

    @Test
    public void testDoesnMatchBroadcastOutsideAfterFlexibilityAtStart() {
        mismatchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(11L), dateTime(15L))
        );
    }

    @Test
    public void testDoesnMatchBroadcastOutsideBeforeFlexibilityAtStart() {
        mismatchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(-1L), dateTime(15L))
        );
    }

    @Test
    public void testDoesnMatchBroadcastsOnDifferentChannel() {
        mismatchesSymmetrically(
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(2L), dateTime(5L), dateTime(15L))
        );
    }

    @Test
    public void testMatchesBroadcastIfEndFlexibilityIsAbsentAndStartsMatch() {
        matchesSymmetrically(
                new FlexibleBroadcastMatcher(Duration.millis(5)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(200L))
        );
    }

    @Test
    public void testDoesntMatchesBroadcastIfEndFlexibilityIsAbsentAndStartsDontMatch() {
        mismatchesSymmetrically(
                new FlexibleBroadcastMatcher(Duration.millis(5)),
                new Broadcast(channel(1L), dateTime(11L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(5L), dateTime(200L))
        );
    }

    @Test
    public void testExactMatchersOnlyMatchExactly() {
        matchesSymmetrically(
                FlexibleBroadcastMatcher.exactStart(),
                new Broadcast(channel(1L), dateTime(10L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(10L), dateTime(200L))
        );
        mismatchesSymmetrically(
                FlexibleBroadcastMatcher.exactStart(),
                new Broadcast(channel(1L), dateTime(10L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(11L), dateTime(200L))
        );
        matchesSymmetrically(
                FlexibleBroadcastMatcher.exactStartEnd(),
                new Broadcast(channel(1L), dateTime(10L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(10L), dateTime(15L))
        );
        mismatchesSymmetrically(
                FlexibleBroadcastMatcher.exactStartEnd(),
                new Broadcast(channel(1L), dateTime(10L), dateTime(15L)),
                new Broadcast(channel(1L), dateTime(10L), dateTime(16L))
        );
    }

    @Test
    public void testFindsExactBroadcastInList() {

        Broadcast subject = new Broadcast(channel(1L), dateTime(0L), dateTime(10L));
        Broadcast object1 = new Broadcast(channel(1L), dateTime(1L), dateTime(10L));
        Broadcast object2 = new Broadcast(channel(1L), dateTime(2L), dateTime(10L));
        Broadcast object3 = new Broadcast(channel(2L), dateTime(0L), dateTime(10L));
        Broadcast target = new Broadcast(channel(1L), dateTime(0L), dateTime(10L));
        Broadcast object4 = new Broadcast(channel(1L), dateTime(3L), dateTime(10L));

        Optional<Broadcast> result = FlexibleBroadcastMatcher.exactStartEnd().findMatchingBroadcast(
                subject,
                ImmutableList.of(object1, object2, object3, target, object4)
        );

        assertThat(result.get(), is(target));

    }

    private void matchesSymmetrically(FlexibleBroadcastMatcher matcher,
            Broadcast s, Broadcast o) {
        assertTrue(matcher.matches(s, o));
        assertTrue(matcher.matches(o, s));
    }

    private void matchesSymmetrically(Broadcast s, Broadcast o) {
        matchesSymmetrically(matcher, s, o);
    }

    private void mismatchesSymmetrically(Broadcast s, Broadcast o) {
        mismatchesSymmetrically(matcher, s, o);
    }

    private void mismatchesSymmetrically(FlexibleBroadcastMatcher matcher, Broadcast s,
            Broadcast o) {
        assertFalse(matcher.matches(s, o));
        assertFalse(matcher.matches(o, s));
    }

    private DateTime dateTime(long millis) {
        return new DateTime(millis, DateTimeZones.UTC);
    }

    private Channel channel(long cid) {
        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(cid);
        return channel;
    }
}
