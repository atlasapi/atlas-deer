package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.Broadcast;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.base.Optional;

/**
 * <p>Determines whether two {@link Broadcast}s share a schedule slot to within a
 * certain transmission time flexibility.</p>
 * 
 * Broadcasts share a schedule slot if:
 * <ul>
 *      <li>they are on the same channel</li>
 *      <li>start at the same instant ± flexibility</li>
 *      <li>(optionally) end at the same instance ± flexibility</li>
 * <ul>
 * 
 */
public class FlexibleBroadcastMatcher {

    private static final FlexibleBroadcastMatcher EXACT_START
        = new FlexibleBroadcastMatcher(Duration.ZERO);
    private static final FlexibleBroadcastMatcher EXACT_START_END
        = new FlexibleBroadcastMatcher(Duration.ZERO, Optional.of(Duration.ZERO));

    public static final FlexibleBroadcastMatcher exactStart() {
        return EXACT_START;
    }

    public static final FlexibleBroadcastMatcher exactStartEnd() {
        return EXACT_START_END;
    }
    
    private Duration startFlexibility;
    private Optional<Duration> endFlexibility;
    
    public FlexibleBroadcastMatcher(Duration startFlexibility) {
        this(startFlexibility, Optional.<Duration>absent());
    }

    public FlexibleBroadcastMatcher(Duration startFlexibility, Optional<Duration> endFlexibility) {
        this.startFlexibility = checkNotNull(startFlexibility);
        this.endFlexibility = checkNotNull(endFlexibility);
    }
    
    public boolean matches(Broadcast subject, Broadcast object) {
        return channelsMatch(subject, object)
            && startTimeMatch(subject, object)
            && endTimeMatch(subject, object);
    }

    public Optional<Broadcast> findMatchingBroadcast(Broadcast subject, Iterable<Broadcast> objects) {
        for (Broadcast object : objects) {
            if (matches(subject, object)) {
                return Optional.of(object);
            }
        }
        return Optional.absent();
    }

    private boolean channelsMatch(Broadcast subject, Broadcast object) {
        return subject.getChannelId().equals(object.getChannelId());
    }

    private boolean startTimeMatch(Broadcast subject, Broadcast object) {
        return withInFlexibility(subject.getTransmissionTime(), startFlexibility, object.getTransmissionTime());
    }
    
    private boolean endTimeMatch(Broadcast subject, Broadcast object) {
        return !endFlexibility.isPresent() ||
            withInFlexibility(subject.getTransmissionEndTime(), endFlexibility.get(), object.getTransmissionEndTime());
    }
    
    private boolean withInFlexibility(DateTime subj, Duration flex, DateTime obj) {
        return !(subj.minus(flex).isAfter(obj) || subj.plus(flex).isBefore(obj));
    }
    
}
