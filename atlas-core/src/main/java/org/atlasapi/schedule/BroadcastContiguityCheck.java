package org.atlasapi.schedule;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.content.Broadcast;

import com.google.common.base.Predicate;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class BroadcastContiguityCheck implements Predicate<List<Broadcast>> {

    private Duration maxGap;

    public BroadcastContiguityCheck() {
        this(Duration.ZERO);
    }

    public BroadcastContiguityCheck(Duration maxGap) {
        this.maxGap = maxGap;
    }

    public boolean apply(List<Broadcast> broadcasts) {
        boolean valid = true;
        Iterator<Broadcast> iterator = broadcasts.iterator();
        if (iterator.hasNext()) {
            DateTime lastEnd = iterator.next().getTransmissionEndTime();
            while (valid && iterator.hasNext()) {
                Broadcast broadcast = iterator.next();
                DateTime start = broadcast.getTransmissionTime();
                valid = valid(start, lastEnd);
                lastEnd = broadcast.getTransmissionEndTime();
            }
        }
        return valid;
    }

    private boolean valid(DateTime start, DateTime lastEnd) {
        boolean overlaps = start.isBefore(lastEnd);
        boolean withinMaxGap = !start.isAfter(lastEnd.plus(maxGap));
        return !overlaps && withinMaxGap;
    }

}
