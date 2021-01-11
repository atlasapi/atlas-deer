package org.atlasapi.content;

import com.google.common.base.MoreObjects;
import org.atlasapi.entity.Id;
import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

public final class BroadcastRef implements Hashable {

    public static final java.util.function.Predicate<BroadcastRef> IS_UPCOMING = b -> b.getTransmissionInterval()
            .getEnd()
            .isAfter(DateTime.now(DateTimeZone.UTC));

    private final String sourceId;
    private final Id channelId;
    private final Interval transmissionInterval;

    public BroadcastRef(String sourceId, Id channelId, Interval transmissionInterval) {
        this.sourceId = checkNotNull(sourceId);
        this.channelId = checkNotNull(channelId);
        this.transmissionInterval = checkNotNull(transmissionInterval);
    }

    @FieldName("source_id")
    public String getSourceId() {
        return sourceId;
    }

    @FieldName("transmission_interval")
    public Interval getTransmissionInterval() {
        return transmissionInterval;
    }

    @FieldName("channel_id")
    public Id getChannelId() {
        return channelId;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof BroadcastRef) {
            BroadcastRef other = (BroadcastRef) that;
            return sourceId.equals(other.sourceId)
                    && channelId.equals(other.channelId)
                    && transmissionInterval.equals(other.transmissionInterval);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return sourceId.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("id", sourceId)
                .add("channel", channelId)
                .add("interval", transmissionInterval)
                .toString();
    }

}
