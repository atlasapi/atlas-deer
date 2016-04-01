package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.BroadcastRef;
import org.atlasapi.entity.Id;

import org.joda.time.Interval;

public class BroadcastRefSerialization {

    public BroadcastRef serialize(
            org.atlasapi.content.BroadcastRef broadcastRef) {
        if (broadcastRef == null) {
            return null;
        }

        BroadcastRef internal = new BroadcastRef();

        internal.setSourceId(broadcastRef.getSourceId());
        Id channelId = broadcastRef.getChannelId();
        if (channelId != null) {
            internal.setChannelId(channelId.longValue());
        }

        Interval transmissionInterval = broadcastRef.getTransmissionInterval();
        if (transmissionInterval != null) {
            internal.setStart(transmissionInterval.getStart().toInstant());
            internal.setEnd(transmissionInterval.getEnd().toInstant());
        }

        return internal;
    }

    public org.atlasapi.content.BroadcastRef deserialize(BroadcastRef br) {
        if (br == null) {
            return null;
        }

        return new org.atlasapi.content.BroadcastRef(
                br.getSourceId(),
                Id.valueOf(br.getChannelId()),
                new Interval(br.getStart(), br.getEnd())
        );
    }

}