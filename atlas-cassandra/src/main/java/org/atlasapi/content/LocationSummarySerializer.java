package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.CommonProtos;

import static org.atlasapi.entity.ProtoBufUtils.deserializeDateTime;
import static org.atlasapi.entity.ProtoBufUtils.serializeDateTime;

public class LocationSummarySerializer {


    public CommonProtos.LocationSummary.Builder serialize(LocationSummary location) {
        CommonProtos.LocationSummary.Builder builder = CommonProtos.LocationSummary.newBuilder();

        if (location.getAvailable() != null) {
            builder.setAvailable(location.getAvailable());
        }
        if (location.getUri() != null) {
            builder.setUri(location.getUri());
        }
        if (location.getAvailabilityStart().isPresent()) {
            builder.setAvailabilityStart(
                    serializeDateTime(location.getAvailabilityStart().get())
            );
        }

        if (location.getAvailabilityEnd().isPresent()) {
            builder.setAvailabilityEnd(
                    serializeDateTime(location.getAvailabilityEnd().get())
            );
        }
        return builder;
    }

    public LocationSummary deserialize(CommonProtos.LocationSummary msg) {
        return new LocationSummary(
                msg.hasAvailable() ? msg.getAvailable() : null,
                msg.hasUri() ? msg.getUri(): null,
                msg.hasAvailabilityStart() ? deserializeDateTime(msg.getAvailabilityStart()) : null,
                msg.hasAvailabilityEnd() ? deserializeDateTime(msg.getAvailabilityEnd()) : null
        );
    }

}
