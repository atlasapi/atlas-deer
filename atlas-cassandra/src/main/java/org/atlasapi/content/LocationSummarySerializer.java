package org.atlasapi.content;

import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

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
                    new DateTimeSerializer().serialize(location.getAvailabilityStart().get())
            );
        }

        if (location.getAvailabilityEnd().isPresent()) {
            builder.setAvailabilityEnd(
                    new DateTimeSerializer().serialize(location.getAvailabilityEnd().get())
            );
        }
        return builder;
    }

    public LocationSummary deserialize(CommonProtos.LocationSummary msg) {
        return new LocationSummary(
                msg.hasAvailable() ? msg.getAvailable() : null,
                msg.hasUri() ? msg.getUri(): null,
                msg.hasAvailabilityStart() ?
                new DateTimeSerializer().deserialize(msg.getAvailabilityStart()) : null,
                msg.hasAvailabilityEnd() ?
                new DateTimeSerializer().deserialize(msg.getAvailabilityEnd()) : null
        );
    }

}
