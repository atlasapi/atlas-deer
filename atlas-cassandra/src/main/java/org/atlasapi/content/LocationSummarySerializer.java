package org.atlasapi.content;

import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class LocationSummarySerializer {

    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();

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
                    dateTimeSerializer.serialize(location.getAvailabilityStart().get())
            );
        }

        if (location.getAvailabilityEnd().isPresent()) {
            builder.setAvailabilityEnd(
                    dateTimeSerializer.serialize(location.getAvailabilityEnd().get())
            );
        }
        return builder;
    }

    public LocationSummary deserialize(CommonProtos.LocationSummary msg) {
        return new LocationSummary(
                msg.hasAvailable() ? msg.getAvailable() : null,
                msg.hasUri() ? msg.getUri() : null,
                msg.hasAvailabilityStart() ?
                dateTimeSerializer.deserialize(msg.getAvailabilityStart()) : null,
                msg.hasAvailabilityEnd() ?
                dateTimeSerializer.deserialize(msg.getAvailabilityEnd()) : null
        );
    }

}
