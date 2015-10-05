package org.atlasapi.entity;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.serialization.protobuf.CommonProtos;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.metabroadcast.common.time.DateTimeZones;

public class DateTimeSerializer {

    public CommonProtos.DateTime serialize(DateTime dateTime) {
        dateTime = checkNotNull(dateTime).toDateTime(DateTimeZones.UTC);
        return CommonProtos.DateTime.newBuilder()
                .setMillis(dateTime.getMillis())
                .build();
    }

    public DateTime deserialize(CommonProtos.DateTime dateTime) {
        return new DateTime(dateTime.getMillis(), DateTimeZone.UTC);
    }
}
