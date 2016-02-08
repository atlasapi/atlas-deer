package org.atlasapi.entity;

import org.atlasapi.serialization.protobuf.CommonProtos;

import com.metabroadcast.common.time.DateTimeZones;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.google.common.base.Preconditions.checkNotNull;

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
