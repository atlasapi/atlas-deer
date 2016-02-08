package org.atlasapi.generation.generated.model;

import java.util.Set;

import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelClassInfo;

import com.google.common.collect.ImmutableSet;

public class ChannelScheduleInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                    FieldInfo.builder()
                            .withName("channel")
                            .withDescription("")
                            .withType("Channel")
                            .withIsMultiple(false)
                            .withIsModelType(true)
                            .withJsonType(JsonType.OBJECT)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("interval")
                            .withDescription("")
                            .withType("Interval")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("entries")
                            .withDescription("")
                            .withType("ItemAndBroadcast")
                            .withIsMultiple(true)
                            .withIsModelType(false)
                            .withJsonType(JsonType.ARRAY)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("channel")
                            .withDescription("")
                            .withType("Channel")
                            .withIsMultiple(false)
                            .withIsModelType(true)
                            .withJsonType(JsonType.OBJECT)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("interval")
                            .withDescription("")
                            .withType("Interval")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("entries")
                            .withDescription("")
                            .withType("ItemAndBroadcast")
                            .withIsMultiple(true)
                            .withIsModelType(false)
                            .withJsonType(JsonType.ARRAY)
                            .build()
            )
            .build();

    @Override
    public Set<FieldInfo> fields() {
        return fields;
    }

    @Override
    public String key() {
        return "channelschedule";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return org.atlasapi.schedule.ChannelSchedule.class;
    }

}
