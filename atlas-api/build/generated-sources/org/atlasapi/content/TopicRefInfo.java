package org.atlasapi.content;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;


public class TopicRefInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                FieldInfo.builder()
                    .withName("weighting")
                    .withDescription("")
                    .withType("Float")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.NUMBER)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("is_supervised")
                    .withDescription("")
                    .withType("Boolean")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.BOOLEAN)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("topic")
                    .withDescription("")
                    .withType("Id")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("relationship")
                    .withDescription("")
                    .withType("Relationship")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("offset")
                    .withDescription("")
                    .withType("Integer")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.NUMBER)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("publisher")
                    .withDescription("")
                    .withType("Publisher")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("weighting")
                    .withDescription("")
                    .withType("Float")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.NUMBER)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("is_supervised")
                    .withDescription("")
                    .withType("Boolean")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.BOOLEAN)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("topic")
                    .withDescription("")
                    .withType("Id")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("relationship")
                    .withDescription("")
                    .withType("Relationship")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("offset")
                    .withDescription("")
                    .withType("Integer")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.NUMBER)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("publisher")
                    .withDescription("")
                    .withType("Publisher")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .build();

    @Override
    public Set<FieldInfo> fields() {
        return fields;
    }

    @Override
    public String name() {
        return "topicref";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return TopicRefInfo.class;
    }

}
