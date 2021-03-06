package org.atlasapi.generation.generated.model;

import java.util.Set;

import org.atlasapi.content.Tag;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelClassInfo;

import com.google.common.collect.ImmutableSet;

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
    public String key() {
        return "topicref";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return Tag.class;
    }

}
