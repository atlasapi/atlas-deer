package org.atlasapi.generation.generated.model;

import java.util.Set;

import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelClassInfo;

import com.google.common.collect.ImmutableSet;

public class ContainerSummaryInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                    FieldInfo.builder()
                            .withName("type")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("title")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("description")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("series_number")
                            .withDescription("")
                            .withType("Integer")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.NUMBER)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("type")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("title")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("description")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("series_number")
                            .withDescription("")
                            .withType("Integer")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.NUMBER)
                            .build()
            )
            .build();

    @Override
    public Set<FieldInfo> fields() {
        return fields;
    }

    @Override
    public String key() {
        return "containersummary";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return org.atlasapi.content.ContainerSummary.class;
    }

}
