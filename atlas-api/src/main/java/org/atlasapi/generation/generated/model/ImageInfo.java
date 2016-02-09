package org.atlasapi.generation.generated.model;

import java.util.Set;

import org.atlasapi.content.Image;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelClassInfo;

import com.google.common.collect.ImmutableSet;

public class ImageInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                    FieldInfo.builder()
                            .withName("uri")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("height")
                            .withDescription("")
                            .withType("Integer")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.NUMBER)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("width")
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
                            .withType("Type")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("color")
                            .withDescription("")
                            .withType("Color")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("theme")
                            .withDescription("")
                            .withType("Theme")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("aspect_ratio")
                            .withDescription("")
                            .withType("AspectRatio")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("mime_type")
                            .withDescription("")
                            .withType("MimeType")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("availability_start")
                            .withDescription("")
                            .withType("DateTime")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("availability_end")
                            .withDescription("")
                            .withType("DateTime")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("uri")
                            .withDescription("")
                            .withType("String")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("height")
                            .withDescription("")
                            .withType("Integer")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.NUMBER)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("width")
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
                            .withType("Type")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("color")
                            .withDescription("")
                            .withType("Color")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("theme")
                            .withDescription("")
                            .withType("Theme")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("aspect_ratio")
                            .withDescription("")
                            .withType("AspectRatio")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("mime_type")
                            .withDescription("")
                            .withType("MimeType")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("availability_start")
                            .withDescription("")
                            .withType("DateTime")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("availability_end")
                            .withDescription("")
                            .withType("DateTime")
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
        return "image";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return Image.class;
    }

}
