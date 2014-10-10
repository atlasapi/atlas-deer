package org.atlasapi.generatation.generated.model;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;


public class DescriptionInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
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
                    .withName("synopsis")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("image")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("thumbnail")
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
                    .withName("synopsis")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("image")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("thumbnail")
                    .withDescription("")
                    .withType("String")
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
        return "description";
    }

    @Override
    public String description() {
        return " A Description of something  @author Fred van den Driessche (fred@metabroadcast.com)";
    }

    @Override
    public Class<?> describedType() {
        return DescriptionInfo.class;
    }

}
