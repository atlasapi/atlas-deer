package org.atlasapi.generatation.generated.model;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;


public class IdentifiedInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                FieldInfo.builder()
                    .withName("alias_urls")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("aliases")
                    .withDescription("")
                    .withType("Alias")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
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
                    .withName("curie")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("uris")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("last_updated")
                    .withDescription("")
                    .withType("DateTime")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("id")
                    .withDescription("")
                    .withType("Id")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("equivalence_update")
                    .withDescription("")
                    .withType("DateTime")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("alias_urls")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("aliases")
                    .withDescription("")
                    .withType("Alias")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
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
                    .withName("curie")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("uris")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("last_updated")
                    .withDescription("")
                    .withType("DateTime")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("id")
                    .withDescription("")
                    .withType("Id")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("equivalence_update")
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
    public String name() {
        return "identified";
    }

    @Override
    public String description() {
        return " Base type for descriptions of resources. @author Robert Chatley @author Lee Denison";
    }

    @Override
    public Class<?> describedType() {
        return IdentifiedInfo.class;
    }

}
