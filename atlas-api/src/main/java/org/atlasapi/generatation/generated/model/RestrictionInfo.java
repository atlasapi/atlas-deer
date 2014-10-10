package org.atlasapi.generatation.generated.model;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;


public class RestrictionInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                FieldInfo.builder()
                    .withName("is_restricted")
                    .withDescription("")
                    .withType("Boolean")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.BOOLEAN)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("minimum_age")
                    .withDescription("")
                    .withType("Integer")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.NUMBER)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("message")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("has_restriction_information")
                    .withDescription("")
                    .withType("boolean")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.BOOLEAN)
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
            .add(
                FieldInfo.builder()
                    .withName("is_restricted")
                    .withDescription("")
                    .withType("Boolean")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.BOOLEAN)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("minimum_age")
                    .withDescription("")
                    .withType("Integer")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.NUMBER)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("message")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("has_restriction_information")
                    .withDescription("")
                    .withType("boolean")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.BOOLEAN)
                    .build()
            )
            .build();

    @Override
    public Set<FieldInfo> fields() {
        return fields;
    }

    @Override
    public String name() {
        return "restriction";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return RestrictionInfo.class;
    }

}
