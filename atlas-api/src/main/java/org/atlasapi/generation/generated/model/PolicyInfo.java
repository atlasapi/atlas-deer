package org.atlasapi.generation.generated.model;

import java.util.Set;

import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelClassInfo;

import com.google.common.collect.ImmutableSet;

public class PolicyInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                    FieldInfo.builder()
                            .withName("available_countries")
                            .withDescription("")
                            .withType("Country")
                            .withIsMultiple(true)
                            .withIsModelType(false)
                            .withJsonType(JsonType.ARRAY)
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
                            .withName("drm_playable_from")
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
                            .withName("platform")
                            .withDescription("")
                            .withType("Platform")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("network")
                            .withDescription("")
                            .withType("Network")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("actual_availability_start")
                            .withDescription("")
                            .withType("DateTime")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("revenue_contract")
                            .withDescription("")
                            .withType("RevenueContract")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("price")
                            .withDescription("")
                            .withType("Price")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("availability_length")
                            .withDescription("")
                            .withType("Integer")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.NUMBER)
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
                            .withIsModelType(true)
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
                            .withName("available_countries")
                            .withDescription("")
                            .withType("Country")
                            .withIsMultiple(true)
                            .withIsModelType(false)
                            .withJsonType(JsonType.ARRAY)
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
                            .withName("drm_playable_from")
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
                            .withName("platform")
                            .withDescription("")
                            .withType("Platform")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("network")
                            .withDescription("")
                            .withType("Network")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("actual_availability_start")
                            .withDescription("")
                            .withType("DateTime")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("revenue_contract")
                            .withDescription("")
                            .withType("RevenueContract")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("price")
                            .withDescription("")
                            .withType("Price")
                            .withIsMultiple(false)
                            .withIsModelType(false)
                            .withJsonType(JsonType.STRING)
                            .build()
            )
            .add(
                    FieldInfo.builder()
                            .withName("availability_length")
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
        return "policy";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return org.atlasapi.content.Policy.class;
    }

}
