package org.atlasapi.generation.generated.model;

import java.util.Set;

import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelClassInfo;

import com.google.common.collect.ImmutableSet;

public class ChannelGroupInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                    FieldInfo.builder()
                            .withName("source")
                            .withDescription("")
                            .withType("Publisher")
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
                            .withName("source")
                            .withDescription("")
                            .withType("Publisher")
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
                            .withName("title")
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
    public String key() {
        return "channelgroup";
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public Class<?> describedType() {
        return org.atlasapi.channel.ChannelGroup.class;
    }

}
