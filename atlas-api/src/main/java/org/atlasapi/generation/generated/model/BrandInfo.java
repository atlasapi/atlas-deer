package org.atlasapi.generation.generated.model;

import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;


public class BrandInfo implements ModelClassInfo {

    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()
            .add(
                FieldInfo.builder()
                    .withName("series_refs")
                    .withDescription("")
                    .withType("SeriesRef")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
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
                    .withName("last_fetched")
                    .withDescription("")
                    .withType("DateTime")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("first_seen")
                    .withDescription("")
                    .withType("DateTime")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("genres")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("synopses")
                    .withDescription("")
                    .withType("Synopses")
                    .withIsMultiple(false)
                    .withIsModelType(true)
                    .withJsonType(JsonType.OBJECT)
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
                    .withName("short_description")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("medium_description")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("long_description")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("tags")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
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
                    .withName("this_or_child_last_updated")
                    .withDescription("")
                    .withType("DateTime")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("media_type")
                    .withDescription("")
                    .withType("MediaType")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("specialization")
                    .withDescription("")
                    .withType("Specialization")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("schedule_only")
                    .withDescription("")
                    .withType("boolean")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.BOOLEAN)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("presentation_channel")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.STRING)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("images")
                    .withDescription("")
                    .withType("Image")
                    .withIsMultiple(true)
                    .withIsModelType(true)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("primary_image")
                    .withDescription("")
                    .withType("Image")
                    .withIsMultiple(false)
                    .withIsModelType(true)
                    .withJsonType(JsonType.OBJECT)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("related_links")
                    .withDescription("")
                    .withType("RelatedLink")
                    .withIsMultiple(true)
                    .withIsModelType(true)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("equivalent_to")
                    .withDescription("")
                    .withType("EquivalenceRef")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("clips")
                    .withDescription("")
                    .withType("Clip")
                    .withIsMultiple(true)
                    .withIsModelType(true)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("topic_refs")
                    .withDescription("")
                    .withType("TopicRef")
                    .withIsMultiple(true)
                    .withIsModelType(true)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("content_group_refs")
                    .withDescription("")
                    .withType("ContentGroupRef")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("key_phrases")
                    .withDescription("")
                    .withType("KeyPhrase")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("people")
                    .withDescription("")
                    .withType("CrewMember")
                    .withIsMultiple(true)
                    .withIsModelType(true)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("actors")
                    .withDescription("")
                    .withType("Actor")
                    .withIsMultiple(true)
                    .withIsModelType(true)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("languages")
                    .withDescription("")
                    .withType("String")
                    .withIsMultiple(true)
                    .withIsModelType(false)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("certificates")
                    .withDescription("")
                    .withType("Certificate")
                    .withIsMultiple(true)
                    .withIsModelType(true)
                    .withJsonType(JsonType.ARRAY)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("year")
                    .withDescription("")
                    .withType("Integer")
                    .withIsMultiple(false)
                    .withIsModelType(false)
                    .withJsonType(JsonType.NUMBER)
                    .build()
            )
            .add(
                FieldInfo.builder()
                    .withName("series_refs")
                    .withDescription("")
                    .withType("SeriesRef")
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
    public String name() {
        return "brand";
    }

    @Override
    public String description() {
        return "  @author Robert Chatley (robert@metabroadcast.com) @author Chris Jackson";
    }

    @Override
    public Class<?> describedType() {
        return BrandInfo.class;
    }

}
