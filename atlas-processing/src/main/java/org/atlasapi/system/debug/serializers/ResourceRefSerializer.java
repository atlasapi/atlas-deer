package org.atlasapi.system.debug.serializers;

import java.lang.reflect.Type;
import java.math.BigInteger;

import org.atlasapi.content.ContentRef;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.topic.TopicRef;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class ResourceRefSerializer implements JsonSerializer<ResourceRef> {

    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private ResourceRefSerializer() {
    }

    public static ResourceRefSerializer create() {
        return new ResourceRefSerializer();
    }

    @Override
    public JsonElement serialize(
            ResourceRef src,
            Type typeOfSrc,
            JsonSerializationContext context
    ) {
        JsonObject json = new JsonObject();

        json.add(
                "id",
                context.serialize(codec.encode(BigInteger.valueOf(src.getId().longValue())))
        );
        json.add(
                "source",
                context.serialize(src.getSource().key())
        );

        if (src instanceof TopicRef) {
            json.add(
                    "type",
                    context.serialize(src.getResourceType())
            );
        } else {
            ContentRef contentRef = (ContentRef) src;
            json.add(
                    "type",
                    context.serialize(contentRef.getContentType())
            );
        }

        return json;
    }
}
