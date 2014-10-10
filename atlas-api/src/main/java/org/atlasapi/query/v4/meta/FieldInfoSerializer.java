package org.atlasapi.query.v4.meta;

import static org.atlasapi.query.v4.meta.EndpointClassInfoSerializer.createModelLinkFrom;

import java.lang.reflect.Type;

import org.atlasapi.generation.model.FieldInfo;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;


public class FieldInfoSerializer implements JsonSerializer<FieldInfo> {

    @Override
    public JsonElement serialize(FieldInfo src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject json = new JsonObject();
        
        json.add("name", context.serialize(src.name()));
        json.add("description", context.serialize(src.description()));
        json.add("type", context.serialize(src.type()));
        json.add("is_multiple", context.serialize(src.isMultiple()));
        if (src.isModelType()) {
            json.add("model_class_link", context.serialize(createModelLinkFrom(src.type().toLowerCase())));
        }
        json.add("jsonType", context.serialize(src.jsonType().value()));
        
        return json;
    }
}
