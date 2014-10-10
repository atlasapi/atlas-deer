package org.atlasapi.query.v4.meta;

import java.lang.reflect.Type;

import org.atlasapi.generation.model.ModelClassInfo;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;


public class ModelClassInfoSerializer implements JsonSerializer<ModelClassInfo> {

    @Override
    public JsonElement serialize(ModelClassInfo src, Type typeOfSrc,
            JsonSerializationContext context) {
        
        JsonObject json = new JsonObject();
        
        json.add("name", context.serialize(src.name()));
        json.add("description", context.serialize(src.description()));
        json.add("fields", context.serialize(src.fields()));
        json.add("described_type", context.serialize(src.describedType().getCanonicalName()));
        
        return json;
    }

}
