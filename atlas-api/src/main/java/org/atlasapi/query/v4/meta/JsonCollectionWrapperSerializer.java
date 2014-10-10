package org.atlasapi.query.v4.meta;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;


public class JsonCollectionWrapperSerializer<T> implements JsonSerializer<JsonCollectionWrapper<T>> {

    @Override
    public JsonElement serialize(JsonCollectionWrapper<T> src, Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject json = new JsonObject();
        
        json.add(src.name(), context.serialize(src.collection()));
        
        return json;
    }

}
