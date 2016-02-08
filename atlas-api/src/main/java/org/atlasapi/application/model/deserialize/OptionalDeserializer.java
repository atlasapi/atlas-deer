package org.atlasapi.application.model.deserialize;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.google.common.base.Optional;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OptionalDeserializer implements JsonSerializer<Optional<?>>,
        JsonDeserializer<Optional<?>> {

    @Override
    public JsonElement serialize(Optional<?> optional, Type type,
            JsonSerializationContext context) {
        if (optional.isPresent()) {
            return context.serialize(optional.get());
        }
        return new JsonNull();
    }

    @Override
    public Optional<?> deserialize(JsonElement json, Type type, JsonDeserializationContext context)
            throws JsonParseException {
        Object value = context.deserialize(
                json,
                ((ParameterizedType) type).getActualTypeArguments()[0]
        );
        return Optional.fromNullable(value);
    }

}
