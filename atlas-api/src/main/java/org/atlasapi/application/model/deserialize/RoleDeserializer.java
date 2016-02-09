package org.atlasapi.application.model.deserialize;

import java.lang.reflect.Type;

import org.atlasapi.application.users.Role;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

public class RoleDeserializer implements JsonDeserializer<Role> {

    @Override
    public Role deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        return Role.valueOf(json.getAsJsonPrimitive().getAsString().toUpperCase());
    }

}
