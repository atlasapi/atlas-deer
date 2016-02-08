package org.atlasapi.application.model.deserialize;

import java.lang.reflect.Type;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

public class IdDeserializer implements JsonDeserializer<Id> {

    private final NumberToShortStringCodec idCodec;

    public IdDeserializer(NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
    }

    @Override
    public Id deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        return Id.valueOf(idCodec.decode(json.getAsJsonPrimitive().getAsString()));
    }
}
