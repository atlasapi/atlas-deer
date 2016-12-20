package org.atlasapi.system.debug.serializers;

import java.lang.reflect.Type;
import java.math.BigInteger;

import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class EquivalenceGraphSerializer implements JsonSerializer<EquivalenceGraph> {

    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private EquivalenceGraphSerializer() {
    }

    public static EquivalenceGraphSerializer create() {
        return new EquivalenceGraphSerializer();
    }

    @Override
    public JsonElement serialize(
            EquivalenceGraph src,
            Type typeOfSrc,
            JsonSerializationContext context
    ) {
        JsonObject json = new JsonObject();

        json.add(
                "id",
                context.serialize(codec.encode(BigInteger.valueOf(src.getId().longValue())))
        );
        json.add(
                "updated",
                context.serialize(src.getUpdated())
        );

        JsonArray jsonArray = new JsonArray();
        for (Adjacents adjacents : src.getAdjacencyList().values()) {
            jsonArray.add(context.serialize(adjacents));
        }
        json.add(
                "adjacencies",
                jsonArray
        );

        return json;
    }

}
